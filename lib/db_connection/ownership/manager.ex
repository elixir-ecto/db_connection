defmodule DBConnection.Ownership.Manager do
  @moduledoc false
  use GenServer
  require Logger
  alias DBConnection.Ownership.{Proxy, ProxySupervisor}

  @timeout 5_000

  @callback start_link({module, opts :: Keyword.t}) ::
    GenServer.on_start
  def start_link({module, opts}) do
    {owner_opts, pool_opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, {module, owner_opts, pool_opts}, owner_opts)
  end

  @spec checkout(GenServer.server, Keyword.t) ::
    {:ok, pid} | {:already, :owner | :allowed}
  def checkout(manager, opts) do
    GenServer.call(manager, {:checkout, opts}, :infinity)
  end

  @spec checkin(GenServer.server, Keyword.t) ::
    :ok | :not_owner | :not_found
  def checkin(manager, opts) do
    timeout = Keyword.get(opts, :timeout, @timeout)
    GenServer.call(manager, :checkin, timeout)
  end

  @spec mode(GenServer.server, :auto | :manual | {:shared, pid}, Keyword.t) ::
    :ok | :already_shared | :not_owner | :not_found
  def mode(manager, mode, opts)
      when mode in [:auto, :manual]
      when elem(mode, 0) == :shared and is_pid(elem(mode, 1)) do
    timeout = Keyword.get(opts, :timeout, @timeout)
    GenServer.call(manager, {:mode, mode}, timeout)
  end

  @spec allow(GenServer.server, parent :: pid, allow :: pid, Keyword.t) ::
    :ok | {:already, :owner | :allowed} | :not_found
  def allow(manager, parent, allow, opts) do
    timeout = Keyword.get(opts, :timeout, @timeout)
    GenServer.call(manager, {:allow, parent, allow}, timeout)
  end

  ## Callbacks

  def init({module, owner_opts, pool_opts}) do
    ets =
      case Keyword.fetch(owner_opts, :name) do
        {:ok, name} when is_atom(name) ->
          :ets.new(name, [:named_table, :protected, read_concurrency: true])
        _ ->
          nil
      end

    # We can only start the connection pool directly because
    # neither the pool's GenServer nor the manager trap exits.
    # Otherwise we would need a supervisor plus a watcher process.
    pool_opts = Keyword.delete(pool_opts, :pool)
    {:ok, pool} = DBConnection.start_link(module, pool_opts)

    mode = Keyword.get(pool_opts, :ownership_mode, :auto)
    log = Keyword.get(pool_opts, :ownership_log, nil)

    {:ok, %{pool: pool, checkouts: %{}, owners: %{},
            mode: mode, mode_ref: nil, ets: ets, log: log}}
  end

  def handle_call({:mode, {:shared, pid}}, _from, %{mode: {:shared, current}} = state) do
    cond do
      pid == current ->
        {:reply, :ok, state}
      Process.alive?(current) ->
        {:reply, :already_shared, state}
      true ->
        share_and_reply(state, pid)
    end
  end
  def handle_call({:mode, {:shared, pid}}, _from, state) do
    share_and_reply(state, pid)
  end
  def handle_call({:mode, mode}, _from, %{mode: mode} = state) do
    {:reply, :ok, state}
  end
  def handle_call({:mode, mode}, _from, state) do
    state = proxy_checkin_all(state, nil)
    {:reply, :ok, %{state | mode: mode, mode_ref: nil}}
  end

  def handle_call(:checkin, {caller, _}, state) do
    {reply, state} = proxy_checkin(state, caller)
    {:reply, reply, state}
  end

  def handle_call({:allow, caller, allow}, _from, %{checkouts: checkouts} = state) do
    if kind = already_checked_out(checkouts, allow) do
      {:reply, {:already, kind}, state}
    else
      case Map.get(checkouts, caller, :not_found) do
        {:owner, ref, proxy} ->
          {:reply, :ok, owner_allow(state, allow, ref, proxy)}
        {:allowed, ref, proxy} ->
          {:reply, :ok, owner_allow(state, allow, ref, proxy)}
        :not_found ->
          {:reply, :not_found, state}
      end
    end
  end

  def handle_call({:checkout, opts}, {caller, _}, %{checkouts: checkouts} = state) do
    if kind = already_checked_out(checkouts, caller) do
      {:reply, {:already, kind}, state}
    else
      {proxy, state} = proxy_checkout(state, caller, opts)
      {:reply, {:ok, proxy}, state}
    end
  end

  def handle_info({:db_connection, from, {:checkout, caller, _now, queue?}}, state) do
    %{checkouts: checkouts, mode: mode} = state
    case Map.get(checkouts, caller, :not_found) do
      {status, _ref, proxy} when status in [:owner, :allowed] ->
        DBConnection.Holder.reply_redirect(from, proxy)
        {:noreply, state}
      :not_found when mode == :auto ->
        {proxy, state} = proxy_checkout(state, caller, [queue: queue?])
        DBConnection.Holder.reply_redirect(from, proxy)
        {:noreply, state}
      :not_found when mode == :manual ->
        not_found(from)
        {:noreply, state}
      :not_found ->
        {:shared, shared} = mode
        {:owner, ref, proxy} = Map.fetch!(checkouts, shared)
        DBConnection.Holder.reply_redirect(from, proxy)
        {:noreply, owner_allow(state, caller, ref, proxy)}
    end
  end

  def handle_info({:DOWN, ref, _, _, _}, state) do
    {:noreply, state |> owner_down(ref) |> unshare(ref)}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  defp already_checked_out(checkouts, pid) do
    case Map.get(checkouts, pid, :not_found) do
      {:owner, _, _} -> :owner
      {:allowed, _, _} -> :allowed
      :not_found -> nil
    end
  end

  defp proxy_checkout(state, caller, opts) do
    %{pool: pool, checkouts: checkouts, owners: owners,
      ets: ets, log: log} = state
    {:ok, proxy} = ProxySupervisor.start_owner(caller, pool, opts)
    log && Logger.log(log, fn -> [inspect(caller), " owns proxy " | inspect(proxy)] end)
    ref = Process.monitor(proxy)
    checkouts = Map.put(checkouts, caller, {:owner, ref, proxy})
    owners = Map.put(owners, ref, {proxy, caller, []})
    ets && :ets.insert(ets, {caller, proxy})
    {proxy, %{state | checkouts: checkouts, owners: owners}}
  end

  defp proxy_checkin(state, caller) do
    case get_and_update_in(state.checkouts, &Map.pop(&1, caller, :not_found)) do
      {{:owner, ref, proxy}, state} ->
        Proxy.stop(proxy, caller)
        {:ok, state |> owner_down(ref) |> unshare(ref)}
      {{:allowed, _, _}, _} ->
        {:not_owner, state}
      {:not_found, _} ->
        {:not_found, state}
    end
  end

  defp proxy_checkin_all(state, pid) do
    Enum.reduce(state.checkouts, state, fn {key, _}, state ->
      if key == pid do
        state
      else
        {_, state} = proxy_checkin(state, key)
        state
      end
    end)
  end

  defp owner_allow(%{ets: ets, log: log} = state, allow, ref, proxy) do
    log && Logger.log(log, fn -> [inspect(allow), " allowed on proxy " | inspect(proxy)] end)
    state = put_in(state.checkouts[allow], {:allowed, ref, proxy})
    state = update_in(state.owners[ref], fn {proxy, caller, allowed} ->
      {proxy, caller, [allow|List.delete(allowed, allow)]}
    end)
    ets && :ets.insert(ets, {allow, proxy})
    state
  end

  defp owner_down(%{ets: ets, log: log} = state, ref) do
    case get_and_update_in(state.owners, &Map.pop(&1, ref)) do
      {{proxy, caller, allowed}, state} ->
        Process.demonitor(ref, [:flush])
        entries = [caller|allowed]
        log && Logger.log(log, fn ->
          [Enum.map_join(entries, ", ", &inspect/1), " lose proxy " | inspect(proxy)]
        end)
        ets && Enum.each(entries, &:ets.delete(ets, &1))
        update_in(state.checkouts, &Map.drop(&1, entries))
      {nil, state} ->
        state
    end
  end

  defp share_and_reply(%{checkouts: checkouts} = state, pid) do
    case Map.get(checkouts, pid, :not_found) do
      {:owner, ref, _} ->
        state = proxy_checkin_all(state, pid)
        {:reply, :ok, %{state | mode: {:shared, pid}, mode_ref: ref}}
      {:allowed, _, _} ->
        {:reply, :not_owner, state}
      :not_found ->
        {:reply, :not_found, state}
    end
  end

  defp unshare(%{mode_ref: ref} = state, ref) do
    %{state | mode: :manual, mode_ref: nil}
  end
  defp unshare(state, _ref) do
    state
  end

  defp not_found({pid, _} = from) do
    msg = """
    cannot find ownership process for #{inspect pid}.

    When using ownership, you must manage connections in one
    of the four ways:

    * By explicitly checking out a connection
    * By explicitly allowing a spawned process
    * By running the pool in shared mode
    * By using :caller option with allowed process

    The first two options require every new process to explicitly
    check a connection out or be allowed by calling checkout or
    allow respectively.

    The third option requires a {:shared, pid} mode to be set.
    If using shared mode in tests, make sure your tests are not
    async.

    The fourth option requires [caller: pid] to be used when
    checking out a connection from the pool. The caller process
    should already be allowed on a connection.

    If you are reading this error, it means you have not done one
    of the steps above or that the owner process has crashed.
    """

    DBConnection.Holder.reply_error(from, DBConnection.OwnershipError.exception(msg))
  end
end
