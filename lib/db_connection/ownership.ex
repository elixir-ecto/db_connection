defmodule DBConnection.Ownership do
  @moduledoc """
  A `DBConnection.Pool` that requires explicit checkout and checking
  as a mechanism to coordinate between processes.

  ### Options

    * `:ownership_pool` - The actual pool to use to power the ownership
      mechanism. The pool is started when the ownership pool is started,
      although this option may also be given on `ownership_checkout/2`
      allowing developers to customize the pool checkout/checkin
    * `:ownership_timeout` - The timeout for ownership operations
    * `:ownership_mode` - When mode is `:manual`, all connections must
      be explicitly checked out before by using `ownership_checkout/2`.
      Otherwise, mode is `:auto` and connections are checked out
      implicitly. In both cases, checkins are implicit via
      `ownership_checkin/2`. Defaults to `:auto`.
  """

  @behaviour DBConnection.Pool
  @strategy :one_for_all

  defmodule Owner do
    @moduledoc false

    use GenServer
    @timeout 15_000

    def start(from, pool, pool_opts) do
      GenServer.start(__MODULE__, {from, pool, pool_opts}, [])
    end

    def checkout(owner, opts) do
      timeout = Keyword.get(opts, :owner_timeout, @timeout)
      queue?  = Keyword.get(opts, :queue, true)
      GenServer.call(owner, {:checkout, queue?}, timeout)
    end

    def checkin(owner, state, opts) do
      timeout = Keyword.get(opts, :owner_timeout, @timeout)
      GenServer.call(owner, {:checkin, state}, timeout)
    end

    def disconnect(owner, exception, state, opts) do
      timeout = Keyword.get(opts, :owner_timeout, @timeout)
      GenServer.call(owner, {:disconnect, exception, state}, timeout)
    end

    def stop(owner, reason, state, opts) do
      timeout = Keyword.get(opts, :owner_timeout, @timeout)
      GenServer.call(owner, {:stop, reason, state}, timeout)
    end

    def stop(owner) do
      GenServer.cast(owner, :stop)
    end

    # Callbacks

    def init(args) do
      send self(), :init
      {:ok, args}
    end

    def handle_info(:init, {from, pool, pool_opts}) do
      pool_mod = Keyword.get(pool_opts, :ownership_pool, DBConnection.Poolboy)

      state = %{client_ref: nil, conn_state: nil, conn_module: nil,
                owner_ref: nil, pool: pool, pool_mod: pool_mod,
                pool_opts: pool_opts, pool_ref: nil, queue: :queue.new}

      try do
        pool_mod.checkout(pool, pool_opts)
      catch
        kind, reason ->
          GenServer.reply(from, {kind, reason, System.stacktrace})
          {:stop, {:shutdown, "no checkout"}, state}
      else
        {:ok, pool_ref, conn_module, conn_state} ->
          GenServer.reply(from, {:ok, self()})
          {caller, _} = from
          ref = Process.monitor(caller)
          {:noreply, %{state | conn_state: conn_state, conn_module: conn_module,
                               owner_ref: ref, pool_ref: pool_ref}}
        :error ->
          GenServer.reply(from, :error)
          {:stop, {:shutdown, "no checkout"}, state}
      end
    end

    def handle_info({:DOWN, ref, _, _, _}, %{client_ref: ref} = state) do
      %{conn_state: conn_state, pool_mod: pool_mod,
        pool_opts: pool_opts, pool_ref: pool_ref} = state
      error = DBConnection.Error.exception("client down")
      pool_mod.disconnect(pool_ref, error, conn_state, pool_opts)
      {:stop, {:shutdown, "client down"}, state}
    end

    def handle_info({:DOWN, ref, _, _, _}, %{owner_ref: ref} = state) do
      down("owner down", state)
    end

    def handle_info(_msg, state) do
      {:noreply, state}
    end

    def handle_call({:checkout, queue?}, from, %{queue: queue} = state) do
      if queue? or :queue.is_empty(queue) do
        queue = :queue.in(from, queue)
        {:noreply, next(queue, state)}
      else
        {:reply, :error, state}
      end
    end

    def handle_call({:checkin, conn_state}, _from, %{queue: queue, client_ref: ref} = state) do
      Process.demonitor(ref, [:flush])
      {:reply, :ok, next(:queue.drop(queue), %{state | conn_state: conn_state})}
    end

    def handle_call({:stop, reason, conn_state}, from, state) do
      %{pool_mod: pool_mod, pool_ref: pool_ref, pool_opts: pool_opts} = state
      GenServer.reply(from, pool_mod.stop(pool_ref, reason, conn_state, pool_opts))
      {:stop, {:shutdown, "stop"}, state}
    end

    def handle_call({:disconnect, error, conn_state}, from, state) do
      %{pool_mod: pool_mod, pool_ref: pool_ref, pool_opts: pool_opts} = state
      GenServer.reply(from, pool_mod.disconnect(pool_ref, error, conn_state, pool_opts))
      {:stop, {:shutdown, "disconnect"}, state}
    end

    def handle_cast(:stop, state) do
      down("owner checkin", state)
    end

    defp next(queue, state) do
      case :queue.peek(queue) do
        {:value, from} ->
          {caller, _} = from
          %{conn_module: conn_module, conn_state: conn_state} = state
          GenServer.reply(from, {:ok, self(), conn_module, conn_state})
          %{state | queue: queue, client_ref: Process.monitor(caller)}
        :empty ->
          %{state | queue: queue, client_ref: nil}
      end
    end

    # If it is down but it has no client, checkin
    defp down(reason, %{client_ref: nil} = state) do
      %{pool_mod: pool_mod, pool_ref: pool_ref,
        conn_state: conn_state, pool_opts: pool_opts} = state
      pool_mod.checkin(pool_ref, conn_state, pool_opts)
      {:stop, {:shutdown, reason}, state}
    end

    # If it is down but it has a client, disconnect
    defp down(reason, state) do
      %{pool_mod: pool_mod, pool_ref: pool_ref,
        conn_state: conn_state, pool_opts: pool_opts} = state
      error = DBConnection.Error.exception(reason)
      pool_mod.disconnect(pool_ref, error, conn_state, pool_opts)
      {:stop, {:shutdown, reason}, state}
    end
  end

  defmodule Manager do
    @moduledoc false

    use GenServer
    @timeout 15_000

    @callback start_link(module, opts :: Keyword.t) ::
      GenServer.on_start
    def start_link(module, opts) do
      pool_mod = Keyword.get(opts, :ownership_pool, DBConnection.Poolboy)
      {owner_opts, pool_opts} = Keyword.split(opts, [:name])
      GenServer.start_link(__MODULE__, {module, pool_mod, pool_opts}, owner_opts)
    end

    @spec checkout(GenServer.server, Keyword.t) ::
      {:ok, pid} | {:already, :owner | :allowed} | :error |
      {kind :: atom, reason :: term, stack :: Exception.stacktrace}
    def checkout(manager, opts) do
      timeout = Keyword.get(opts, :owner_timeout, @timeout)
      GenServer.call(manager, {:checkout, opts}, timeout)
    end

    @spec checkin(GenServer.server, Keyword.t) ::
      :ok | :not_owner | :not_found
    def checkin(manager, opts) do
      timeout = Keyword.get(opts, :owner_timeout, @timeout)
      GenServer.call(manager, :checkin, timeout)
    end

    @spec mode(GenServer.server, :auto | :manual, Keyword.t) :: :ok
    def mode(manager, mode, opts) when mode in [:auto, :manual] do
      timeout = Keyword.get(opts, :owner_timeout, @timeout)
      GenServer.call(manager, {:mode, mode}, timeout)
    end

    @spec allow(GenServer.server, parent :: pid, allow :: pid, Keyword.t) ::
      :ok | {:already, :owner | :allowed} | :not_owner | :not_found
    def allow(manager, parent, allow, opts) do
      timeout = Keyword.get(opts, :owner_timeout, @timeout)
      GenServer.call(manager, {:allow, parent, allow}, timeout)
    end

    @spec lookup(GenServer.server, Keyword.t) ::
      {:ok, pid} | :not_found | :error |
      {kind :: atom, reason :: term, stack :: Exception.stacktrace}
    def lookup(manager, opts) do
      timeout = Keyword.get(opts, :owner_timeout, @timeout)
      GenServer.call(manager, {:lookup, opts}, timeout)
    end

    ## Callbacks

    def init({module, pool_mod, pool_opts}) do
      {:ok, pool} = pool_mod.start_link(module, pool_opts)
      mode = Keyword.get(pool_opts, :ownership_mode, :auto)
      {:ok, %{pool: pool, checkouts: %{}, owners: %{}, mode: mode}}
    end

    def handle_call({:mode, mode}, _from, state) do
      {:reply, :ok, %{state | mode: mode}}
    end

    def handle_call({:lookup, opts}, {caller, _} = from,
                    %{checkouts: checkouts, mode: mode} = state) do
      case Map.get(checkouts, caller, :not_found) do
        {:owner, _, owner} ->
          {:reply, {:ok, owner}, state}
        {:allowed, owner} ->
          {:reply, {:ok, owner}, state}
        :not_found when mode == :manual ->
          {:reply, :not_found, state}
        :not_found when mode == :auto ->
          {:noreply, checkout(from, opts, state)}
      end
    end

    def handle_call(:checkin, {caller, _}, state) do
      case get_and_update_in(state.checkouts, &Map.pop(&1, caller, :not_found)) do
        {{:owner, ref, owner}, state} ->
          Owner.stop(owner)
          {:reply, :ok, owner_down(ref, state)}
        {{:allowed, _}, state} ->
          {:reply, :not_owner, state}
        {:not_found, state} ->
          {:reply, :not_found, state}
      end
    end

    def handle_call({:allow, caller, allow}, _from, %{checkouts: checkouts} = state) do
      if kind = already_checked_out(checkouts, allow) do
        {:reply, {:already, kind}, state}
      else
        case Map.get(checkouts, caller, :not_found) do
          {:owner, ref, owner} ->
            state = put_in(state.checkouts[allow], {:allowed, owner})
            state = update_in(state.owners[ref], fn {owner, caller, allowed} ->
              {owner, caller, [allow|List.delete(allowed, allow)]}
            end)
            {:reply, :ok, state}
          {:allowed, _} ->
            {:reply, :not_owner, state}
          :not_found ->
            {:reply, :not_found, state}
        end
      end
    end

    def handle_call({:checkout, opts}, {caller, _} = from, %{checkouts: checkouts} = state) do
      if kind = already_checked_out(checkouts, caller) do
        {:reply, {:already, kind}, state}
      else
        {:noreply, checkout(from, opts, state)}
      end
    end

    def handle_info({:DOWN, ref, _, _, _}, state) do
      {:noreply, owner_down(ref, state)}
    end

    def handle_info(_msg, state) do
      {:noreply, state}
    end

    defp already_checked_out(checkouts, pid) do
      case Map.get(checkouts, pid, :not_found) do
        {:owner, _, _} -> :owner
        {:allowed, _} -> :allowed
        :not_found -> nil
      end
    end

    # TODO: Move this to a supervisor
    # TODO: Cache using ETS
    defp checkout({caller, _} = from, opts, state) do
      %{pool: pool, checkouts: checkouts, owners: owners} = state
      {:ok, owner} = Owner.start(from, pool, opts)
      ref = Process.monitor(owner)
      checkouts = Map.put(checkouts, caller, {:owner, ref, owner})
      owners = Map.put(owners, ref, {owner, caller, []})
      %{state | checkouts: checkouts, owners: owners}
    end

    defp owner_down(ref, state) do
      case get_and_update_in(state.owners, &Map.pop(&1, ref)) do
        {{_owner, caller, allowed}, state} ->
          Process.demonitor(ref, [:flush])
          update_in(state.checkouts, &Map.drop(&1, [caller|allowed]))
        {nil, state} ->
          state
      end
    end
  end

  ## Ownership API

  @spec ownership_checkout(GenServer.server, Keyword.t) ::
    :ok | {:already, :owner | :allowed} | :error | no_return
  def ownership_checkout(manager, opts) do
     case Manager.checkout(manager, opts) do
      {:ok, _} -> :ok
      {:already, _} = already -> already
      :error -> :error
      {kind, reason, stack} -> :erlang.raise(kind, reason, stack)
    end
  end

  defdelegate ownership_mode(manager, mode, opts), to: Manager, as: :mode

  defdelegate ownership_checkin(manager, opts), to: Manager, as: :checkin

  defdelegate ownership_allow(manager, parent, allow, opts), to: Manager, as: :allow

  ## Pool callbacks

  @doc false
  def start_link(module, opts) do
    Manager.start_link(module, opts)
  end

  @doc false
  def child_spec(module, opts, child_opts) do
    Supervisor.Spec.worker(Manager, [module, opts], child_opts)
  end

  @doc false
  def checkout(manager, opts) do
    case Manager.lookup(manager, opts) do
      {:ok, owner} ->
        Owner.checkout(owner, opts)
      :not_found ->
        raise "cannot find ownership process for #{inspect self()}. " <>
              "This may happen if you have not explicitly checked out or " <>
              "the checked out process crashed"
      :error ->
        :error
      {kind, reason, stack} ->
        :erlang.raise(kind, reason, stack)
    end
  end

  @doc false
  def checkin(owner, state, opts) do
    Owner.checkin(owner, state, opts)
  end

  @doc false
  def disconnect(owner, exception, state, opts) do
    Owner.disconnect(owner, exception, state, opts)
  end

  @doc false
  def stop(owner, reason, state, opts) do
    Owner.stop(owner, reason, state, opts)
  end
end
