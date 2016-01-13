defmodule DBConnection.Ownership.Manager do
  @moduledoc false
  use GenServer

  alias DBConnection.Ownership.PoolSupervisor
  alias DBConnection.Ownership.OwnerSupervisor
  alias DBConnection.Ownership.Owner

  @timeout 5_000

  @callback start_link(module, opts :: Keyword.t) ::
    GenServer.on_start
  def start_link(module, opts) do
    pool_mod = Keyword.get(opts, :ownership_pool, DBConnection.Poolboy)
    {owner_opts, pool_opts} = Keyword.split(opts, [:name])
    GenServer.start_link(__MODULE__, {module, owner_opts, pool_mod, pool_opts}, owner_opts)
  end

  @spec checkout(GenServer.server, Keyword.t) ::
    {:ok, pid} | {:already, :owner | :allowed} | :error |
    {kind :: atom, reason :: term, stack :: Exception.stacktrace}
  def checkout(manager, opts) do
    timeout = Keyword.get(opts, :pool_timeout, @timeout)
    GenServer.call(manager, {:checkout, opts}, timeout)
  end

  @spec checkin(GenServer.server, Keyword.t) ::
    :ok | :not_owner | :not_found
  def checkin(manager, opts) do
    timeout = Keyword.get(opts, :pool_timeout, @timeout)
    GenServer.call(manager, :checkin, timeout)
  end

  @spec mode(GenServer.server, :auto | :manual, Keyword.t) :: :ok
  def mode(manager, mode, opts) when mode in [:auto, :manual] do
    timeout = Keyword.get(opts, :pool_timeout, @timeout)
    GenServer.call(manager, {:mode, mode}, timeout)
  end

  @spec allow(GenServer.server, parent :: pid, allow :: pid, Keyword.t) ::
    :ok | {:already, :owner | :allowed} | :not_owner | :not_found
  def allow(manager, parent, allow, opts) do
    timeout = Keyword.get(opts, :pool_timeout, @timeout)
    GenServer.call(manager, {:allow, parent, allow}, timeout)
  end

  @spec lookup(GenServer.server, Keyword.t) ::
    {:ok, pid} | :not_found | :error |
    {kind :: atom, reason :: term, stack :: Exception.stacktrace}
  def lookup(manager, opts) when is_atom(manager) do
    client = self()
    case :ets.lookup(manager, client) do
      [{^client, owner}] -> {:ok, owner}
      [] -> server_lookup(manager, opts)
    end
  end

  def lookup(manager, opts) do
    server_lookup(manager, opts)
  end

  defp server_lookup(manager, opts) do
    timeout = Keyword.get(opts, :pool_timeout, @timeout)
    GenServer.call(manager, {:lookup, opts}, timeout)
  end

  ## Callbacks

  def init({module, owner_opts, pool_mod, pool_opts}) do
    ets =
      case Keyword.fetch(owner_opts, :name) do
        {:ok, name} when is_atom(name) ->
          :ets.new(name, [:named_table, :protected, read_concurrency: true])
        _ ->
          nil
      end

    pool_opts = Keyword.put(pool_opts, :pool, pool_mod)
    {:ok, pool} = PoolSupervisor.start_pool(module, pool_opts)
    mode = Keyword.get(pool_opts, :ownership_mode, :auto)
    {:ok, %{pool: pool, checkouts: %{}, owners: %{}, mode: mode, ets: ets}}
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
      {{:allowed, _}, _} ->
        {:reply, :not_owner, state}
      {:not_found, _} ->
        {:reply, :not_found, state}
    end
  end

  def handle_call({:allow, caller, allow}, _from, %{checkouts: checkouts, ets: ets} = state) do
    if kind = already_checked_out(checkouts, allow) do
      {:reply, {:already, kind}, state}
    else
      case Map.get(checkouts, caller, :not_found) do
        {:owner, ref, owner} ->
          state = put_in(state.checkouts[allow], {:allowed, owner})
          state = update_in(state.owners[ref], fn {owner, caller, allowed} ->
            {owner, caller, [allow|List.delete(allowed, allow)]}
          end)
          ets && :ets.insert(ets, {allow, owner})
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

  defp checkout({caller, _} = from, opts, state) do
    %{pool: pool, checkouts: checkouts, owners: owners, ets: ets} = state
    {:ok, owner} = OwnerSupervisor.start_owner(self(), from, pool, opts)
    ref = Process.monitor(owner)
    checkouts = Map.put(checkouts, caller, {:owner, ref, owner})
    owners = Map.put(owners, ref, {owner, caller, []})
    ets && :ets.insert(ets, {caller, owner})
    %{state | checkouts: checkouts, owners: owners}
  end

  defp owner_down(ref, %{ets: ets} = state) do
    case get_and_update_in(state.owners, &Map.pop(&1, ref)) do
      {{_owner, caller, allowed}, state} ->
        Process.demonitor(ref, [:flush])
        entries = [caller|allowed]
        ets && Enum.each(entries, &:ets.delete(ets, &1))
        update_in(state.checkouts, &Map.drop(&1, entries))
      {nil, state} ->
        state
    end
  end
end
