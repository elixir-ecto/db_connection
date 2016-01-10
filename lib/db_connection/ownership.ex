defmodule DBConnection.Ownership do
  @moduledoc """
  A `DBConnection.Pool` that requires explicit checkout and checking
  as a mechanism to coordinate between processes.

  ### Options

    * `:ownership_pool` - The actual pool to use to power the ownership
      mechanism
    * `:ownership_timeout` - The timeout for ownership operations
  """

  @behaviour DBConnection.Pool
  @strategy :one_for_all

  defmodule Owner do
    @moduledoc false

    use GenServer
    @timeout 15_000

    def start(from, pool, pool_impl, pool_opts) do
      GenServer.start(__MODULE__, {from, pool, pool_impl, pool_opts}, [])
    end

    def checkout(owner, opts) do
      timeout = Keyword.get(opts, :owner_timeout, @timeout)
      GenServer.call(owner, :checkout, timeout)
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

    def handle_info(:init, {from, pool, pool_impl, pool_opts}) do
      state = %{client_ref: nil, conn_state: nil, owner_ref: nil,
                pool: pool, pool_module: nil, pool_impl: pool_impl,
                pool_opts: pool_opts, pool_ref: nil, queue: :queue.new}

      try do
        pool_impl.checkout(pool, pool_opts)
      catch
        kind, reason ->
          GenServer.reply(from, {kind, reason, System.stacktrace})
          {:stop, {:shutdown, "no checkout"}, state}
      else
        {:ok, pool_ref, pool_module, conn_state} ->
          GenServer.reply(from, {:ok, self()})
          {caller, _} = from
          ref = Process.monitor(caller)
          {:noreply, %{state | conn_state: conn_state, owner_ref: ref,
                               pool_module: pool_module, pool_ref: pool_ref}}
        :error ->
          GenServer.reply(from, :error)
          {:stop, {:shutdown, "no checkout"}, state}
      end
    end

    def handle_info({:DOWN, ref, _, _, _}, %{client_ref: ref} = state) do
      %{pool_impl: pool_impl, pool_ref: pool_ref,
        conn_state: conn_state, pool_opts: pool_opts} = state
      error = DBConnection.Error.exception("client down")
      pool_impl.disconnect(pool_ref, error, conn_state, pool_opts)
      {:stop, {:shutdown, "client down"}, state}
    end

    def handle_info({:DOWN, ref, _, _, _}, %{owner_ref: ref} = state) do
      down("owner down", state)
    end

    def handle_info(_msg, state) do
      {:noreply, state}
    end

    def handle_call(:checkout, from, %{queue: queue} = state) do
      queue = :queue.in(from, queue)
      {:noreply, next(queue, state)}
    end

    def handle_call({:checkin, conn_state}, _from, %{queue: queue, client_ref: ref} = state) do
      Process.demonitor(ref, [:flush])
      {:reply, :ok, next(queue, %{state | conn_state: conn_state})}
    end

    def handle_call({:stop, reason, conn_state}, from, state) do
      %{pool_impl: pool_impl, pool_ref: pool_ref, pool_opts: pool_opts} = state
      GenServer.reply(from, pool_impl.stop(pool_ref, reason, conn_state, pool_opts))
      {:stop, {:shutdown, "stop"}, state}
    end

    def handle_call({:disconnect, error, conn_state}, from, state) do
      %{pool_impl: pool_impl, pool_ref: pool_ref, pool_opts: pool_opts} = state
      GenServer.reply(from, pool_impl.disconnect(pool_ref, error, conn_state, pool_opts))
      {:stop, {:shutdown, "disconnect"}, state}
    end

    def handle_cast(:stop, state) do
      down("owner checkin", state)
    end

    defp next(queue, state) do
      case :queue.out(queue) do
        {{:value, from}, queue} ->
          {caller, _} = from
          %{pool_module: pool_module, conn_state: conn_state} = state
          GenServer.reply(from, {:ok, self(), pool_module, conn_state})
          %{state | queue: queue, client_ref: Process.monitor(caller)}
        {:empty, queue} ->
          %{state | queue: queue, client_ref: nil}
      end
    end

    # If it is down but it has no client, checkin
    defp down(reason, %{client_ref: nil} = state) do
      %{pool_impl: pool_impl, pool_ref: pool_ref,
        conn_state: conn_state, pool_opts: pool_opts} = state
      pool_impl.checkin(pool_ref, conn_state, pool_opts)
      {:stop, {:shutdown, reason}, state}
    end

    # If it is down but it has a client, disconnect
    defp down(reason, state) do
      %{pool_impl: pool_impl, pool_ref: pool_ref,
        conn_state: conn_state, pool_opts: pool_opts} = state
      error = DBConnection.Error.exception(reason)
      pool_impl.disconnect(pool_ref, error, conn_state, pool_opts)
      {:stop, {:shutdown, reason}, state}
    end
  end

  defmodule Manager do
    @moduledoc false

    use GenServer
    @timeout 15_000

    def start_link(module, opts) do
      pool_impl = Keyword.fetch!(opts, :ownership_pool)
      {owner_opts, pool_opts} = Keyword.split(opts, [:name])
      GenServer.start_link(__MODULE__, {module, pool_impl, pool_opts}, owner_opts)
    end

    def checkout(manager, opts) do
      timeout = Keyword.get(opts, :owner_timeout, @timeout)
      GenServer.call(manager, {:checkout, opts}, timeout)
    end

    def lookup(manager, opts) do
      timeout = Keyword.get(opts, :owner_timeout, @timeout)
      GenServer.call(manager, :lookup, timeout)
    end

    ## Callbacks

    def init({module, pool_impl, pool_opts}) do
      {:ok, pool} = pool_impl.start_link(module, pool_opts)
      {:ok, %{pool: pool, pool_impl: pool_impl, checkouts: %{}, owners: %{}}}
    end

    def handle_call(:lookup, {caller, _}, %{checkouts: checkouts} = state) do
      {:reply, Map.fetch(checkouts, caller), state}
    end

    def handle_call(:checkin, {caller, _}, state) do
      case get_and_update_in(state.checkouts, &Map.pop(&1, caller)) do
        {nil, state} ->
          {:reply, :ok, state}
        {owner, state} ->
          Owner.stop(owner)
          {:reply, :ok, state}
      end
    end

    def handle_call({:checkout, opts}, {caller, _} = from, state) do
      %{pool_impl: pool_impl, pool: pool, checkouts: checkouts, owners: owners} = state

      # TODO: Raise if the caller already has a checkout
      # TODO: Move this to a supervisor
      # TODO: Cache using ETS
      case Map.fetch(checkouts, caller) do
        {:ok, owner} ->
          {:reply, {:ok, owner}, state}
        :error ->
          {:ok, owner} = Owner.start(from, pool, pool_impl, opts)
          checkouts = Map.put(checkouts, caller, owner)
          owners = Map.put(owners, Process.monitor(owner), {owner, caller})
          {:noreply, %{state | checkouts: checkouts, owners: owners}}
      end
    end

    def handle_info({:DOWN, ref, _, _, _}, state) do
      case get_and_update_in(state.owners, &Map.pop(&1, ref)) do
        {{_owner, caller}, state} ->
          state = update_in(state.checkouts, &Map.delete(&1, caller))
          {:noreply, state}
        {nil, state} ->
          {:noreply, state}
      end
    end

    def handle_info(_msg, state) do
      {:noreply, state}
    end
  end

  def start_link(module, opts) do
    Manager.start_link(module, opts)
  end

  def child_spec(module, opts, child_opts) do
    Supervisor.Spec.worker(Manager, [module, opts], child_opts)
  end

  def checkout(manager, opts) do
    # TODO: Expose Manager.checkout and Manager.checkin as explicit functions
    case Manager.checkout(manager, opts) do
      {:ok, owner} -> Owner.checkout(owner, opts)
      :error -> :error
      {kind, reason, stack} -> :erlang.raise(kind, reason, stack)
    end
  end

  def checkin(owner, state, opts) do
    Owner.checkin(owner, state, opts)
  end

  def disconnect(owner, exception, state, opts) do
    Owner.disconnect(owner, exception, state, opts)
  end

  def stop(owner, reason, state, opts) do
    Owner.stop(owner, reason, state, opts)
  end
end
