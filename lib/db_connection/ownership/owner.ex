defmodule DBConnection.Ownership.Owner do
  @moduledoc false

  use GenServer
  @timeout 15_000

  def start_link(from, pool, pool_opts) do
    GenServer.start_link(__MODULE__, {from, pool, pool_opts}, [])
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
