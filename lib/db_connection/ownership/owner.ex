defmodule DBConnection.Ownership.Owner do
  @moduledoc false

  use GenServer
  @pool_timeout      5_000
  @ownership_timeout 15_000
  @timeout           15_000

  def start_link(manager, caller, pool, pool_opts) do
    GenServer.start_link(__MODULE__, {manager, caller, pool, pool_opts}, [])
  end

  def init(owner, opts) do
    ownership_timeout = opts[:ownership_timeout] || @ownership_timeout
    case GenServer.call(owner, {:init, ownership_timeout}, :infinity) do
      :ok                 -> :ok
      {:error, _} = error -> error
   end
  end

  def checkout(pool, opts) do
    pool_timeout = opts[:pool_timeout] || @pool_timeout
    queue?       = Keyword.get(opts, :queue, true)
    timeout      = opts[:timeout] || @timeout

    ref = make_ref()
    try do
      GenServer.call(pool, {:checkout, ref, queue?, timeout}, pool_timeout)
    catch
      :exit, {_, {_, :call, [pool | _]}} = reason ->
        GenServer.cast(pool, {:cancel, ref})
        exit(reason)
    end
  end

  def checkin({owner, ref}, state, _opts) do
    GenServer.cast(owner, {:checkin, ref, state})
  end

  def disconnect({owner, ref}, exception, state, _opts) do
    GenServer.cast(owner, {:disconnect, ref, exception, state})
  end

  def stop({owner, ref}, exception, state, _opts) do
    GenServer.cast(owner, {:stop, ref, exception, state})
  end

  def stop(owner, caller) do
    GenServer.cast(owner, {:stop, caller})
  end

  # Callbacks

  def init({_manager, caller, pool, pool_opts}) do
    pool_mod = Keyword.get(pool_opts, :ownership_pool, DBConnection.Poolboy)
    pool_opts = Keyword.put(pool_opts, :timeout, :infinity)

    owner_ref = Process.monitor(caller)

    state = %{client: nil, timer: nil, conn_state: nil, conn_module: nil,
              owner_ref: owner_ref, pool: pool, pool_mod: pool_mod,
              pool_opts: pool_opts, pool_ref: nil, queue: :queue.new,
              ownership_timer: nil}

    {:ok, state}
  end

  def handle_info({:DOWN, mon, _, pid, reason}, %{client: {_, mon}} = state) do
    message = "client #{inspect pid} exited: " <> Exception.format_exit(reason)
    disconnect(message, state)
  end

  def handle_info({:DOWN, ref, _, pid, reason}, %{owner_ref: ref} = state) do
    message = "owner #{inspect pid} exited: " <> Exception.format_exit(reason)
    down(message, state)
  end

  def handle_info({:timeout, timer, {__MODULE__, pid, timeout}},
  %{timer: timer} = state) do
    message = "client #{inspect pid} timed out because " <>
    "it checked out the connection for longer than #{timeout}ms"
    disconnect(message, state)
  end

  def handle_info({:timeout, timer, {__MODULE__, pid, timeout}},
  %{ownership_timer: timer} = state) do
    message = "owner #{inspect pid} timed out because " <>
    "it owned the connection for longer than #{timeout}ms"
    disconnect(message, state)
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  def handle_call({:init, ownership_timeout}, {pid, _} = from, state) do
    %{pool: pool, pool_mod: pool_mod, pool_opts: pool_opts} = state

    try do
      pool_mod.checkout(pool, pool_opts)
    catch
      kind, reason ->
        stack = System.stacktrace()
        msg = "failed to checkout using " <> inspect(pool_mod)
        GenServer.reply(from, {:error, DBConnection.Error.exception(msg)})
        :erlang.raise(kind, reason, stack)
    else
      {:ok, pool_ref, conn_module, conn_state} ->
        state =  %{state | conn_state: conn_state, conn_module: conn_module,
                           ownership_timer: start_timer(pid, ownership_timeout),
                           pool_ref: pool_ref}
        {:reply, :ok, state}
      {:error, exception} = error ->
        {:stop, {:shutdown, exception}, error, state}
    end
  end

  def handle_call({:checkout, ref, queue?, timeout}, from, %{queue: queue} = state) do
    if queue? or :queue.is_empty(queue) do
      {pid, _} = from
      client = {ref, Process.monitor(pid)}
      queue = :queue.in({client, timeout, from}, queue)
      {:noreply, next(queue, state)}
    else
      message = "connection not available and queuing is disabled"
      err = DBConnection.Error.exception(message)
      {:reply, {:error, err}, state}
    end
  end

  def handle_cast({:checkin, ref, conn_state}, %{client: {ref, mon}} = state) do
    %{queue: queue} = state
    Process.demonitor(mon, [:flush])
    {:noreply, next(:queue.drop(queue), %{state | conn_state: conn_state})}
  end

  def handle_cast({:checkin, _, _}, state) do
    {:noreply, state}
  end

  def handle_cast({tag, ref, error, conn_state}, %{client: {ref, _}} = state)
  when tag in [:stop, :disconnect] do
    %{pool_mod: pool_mod, pool_ref: pool_ref, pool_opts: pool_opts} = state
    apply(pool_mod, tag, [pool_ref, error, conn_state, pool_opts])
    {:stop, {:shutdown, error}, state}
  end

  def handle_cast({:cancel, ref}, %{client: {ref, mon}} = state) do
    %{queue: queue} = state
    Process.demonitor(mon, [:flush])
    {:noreply, next(:queue.drop(queue), state)}
  end

  def handle_cast({:cancel, ref}, %{queue: queue} = state) do
    cancel =
      fn({{ref2, mon}, _timeout, _from}) ->
        if ref === ref2 do
          Process.demonitor(mon, [:flush])
          false
        else
          true
        end
      end
    {:noreply, %{state | queue: :queue.filter(cancel, queue)}}
  end

  def handle_cast({:stop, pid}, state) do
    down("owner #{inspect pid} checked in the connection", state)
  end

  defp next(queue, %{timer: timer} = state) do
    cancel_timer(timer)
    case :queue.peek(queue) do
      {:value, {{ref, _} = client, timeout, {pid, _} = from}} ->
        %{conn_module: conn_module, conn_state: conn_state} = state
        GenServer.reply(from, {:ok, {self(), ref}, conn_module, conn_state})
        %{state | queue: queue, client: client,
                  timer: start_timer(pid, timeout)}
      :empty ->
        %{state | queue: queue, client: nil, timer: nil}
    end
  end

  defp start_timer(_, :infinity), do: nil
  defp start_timer(pid, timeout) do
    :erlang.start_timer(timeout, self, {__MODULE__, pid, timeout})
  end

  defp cancel_timer(nil), do: :ok
  defp cancel_timer(timer) do
    case :erlang.cancel_timer(timer) do
      false -> flush_timer(timer)
      _     -> :ok
    end
  end

  defp flush_timer(timer) do
    receive do
      {:timeout, ^timer, {__MODULE__, _, _}} ->
        :ok
    after
      0 ->
        raise ArgumentError, "timer #{inspect(timer)} does not exist"
    end
  end

  # It is down but never checked out from pool
  defp down(reason, %{conn_module: nil} = state) do
    {:stop, {:shutdown, reason}, state}
  end

  # If it is down but it has no client, checkin
  defp down(reason, %{client: nil} = state) do
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

  defp disconnect(reason, state) do
    %{conn_state: conn_state, pool_mod: pool_mod,
      pool_opts: pool_opts, pool_ref: pool_ref} = state
    error = DBConnection.Error.exception(reason)
    pool_mod.disconnect(pool_ref, error, conn_state, pool_opts)
    {:stop, {:shutdown, reason}, state}
  end
end
