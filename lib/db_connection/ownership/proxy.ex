defmodule DBConnection.Ownership.Proxy do
  @moduledoc false

  alias DBConnection.Holder
  use GenServer

  @ownership_timeout 60_000

  def start_link(caller, pool, pool_opts) do
    GenServer.start_link(__MODULE__, {caller, pool, pool_opts}, [])
  end

  def init(proxy, opts) do
    ownership_timeout = opts[:ownership_timeout] || @ownership_timeout
    case GenServer.call(proxy, {:init, ownership_timeout}, :infinity) do
      :ok                 -> :ok
      {:error, _} = error -> error
   end
  end

  def stop(proxy, caller) do
    GenServer.cast(proxy, {:stop, caller})
  end

  # Callbacks

  def init({caller, pool, pool_opts}) do
    pool_opts =
      pool_opts
      |> Keyword.put(:timeout, :infinity)
      |> Keyword.delete(:deadline)
    owner_ref = Process.monitor(caller)

    state = %{client: nil, timer: nil, holder: nil, owner: caller,
              owner_ref: owner_ref, pool: pool, pool_ref: nil,
              pool_opts: pool_opts,  queue: :queue.new,
              ownership_timer: nil}

    {:ok, state}
  end

  def handle_info({:DOWN, ref, _, pid, reason},
                  %{owner_ref: ref, client: nil} = state) do
    message = "owner #{inspect pid} exited with: " <> Exception.format_exit(reason)
    down(message, state)
  end

  def handle_info({:DOWN, ref, _, pid, reason},
                  %{owner_ref: ref, client: {client, _}} = state) do
    message = "owner #{inspect pid} exited while client #{inspect client} is still running with: " <> Exception.format_exit(reason)
    down(message, state)
  end

  def handle_info({:timeout, timer, {__MODULE__, pid, timeout}}, %{ownership_timer: timer} = state) do
    message = "owner #{inspect pid} timed out because " <>
    "it owned the connection for longer than #{timeout}ms (set via the :ownership_timeout option)"
    pool_disconnect(DBConnection.ConnectionError.exception(message), state)
  end

  def handle_info({:timeout, deadline, {_ref, holder, pid, len}}, %{holder: holder} = state) do
    if Holder.handle_deadline(holder, deadline) do
      message = "client #{inspect pid} timed out because " <>
        "it queued and checked out the connection for longer than #{len}ms"
      err = DBConnection.ConnectionError.exception(message)
      pool_disconnect(err, state)
    else
      {:noreply, state}
    end
  end

  def handle_info({:db_connection, from, {:checkout, _caller, _now, _queue?}}, %{client: nil} = state) do
    checkout(from, state)
  end

  def handle_info({:db_connection, from, {:checkout, _caller, _now, queue?}}, state) do
    if queue? do
      %{queue: queue} = state
      queue = :queue.in(from, queue)
      {:noreply, %{state | queue: queue}}
    else
      message = "connection not available and queuing is disabled"
      err = DBConnection.ConnectionError.exception(message)
      GenServer.reply(from, {:error, err})
      {:noreply, state}
    end
  end

  def handle_info({:"ETS-TRANSFER", holder, _, {msg, ref, extra}}, %{holder: holder, client: {_, ref}} = state) do
    case msg do
      :checkin ->
        checkin(state)
      :disconnect ->
        pool_disconnect(extra, state)
      :stop ->
        pool_stop(extra, state)
    end
  end

  def handle_info({:"ETS-TRANSFER", holder, pid, ref}, %{holder: holder, owner_ref: ref} = state) do
    down("client #{inspect pid} exited", state)
  end

  def handle_call({:init, ownership_timeout}, _from, state) do
    %{pool: pool, pool_opts: pool_opts, owner: owner, owner_ref: owner_ref} = state

    case Holder.checkout(pool, pool_opts) do
      {:ok, pool_ref, mod, conn_state} ->
        holder = Holder.new(self(), owner_ref, mod, conn_state)
        state = %{state | pool_ref: pool_ref, holder: holder,
                          ownership_timer: start_timer(owner, ownership_timeout)}
        {:reply, :ok, state}
      {:error, _} = error ->
        {:stop, :normal, error, state}
    end
  end

  def handle_cast({:stop, pid}, state) do
    down("owner #{inspect pid} checked in the connection", state)
  end

  defp checkout({_pid, ref} = from, %{holder: holder} = state) do
    if Holder.handle_checkout(holder, from, ref) do
      {:noreply, %{state | client: from}}
    else
      next(state)
    end
  end

  defp checkin(state) do
    next(%{state | client: nil})
  end

  defp next(%{queue: queue} = state) do
   case :queue.out(queue) do
      {{:value, from}, queue} ->
        checkout(from, %{state | queue: queue})
      {:empty, queue} ->
        {:noreply, %{state | queue: queue}}
    end
  end

  defp start_timer(_, :infinity), do: nil
  defp start_timer(pid, timeout) do
    :erlang.start_timer(timeout, self(), {__MODULE__, pid, timeout})
  end

  # It is down but never checked out from pool
  defp down(_reason, %{holder: nil} = state) do
    {:stop, :normal, state}
  end

  # If it is down but it has no client, checkin
  defp down(_reason, %{client: nil} = state) do
    %{pool_ref: pool_ref, pool_opts: pool_opts, holder: holder} = state
    conn_state = Holder.get_state(holder)
    Holder.checkin(pool_ref, conn_state, pool_opts)
    {:stop, :normal, state}
  end

  # If it is down but it has a client, disconnect
  defp down(reason, state) do
    err = DBConnection.ConnectionError.exception(reason)
    pool_disconnect(err, state)
  end

  defp pool_disconnect(err, state) do
    pool_done(&Holder.disconnect/4, err, state)
  end

  defp pool_stop(reason, state) do
    pool_done(&Holder.stop/4, reason, state)
  end

  defp pool_done(done, err, state) do
    %{holder: holder, pool_ref: pool_ref, pool_opts: pool_opts} = state
    conn_state = Holder.get_state(holder)
    done.(pool_ref, err, conn_state, pool_opts)
    {:stop, :normal, state}
  end
end
