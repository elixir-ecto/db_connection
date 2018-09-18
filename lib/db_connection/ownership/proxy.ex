defmodule DBConnection.Ownership.Proxy do
  @moduledoc false

  alias DBConnection.Holder
  use GenServer

  @time_unit 1000
  @ownership_timeout 60_000
  @queue_target 50
  @queue_interval 1000

  def start_link(caller, pool, pool_opts) do
    GenServer.start_link(__MODULE__, {caller, pool, pool_opts}, [])
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
    ownership_timeout = Keyword.get(pool_opts, :ownership_timeout, @ownership_timeout)
    timeout = Keyword.get(pool_opts, :queue_target, @queue_target) * 2
    interval = Keyword.get(pool_opts, :queue_interval, @queue_interval)

    state = %{client: nil, timer: nil, holder: nil,
              timeout: timeout, interval: interval, poll: nil,
              owner_ref: owner_ref, pool: pool, pool_ref: nil,
              pool_opts: pool_opts,  queue: :queue.new,
              ownership_timer: start_timer(caller, ownership_timeout)}

    now = System.monotonic_time(@time_unit)
    {:ok, start_poll(now, state)}
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

  def handle_info({:timeout, poll, time}, %{poll: poll} = state) do
    state = timeout(time, state)
    {:noreply, start_poll(time, state)}
  end

  def handle_info({:db_connection, from, {:checkout, _caller, _now, _queue?}}, %{holder: nil} = state) do
    %{pool: pool, pool_opts: pool_opts, owner_ref: owner_ref} = state

    case Holder.checkout(pool, pool_opts) do
      {:ok, pool_ref, mod, conn_state} ->
        holder = Holder.new(self(), owner_ref, mod, conn_state)
        state = %{state | pool_ref: pool_ref, holder: holder}
        checkout(from, state)
      {:error, err} = error ->
        GenServer.reply(from, error)
        {:stop, {:shutdown, err}, state}
    end
  end

  def handle_info({:db_connection, from, {:checkout, _caller, _now, _queue?}}, %{client: nil} = state) do
    checkout(from, state)
  end

  def handle_info({:db_connection, from, {:checkout, _caller, now, queue?}}, state) do
    if queue? do
      %{queue: queue} = state
      queue = :queue.in({now, from}, queue)
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
      {{:value, {_, from}}, queue} ->
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
  defp down(reason, %{holder: nil} = state) do
    {:stop, {:shutdown, reason}, state}
  end

  # If it is down but it has no client, checkin
  defp down(reason, %{client: nil} = state) do
    pool_done(reason, state, fn pool_ref, _ ->
      Holder.checkin(pool_ref)
    end)
  end

  # If it is down but it has a client, disconnect
  defp down(reason, state) do
    err = DBConnection.ConnectionError.exception(reason)
    pool_disconnect(err, state)
  end

  defp pool_disconnect(err, state) do
    pool_done(err, state, &Holder.disconnect/2)
  end

  defp pool_stop(reason, state) do
    pool_done(reason, state, &Holder.stop/2)
  end

  defp pool_done(err, state, done) do
    %{holder: holder, pool_ref: pool_ref} = state
    if holder do
      Holder.copy_state(pool_ref, holder)
      done.(pool_ref, err)
    end
    {:stop, {:shutdown, err}, state}
  end

  defp start_poll(now, %{interval: interval} = state) do
    timeout = now + interval
    poll = :erlang.start_timer(timeout, self(), timeout, [abs: true])
    %{state | poll: poll}
  end

  defp timeout(time, %{queue: queue, timeout: timeout} = state) do
    case :queue.out(queue) do
      {{:value, {sent, from}}, queue} when sent + timeout < time ->
        drop(time - sent, from)
        timeout(time, %{state | queue: queue})
      {_, _} ->
        state
    end
  end

  defp drop(delay, from) do
    message = "connection not available " <>
      "and request was dropped from queue after #{delay}ms"
    err = DBConnection.ConnectionError.exception(message)
    GenServer.reply(from, {:error, err})
  end
end
