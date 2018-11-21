defmodule DBConnection.ConnectionPool do
  @moduledoc """
  Default implementation of connection pool.

  ## Options

    * `:name` - the name of the pool
    * `:spawn_opt` - options given to the pool manager
    * `:pool_size` - defaults to 1
    * `:queue_target`
    * `:queue_interval`
    * `:idle_interval`
    * `:max_restarts`
    * `:max_seconds`
    * `:backoff_min`
    * `:backoff_max`
    * `:backoff_type`

  """

  use GenServer
  alias DBConnection.ConnectionPool.PoolSupervisor
  alias DBConnection.Holder

  @queue_target 50
  @queue_interval 1000
  @idle_interval 1000
  @time_unit 1000

  ## child_spec API

  @doc false
  def child_spec(args) do
    Supervisor.Spec.worker(__MODULE__, [args])
  end

  @doc false
  def start_link({mod, opts}) do
    GenServer.start_link(__MODULE__, {mod, opts}, start_opts(opts))
  end

  ## GenServer api

  def init({mod, opts}) do
    queue = :ets.new(__MODULE__.Queue, [:protected, :ordered_set])
    {:ok, _} = PoolSupervisor.start_pool(queue, mod, opts)
    target = Keyword.get(opts, :queue_target, @queue_target)
    interval = Keyword.get(opts, :queue_interval, @queue_interval)
    idle_interval = Keyword.get(opts, :idle_interval, @idle_interval)
    now = System.monotonic_time(@time_unit)
    codel = %{target: target, interval: interval, delay: 0, slow: false,
              next: now, poll: nil, idle_interval: idle_interval, idle: nil}
    codel = start_idle(now, now, start_poll(now, now, codel))
    {:ok, {:busy, queue, codel}}
  end

  def handle_info({:db_connection, from, {:checkout, _caller, now, queue?}}, {:busy, queue, _} = busy) do
    case queue? do
      true ->
        :ets.insert(queue, {{now, System.unique_integer(), from}})
        {:noreply, busy}
      false ->
        message = "connection not available and queuing is disabled"
        err = DBConnection.ConnectionError.exception(message)
        Holder.reply_error(from, err)
        {:noreply, busy}
    end
  end

  def handle_info({:db_connection, from, {:checkout, _caller, _now, _queue?}} = checkout, ready) do
    {:ready, queue, _codel} = ready
    case :ets.first(queue) do
      {_time, holder} = key ->
        Holder.handle_checkout(holder, from, queue) and :ets.delete(queue, key)
        {:noreply, ready}
      :"$end_of_table" ->
        handle_info(checkout, put_elem(ready, 0, :busy))
    end
  end

  def handle_info({:"ETS-TRANSFER", holder, pid, queue}, {_, queue, _} = data) do
    message = "client #{inspect pid} exited"
    err = DBConnection.ConnectionError.exception(message)
    Holder.handle_disconnect(holder, err)
    {:noreply, data}
  end

  def handle_info({:"ETS-TRANSFER", holder, _, {msg, queue, extra}}, {_, queue, _} = data) do
    case msg do
      :checkin ->
        handle_checkin(holder, extra, data)
      :disconnect ->
        Holder.handle_disconnect(holder, extra)
        {:noreply, data}
      :stop ->
        Holder.handle_stop(holder, extra)
        {:noreply, data}
    end
  end

  def handle_info({:timeout, deadline, {queue, holder, pid, len}}, {_, queue, _} = data) do
    # Check that timeout refers to current holder (and not previous)
    if Holder.handle_deadline(holder, deadline) do
      message = "client #{inspect pid} timed out because " <>
        "it queued and checked out the connection for longer than #{len}ms"
      err = DBConnection.ConnectionError.exception(message)
      Holder.handle_disconnect(holder, err)
    end
    {:noreply, data}
  end

  def handle_info({:timeout, poll, {time, last_sent}}, {_, _, %{poll: poll}} = data) do
    {status, queue, codel} = data

    # If no queue progress since last poll check queue
    case :ets.first(queue) do
      {sent, _, _} when sent <= last_sent and status == :busy ->
        delay = time - sent
        timeout(delay, time, queue, start_poll(time, sent, codel))
      {sent, _, _} ->
        {:noreply, {status, queue, start_poll(time, sent, codel)}}
      _ ->
        {:noreply, {status, queue, start_poll(time, time, codel)}}
    end
  end

  def handle_info({:timeout, idle, {time, last_sent}}, {_, _, %{idle: idle}} = data) do
    {status, queue, codel} = data

    # If no queue progress since last idle check oldest connection
    case :ets.first(queue) do
      {sent, holder} = key when sent <= last_sent and status == :ready ->
        :ets.delete(queue, key)
        ping(holder, queue, start_idle(time, last_sent, codel))
      {sent, _} ->
        {:noreply, {status, queue, start_idle(time, sent, codel)}}
      _ ->
        {:noreply, {status, queue, start_idle(time, time, codel)}}
    end
  end

  defp timeout(delay, time, queue, codel) do
    case codel do
      %{delay: min_delay, next: next, target: target, interval: interval}
          when time >= next and min_delay > target ->
        codel = %{codel | slow: true, delay: delay, next: time + interval}
        drop_slow(time, target * 2, queue)
        {:noreply, {:busy, queue, codel}}
      %{next: next, interval: interval} when time >= next ->
        codel = %{codel | slow: false, delay: delay, next: time + interval}
        {:noreply, {:busy, queue, codel}}
      _ ->
        {:noreply, {:busy, queue, codel}}
    end
  end

  defp drop_slow(time, timeout, queue) do
    min_sent = time - timeout
    match = {{:"$1", :_, :"$2"}}
    guards = [{:<, :"$1", min_sent}]
    select_slow = [{match, guards, [{{:"$1", :"$2"}}]}]
    for {sent, from} <- :ets.select(queue, select_slow) do
      drop(time - sent, from)
    end
    :ets.select_delete(queue, [{match, guards, [true]}])
  end

  defp ping(holder, queue, codel) do
    Holder.handle_ping(holder)
    {:noreply, {:ready, queue, codel}}
  end

  defp handle_checkin(holder, now, {:ready, queue, _} = data) do
    :ets.insert(queue, {{now, holder}})
    {:noreply, data}
  end

  defp handle_checkin(holder, now, {:busy, queue, codel}) do
    dequeue(now, holder, queue, codel)
  end

  defp dequeue(time, holder, queue, codel) do
    case codel do
      %{next: next, delay: delay, target: target} when time >= next  ->
        dequeue_first(time, delay > target, holder, queue, codel)
      %{slow: false} ->
        dequeue_fast(time, holder, queue, codel)
      %{slow: true, target: target} ->
        dequeue_slow(time, target * 2, holder, queue, codel)
    end
  end

  defp dequeue_first(time, slow?, holder, queue, codel) do
    %{interval: interval} = codel
    next = time + interval
    case :ets.first(queue) do
      {sent, _, from} = key ->
        :ets.delete(queue, key)
        delay = time - sent
        codel =  %{codel | next: next, delay: delay, slow: slow?}
        go(delay, from, time, holder, queue, codel)
      :"$end_of_table" ->
        codel = %{codel | next: next, delay: 0, slow: slow?}
        :ets.insert(queue, {{time, holder}})
        {:noreply, {:ready, queue, codel}}
    end
  end

  defp dequeue_fast(time, holder, queue, codel) do
    case :ets.first(queue) do
      {sent, _, from} = key ->
        :ets.delete(queue, key)
        go(time - sent, from, time, holder, queue, codel)
      :"$end_of_table" ->
        :ets.insert(queue, {{time, holder}})
        {:noreply, {:ready, queue, %{codel | delay: 0}}}
    end
  end

  defp dequeue_slow(time, timeout, holder, queue, codel) do
    case :ets.first(queue) do
      {sent, _, from} = key when time - sent > timeout ->
        :ets.delete(queue, key)
        drop(time - sent, from)
        dequeue_slow(time, timeout, holder, queue, codel)
      {sent, _, from} = key ->
        :ets.delete(queue, key)
        go(time - sent, from, time, holder, queue, codel)
      :"$end_of_table" ->
        :ets.insert(queue, {{time, holder}})
        {:noreply, {:ready, queue, %{codel | delay: 0}}}
    end
  end

  defp go(delay, from, time, holder, queue, %{delay: min} = codel) do
    case Holder.handle_checkout(holder, from, queue) do
      true when delay < min ->
        {:noreply, {:busy, queue, %{codel | delay: delay}}}
      true ->
        {:noreply, {:busy, queue, codel}}
      false ->
        dequeue(time, holder, queue, codel)
    end
  end

  defp drop(delay, from) do
    message =
      "connection not available and request was dropped from queue after #{delay}ms. " <>
        "You can configure how long requests wait in the queue using :queue_target and " <>
        ":queue_interval. See DBConnection.start_link/2 for more information"

    err = DBConnection.ConnectionError.exception(message)
    Holder.reply_error(from, err)
  end

  defp start_opts(opts) do
    Keyword.take(opts, [:name, :spawn_opt])
  end

  defp start_poll(now, last_sent, %{interval: interval} = codel) do
    timeout = now + interval
    poll = :erlang.start_timer(timeout, self(), {timeout, last_sent}, [abs: true])
    %{codel | poll: poll}
  end

  defp start_idle(now, last_sent, %{idle_interval: interval} = codel) do
    timeout = now + interval
    idle = :erlang.start_timer(timeout, self(), {timeout, last_sent}, [abs: true])
    %{codel | idle: idle}
  end
end
