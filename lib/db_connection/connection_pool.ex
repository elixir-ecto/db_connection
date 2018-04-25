defmodule DBConnection.ConnectionPool do

  @behaviour DBConnection.Pool
  use GenServer
  alias DBConnection.ConnectionPool.PoolSupervisor

  @timeout 5000
  @queue true
  @queue_target 50
  @queue_interval 1000
  @idle_interval 1000
  @time_unit 1000
  @holder_key :__info__

  ## DBConnection.Pool API

  @doc false
  def ensure_all_started(_opts, _type) do
    {:ok, []}
  end

  @doc false
  def start_link(mod, opts) do
    GenServer.start_link(__MODULE__, {mod, opts}, start_opts(opts))
  end

  @doc false
  def child_spec(mod, opts, child_opts \\ []) do
    Supervisor.Spec.worker(__MODULE__, [mod, opts], child_opts)
  end

  @doc false
  def checkout(pool, opts) do
    queue? = Keyword.get(opts, :queue, @queue)
    now = System.monotonic_time(@time_unit)
    timeout = abs_timeout(now, opts)
    case checkout(pool, queue?, now, timeout) do
      {:ok, _, _, _} = ok ->
        ok
      {:error, _} = error ->
        error
      {:exit, reason} ->
        exit({reason, {__MODULE__, :checkout, [pool, opts]}})
    end
  end

  @doc false
  def checkin({pool, ref, deadline, holder}, conn, _) do
    cancel_deadline(deadline)
    now = System.monotonic_time(@time_unit)
    checkin_holder(holder, pool, conn, {:checkin, ref, now})
  end

  @doc false
  def disconnect({pool, ref, deadline, holder}, err, conn, _) do
    cancel_deadline(deadline)
    checkin_holder(holder, pool, conn, {:disconnect, ref, err})
  end

  @doc false
  def stop({pool, ref, deadline, holder}, err, conn, _) do
    cancel_deadline(deadline)
    checkin_holder(holder, pool, conn, {:stop, ref, err})
  end

  ## Holder api

  @doc false
  def update(pool, ref, mod, state) do
    holder = start_holder(pool, ref, mod, state)
    now = System.monotonic_time(@time_unit)
    checkin_holder(holder, pool, state, {:checkin, ref, now})
    holder
  end

  ## GenServer api
    
  def init({mod, opts}) do
    queue = :ets.new(__MODULE__.Queue, [:private, :ordered_set])
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

  def handle_info({:db_connection, from, {:checkout, now, queue?}}, {:busy, queue, _} = busy) do
    case queue? do
      true ->
        :ets.insert(queue, {{now, System.unique_integer(), from}})
        {:noreply, busy}
      false ->
        message = "connection not available and queuing is disabled"
        err = DBConnection.ConnectionError.exception(message)
        GenServer.reply(from, {:error, err})
        {:noreply, busy}
    end
  end

  def handle_info({:db_connection, from, {:checkout, _now, _queue?}} = checkout, ready) do
    {:ready, queue, _codel} = ready 
    case :ets.first(queue) do
      {_time, holder} = key ->
        checkout_holder(holder, from, queue) and :ets.delete(queue, key)
        {:noreply, ready}
      :"$end_of_table" ->
        handle_info(checkout, put_elem(ready, 0, :busy))
    end
  end

  def handle_info({:"ETS-TRANSFER", holder, pid, queue}, {_, queue, _} = data) do
    message = "client #{inspect pid} exited"
    err = DBConnection.ConnectionError.exception(message)
    disconnect_holder(holder, err)
    {:noreply, data}
  end

  def handle_info({:"ETS-TRANSFER", holder, _, {msg, queue, extra}}, {_, queue, _} = data) do
    case msg do
      :checkin ->
        handle_checkin(holder, extra, data)
      :disconnect ->
        disconnect_holder(holder, extra)
        {:noreply, data}
      :stop ->
        stop_holder(holder, extra)
        {:noreply, data}
    end
  end

  def handle_info({:timeout, deadline, {queue, holder, pid, len}}, {_, queue, _} = data) do
    # Check that timeout refers to current holder (and not previous)
    try do
      :ets.lookup_element(holder, @holder_key, 3)
    rescue
      ArgumentError ->
        :ok
    else
      ^deadline ->
        :ets.update_element(holder, @holder_key, {3, nil})
        message = "client #{inspect pid} timed out because " <>
            "it queued and checked out the connection for longer than #{len}ms"
        err = DBConnection.ConnectionError.exception(message)
        disconnect_holder(holder, err)
      _ ->
        :ok
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
    [{_, conn, _, _, state}] = :ets.lookup(holder, @holder_key)
    DBConnection.Connection.ping({conn, holder}, state)
    :ets.delete(holder)
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
    case checkout_holder(holder, from, queue) do
      true when delay < min ->
        {:noreply, {:busy, queue, %{codel | delay: delay}}}
      true ->
        {:noreply, {:busy, queue, codel}}
      false ->
        dequeue(time, holder, queue, codel)
    end
  end

  defp drop(delay, from) do
    message = "connection not available " <>
      "and request was dropped from queue after #{delay}ms"
    err = DBConnection.ConnectionError.exception(message)
    GenServer.reply(from, {:error, err})
  end

  defp start_opts(opts) do
    Keyword.take(opts, [:name, :spawn_opt])
  end

  defp abs_timeout(now, opts) do
    case Keyword.get(opts, :timeout, @timeout) do
      :infinity ->
        Keyword.get(opts, :deadline)
      timeout ->
        min(now + timeout, Keyword.get(opts, :deadline))
    end
  end

  defp start_deadline(nil, _, _, _, _) do
    nil
  end
  defp start_deadline(timeout, pid, ref, holder, start) do
    deadline = :erlang.start_timer(timeout, pid, {ref, holder, self(), timeout-start}, [abs: true])
    :ets.update_element(holder, @holder_key, {3, deadline})
    deadline
  end

  defp cancel_deadline(nil) do
    :ok
  end

  defp cancel_deadline(deadline) do
    :erlang.cancel_timer(deadline, [async: true, info: false])
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

  defp start_holder(pool, ref, mod, state) do
    # Insert before setting heir so that pool can't receive empty table
    holder = :ets.new(__MODULE__.Holder, [:public, :ordered_set])
    :true = :ets.insert_new(holder, {@holder_key, self(), nil, mod, state})
    :ets.setopts(holder, {:heir, pool, ref})
    holder
  end

  defp checkout_holder(holder, {pid, mref}, ref) do
    try do
      :ets.give_away(holder, pid, {mref, ref})
    rescue
      ArgumentError ->
        # pid is not alive
        false
    end
  end

  defp checkout(pool, queue?, start, timeout) do
    case GenServer.whereis(pool) do
      pid when node(pid) == node() ->
        checkout_call(pid, queue?, start, timeout)
      pid when node(pid) != node() ->
        {:exit, {:badnode, node(pid)}}
      {_name, node} ->
        {:exit, {:badnode, node}}
      nil ->
        {:exit, :noproc}
    end
  end

  defp checkout_call(pid, queue?, start, timeout) do
    mref = Process.monitor(pid)
    send(pid, {:db_connection, {self(), mref}, {:checkout, start, queue?}})
    receive do
      {:"ETS-TRANSFER", holder, owner, {^mref, ref}} ->
        Process.demonitor(mref, [:flush])
        deadline = start_deadline(timeout, owner, ref, holder, start)
        pool_ref = {owner, ref, deadline, holder}
        checkout_info(holder, pool_ref)
      {^mref, reply} ->
        Process.demonitor(mref, [:flush])
        reply
      {:DOWN, ^mref, _, _, reason} ->
        {:exit, reason}
    end
  end

  defp checkout_info(holder, pool_ref) do
    try do
      :ets.lookup(holder, @holder_key)
    rescue
      ArgumentError ->
        # Deadline could hit and by handled pool before using connection
        msg = "connection not available because deadline reached while in queue"
        {:error, DBConnection.ConnectionError.exception(msg)}
    else
      [{_, _, _, mod, state}] ->
        {:ok, pool_ref, mod, state}
    end
  end

  defp checkin_holder(holder, pool, state, msg) do
    try do
      :ets.update_element(holder, @holder_key, [{3, nil}, {5, state}])
      :ets.give_away(holder, pool, msg)
    rescue
      ArgumentError ->
        :ok
    else
      true ->
        :ok
    end
  end

  defp disconnect_holder(holder, err) do
    delete_holder(holder, &DBConnection.Connection.disconnect/4, err)
  end

  defp stop_holder(holder, err) do
    delete_holder(holder, &DBConnection.Connection.stop/4, err)
  end

  defp delete_holder(holder, stop, err) do
    [{_, conn, deadline, _, state}] = :ets.lookup(holder, @holder_key)
    :ets.delete(holder)
    cancel_deadline(deadline)
    stop.({conn, holder}, err, state, [])
  end
end
    