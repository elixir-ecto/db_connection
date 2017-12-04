defmodule DBConnection.ConnectionPool do

  alias DBConnection.Ownership.PoolSupervisor

  @behaviour DBConnection.Pool
  use GenServer

  @timeout 5000
  @queue true
  @queue_target 50
  @queue_interval 1000

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
    now = System.monotonic_time(:milliseconds)
    with pid when node(pid) == node() <- GenServer.whereis(pool) do
      queue? = Keyword.get(opts, :queue, @queue)
      # Technically its possible for caller to exit between starting
      # timer and sending message in call. This leaks the timer but should
      # be so rare and last a very short time period.
      deadline = start_deadline(pid, now, opts)
      try do
        GenServer.call(pid, {:checkout, now, queue?, deadline}, :infinity)
      catch
        :exit, {reason, {:gen_statem, :call, [^pid | _]}} ->
          exit({reason, {__MODULE__, :checkout, [pool, opts]}})
      end
    else
      nil ->
        exit({:noproc, {__MODULE__, :checkout, [pool, opts]}})
      {_, node} ->
        exit({{:badnode, node}, {__MODULE__, :checkout, [pool, opts]}})
      pid ->
        exit({{:badnode, node(pid)}, {__MODULE__, :checkout, [pool, opts]}})
    end
  end

  @doc false
  def checkin({pid, {deadline, mon}}, conn, _) do
    cancel_deadline(deadline)
    now = System.monotonic_time(:milliseconds)
    GenServer.cast(pid, {:checkin, now, mon, conn})
  end

  @doc false
  def disconnect({pid, {deadline, mon}}, err, conn, _) do
    cancel_deadline(deadline)
    GenServer.cast(pid, {:disconnect, mon, err, conn})
  end

  @doc false
  def stop({pid, {deadline, mon}}, err, conn, _) do
    cancel_deadline(deadline)
    GenServer.cast(pid, {:stop, mon, err, conn})
  end

  ## GenServer api
    
  def init({mod, opts}) do
    queue = :ets.new(__MODULE__, [:private, :ordered_set])
    {:ok, pool, _} = PoolSupervisor.start_pool(mod, pool_opts(opts, queue))
    target = Keyword.get(opts, :queue_target, @queue_target)
    interval = Keyword.get(opts, :queue_interval, @queue_interval)
    state = %{queue: queue, pool: pool, target: target, interval: interval,
              delay: 0, slow: false, next: System.monotonic_time(:milliseconds),
              client: nil, conn: nil}
    {:ok, state}
  end

  def handle_call({:checkout, time, queue?, deadline}, {pid, _} = from, s) do
    case s do
      %{client: nil, conn: {:ok, _pool_ref, _mod, _state} = conn} ->
        client = {deadline, Process.monitor(pid)}
        {:reply, put_elem(conn, 1, {self(), client}), %{s | client: client}}
      %{client: nil, conn: {:error, _} = error} ->
        cancel_deadline(deadline)
        {:reply, error, s}
      %{queue: queue} when queue? == true ->
        client = {deadline, Process.monitor(pid)}
        # from is {pid, ref} so order could favor certain pid(s)
        :ets.insert(queue, {{time, from}, client})
        {:noreply, s}
      s when queue? == false ->
        message = "connection not available and queuing is disabled"
        err = DBConnection.ConnectionError.exception(message)
        {:reply, {:error, err}, s}
    end
  end

  def handle_cast({:checkin, time, mon, state}, s) do
    case s do
      %{client: {_deadline, ^mon}, conn: {:ok, _pool_ref, _mod, _state} = conn} ->
        result = dequeue(time, put_elem(conn, 3, state), s)
        Process.demonitor(mon, [:flush])
        result
      %{} ->
        {:noreply, s}
    end
  end

  def handle_cast({:disconnect, mon, err, state}, s) do
    abort(&DBConnection.Connection.disconnect/4, mon, err, state, s)
  end

  def handle_cast({:stop, mon, err, state}, s) do
    abort(&DBConnection.Connection.stop/4, mon, err, state, s)
  end

  def handle_cast({:connected, time, queue}, %{queue: queue, conn: conn} = s)
      when conn == nil or elem(conn, 0) == :error do
    # TODO: connection process must send the state async so that pool never blocks.
    case DBConnection.Connection.checkout(s.pool, [queue: false, timeout: :infinity]) do
      {:ok, _pool_ref, _mod, _state} = conn ->
        dequeue(time, conn, %{s | conn: conn})
      {:error, _} = error ->
        error(queue, error)
        {:noreply, %{s | conn: error}}
    end
  end

  def handle_info({:DOWN, mon, _, pid, reason}, s) do
    case s do
      %{client: {deadline, ^mon}} ->
        cancel_deadline(deadline)
        message = "client #{inspect pid} exited with: " <> Exception.format_exit(reason)
        err = DBConnection.ConnectionError.exception(message)
        fail(err, s)
      %{} ->
        down(mon, s)
    end
  end

  def handle_info({:timeout, deadline, {pid, sent, time}}, s) do
    case s do
      %{client: {^deadline, mon}} ->
        Process.demonitor(mon, [:flush])
        message = "client #{inspect pid} timed out because " <>
            "it queued and checked out the connection for longer than #{time-sent}ms"
        err = DBConnection.ConnectionError.exception(message)
        fail(err, s)
      %{} ->
        timeout(deadline, sent, time, s)
    end
  end

  defp dequeue(time, conn, s) do
    case s do
      %{next: next, delay: delay, target: target} when time > next  ->
        dequeue_first(time, delay > target, conn, s)
      %{slow: false, queue: queue, delay: delay} ->
        dequeue_fast(time, queue, delay, conn, s)
      %{slow: true, queue: queue, delay: delay, target: target} ->
        dequeue_slow(time, queue, delay, target * 2, conn, s)
    end
  end

  defp dequeue_first(time, slow?, conn, s) do
    %{queue: queue, interval: interval} = s
    next = time + interval
    case :ets.first(queue) do
      {sent, from} = key ->
        client = go(queue, key, from, conn)
        delay = time - sent
        {:noreply, %{s | next: next, delay: delay, slow: slow?, client: client,
                         conn: conn}}
      :"$end_of_table" ->
        {:noreply, %{s | next: next, delay: 0, slow: slow?, client: nil,
                         conn: conn}}
    end
  end

  defp dequeue_fast(time, queue, delay, conn, s) do
    case :ets.first(queue) do
      {sent, from} = key ->
        delay = min(time - sent, delay)
        client = go(queue, key, from, conn)
        {:noreply, %{s | delay: delay, client: client, conn: conn}}
      :"$end_of_table" ->
        {:noreply, %{s | delay: 0, client: nil, conn: conn}}
    end     
  end

  defp dequeue_slow(time, queue, min_delay, timeout, conn, s) do
    with {sent, from} = key <- :ets.first(queue) do
      case time - sent do
        delay when delay > timeout ->
          drop(queue, key, from, delay)
          dequeue_slow(time, queue, min(delay, min_delay), timeout, conn, s)
        delay ->
          client = go(queue, key, from, conn)
          {:noreply, %{s | delay: min(delay, min_delay), client: client,
                           conn: conn}}
        end
    else
      :"$end_of_table" ->
        {:noreply, %{s | delay: 0, client: nil, conn: conn}}
    end
  end

  defp go(queue, key, from, conn) do
    [{_, client}] = :ets.take(queue, key)
    GenServer.reply(from, put_elem(conn, 1, {self(), client}))
    client
  end

  defp drop(queue, key, from, sojourn) do
    message = "connection not available " <>
      "and request was dropped from queue after #{sojourn}ms"
    err = DBConnection.ConnectionError.exception(message)
    error(queue, key, from, err)
  end
  
  defp error(queue, key, from, err) do
    [{_, {deadline, mon}}] = :ets.take(queue, key)
    GenServer.reply(from, {:error, err})
    Process.demonitor(mon, [:flush])
    cancel_deadline(deadline)
  end

  defp error(queue, err) do
    case :ets.first(queue) do
      {_, from} = key ->
        error(queue, key, from, err)
      :"$end_of_table" ->
        :ok
    end
  end

  defp abort(fun, mon, err, state, s) do
    case s do
      %{client: {_deadline, ^mon}, conn: {:ok, pool_ref, _, _}} ->
        Process.demonitor(mon, [:flush])
        fun.(pool_ref, err, state, [])
        {:noreply, %{s | client: nil, conn: nil}}
      %{} ->
        {:noreply, s}
    end
  end

  defp fail(err, %{conn: {:ok, pool_ref, _, state}} = s) do
    DBConnection.Connection.disconnect(pool_ref, err, state, [])
    {:noreply, %{s | client: nil, conn: nil}}
  end

  defp down(mon, %{queue: queue} = s) do
    case :ets.match_object(queue, {:_, {:_, mon}}, 1) do
      {[{key, {deadline, _}}], _cont} ->
        cancel_deadline(deadline)
        :ets.delete(queue, key)
        {:noreply, s}
      :"$end_of_table"->
        {:noreply, s}
    end
  end

  defp timeout(deadline, sent, time, %{queue: queue} = s) do
    case :ets.match_object(queue, {{sent, :_}, {deadline, :_}}) do
      [{{_, from} = key, {_, mon}}] ->
        message = "connection not available " <>
          "and request was dropped from queue after #{time-sent}ms"
        err = DBConnection.ConnectionError.exception(message)
        GenServer.reply(from, {:error, err})
        Process.demonitor(mon, [:flush])
        :ets.delete(queue, key)
        {:noreply, s}
      [] ->
        {:noreply, s}
    end
  end

  defp start_opts(opts) do
    Keyword.take(opts, [:name, :spawn_opt])
  end

  defp start_deadline(pid, now, opts) do
    case abs_timeout(now, opts) do
      nil ->
        nil
      timeout ->
        :erlang.start_timer(timeout, pid, {self(), now, timeout}, [abs: true])
    end
  end

  defp abs_timeout(now, opts) do
    case Keyword.get(opts, :timeout, @timeout) do
      :infinity ->
        Keyword.get(opts, :deadline)
      timeout ->
        min(now + timeout, Keyword.get(opts, :deadline))
    end
  end

  defp cancel_deadline(deadline) do
    :erlang.cancel_timer(deadline, [async: true, info: false])
  end

  defp pool_opts(opts, ref) do
    # TODO: Use a pool of size > 1
      opts
      |> after_connect_hook(ref)
      |> Keyword.put(:pool, DBConnection.Connection)
      |> Keyword.put(:sync_connect, false)
      |> Keyword.put(:idle, :passive)
  end

  defp after_connect_hook(opts, ref) do
    Keyword.update(opts, :after_connect,
      {__MODULE__, :after_connect, [self(), ref]}, &{__MODULE__, :after_connect, [self(), ref, &1]})
  end

  def after_connect(conn, pool, ref, fun \\ fn _ -> :ok end) do
    res = apply_fun(fun, conn)
    GenServer.cast(pool, {:connected, System.monotonic_time(:milliseconds), ref})
    res
  end

  defp apply_fun(fun, conn) when is_function(fun, 1) do
    fun.(conn)
  end
  defp apply_fun({mod, fun, args}, conn) do
    apply(mod, fun, [conn | args])
  end
end
    