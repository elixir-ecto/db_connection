defmodule DBConnection.Holder do
  @moduledoc false
  require Record

  @queue true
  @timeout 5000
  @time_unit 1000

  Record.defrecord :conn, [:connection, :deadline, :module, :state]
  Record.defrecord :pool_ref, [:pool, :reference, :deadline, :holder]
  
  @type t :: :ets.tid()

  @spec new(pid, reference, module, term) :: t
  def new(pool, ref, mod, state) do
    # Insert before setting heir so that pool can't receive empty table
    holder = :ets.new(__MODULE__, [:public, :ordered_set])
    :true = :ets.insert_new(holder, conn(connection: self(), deadline: nil, module: mod, state: state))
    :ets.setopts(holder, {:heir, pool, ref})
    holder
  end

  @callback checkout(pool :: GenServer.server, opts :: Keyword.t) ::
    {:ok, pool_ref :: any, module, state :: any} | {:error, Exception.t}
  def checkout(pool, opts) do
    caller = Keyword.get(opts, :caller, self())
    queue? = Keyword.get(opts, :queue, @queue)
    now = System.monotonic_time(@time_unit)
    timeout = abs_timeout(now, opts)
    case checkout(pool, caller, queue?, now, timeout) do
      {:ok, _, _, _} = ok ->
        ok
      {:error, _} = error ->
        error
      {:exit, reason} ->
        exit({reason, {__MODULE__, :checkout, [pool, opts]}})
    end
  end

  @spec checkin(pool_ref :: any, state :: any, opts :: Keyword.t) :: :ok
  def checkin(pool_ref, state, _) do
    now = System.monotonic_time(@time_unit)
    done(pool_ref, :checkin, state, now)
  end

  @spec disconnect(pool_ref :: any, err :: Exception.t, state :: any, opts :: Keyword.t) :: :ok
  def disconnect(pool_ref, err, state, _) do
    done(pool_ref, :disconnect, state, err)
  end

  @spec stop(pool_ref :: any, err :: Exception.t, state :: any, opts :: Keyword.t) :: :ok
  def stop(pool_ref, err, state, _) do
    done(pool_ref, :stop, state, err)
  end

  @spec update(pid, reference, module, term) :: t
  def update(pool, ref, mod, state) do
    holder = new(pool, ref, mod, state)
    now = System.monotonic_time(@time_unit)
    :ets.give_away(holder, pool, {:checkin, ref, now})
    holder
  end

  @spec get_state(t) :: term
  def get_state(holder) do
    :ets.lookup_element(holder, :conn, conn(:state)+1)
  end

  @spec handle_checkout(t, {pid, reference}, reference) :: boolean
  def handle_checkout(holder, {pid, mref}, ref) do
    :ets.give_away(holder, pid, {mref, ref})
  rescue
    ArgumentError ->
      # pid is not alive
      false
  end

  @spec handle_deadline(t, reference) :: boolean
  def handle_deadline(holder, deadline) do
    :ets.lookup_element(holder, :conn, conn(:deadline)+1)
  rescue
    ArgumentError ->
      false
  else
    ^deadline ->
      true
    _ ->
      false
  end

  @spec handle_ping(t) :: boolean
  def handle_ping(holder) do
    :ets.lookup(holder, :conn)
  rescue
    ArgumentError ->
      false
  else
    [conn(connection: conn, state: state)] ->
      DBConnection.Connection.ping({conn, holder}, state)
      :ets.delete(holder)
  end

  @spec handle_disconnect(t, Exception.t) :: :ok
  def handle_disconnect(holder, err) do
    handle_done(holder, &DBConnection.Connection.disconnect/4, err)
  end

  @spec handle_stop(t, term) :: :ok
  def handle_stop(holder, err) do
    handle_done(holder, &DBConnection.Connection.stop/4, err)
  end

  defp checkout(pool, caller, queue?, start, timeout) do
    case GenServer.whereis(pool) do
      pid when node(pid) == node() ->
        checkout_call(pid, caller, queue?, start, timeout)
      pid when node(pid) != node() ->
        {:exit, {:badnode, node(pid)}}
      {_name, node} ->
        {:exit, {:badnode, node}}
      nil ->
        {:exit, :noproc}
    end
  end

  defp checkout_call(pid, caller, queue?, start, timeout) do
    mref = Process.monitor(pid)
    send(pid, {:db_connection, {self(), mref}, {:checkout, caller, start, queue?}})
    receive do
      {:"ETS-TRANSFER", holder, pool, {^mref, ref}} ->
        Process.demonitor(mref, [:flush])
        deadline = start_deadline(timeout, pool, ref, holder, start)
        pool_ref = pool_ref(pool: pool, reference: ref, deadline: deadline, holder: holder)
        checkout_result(holder, pool_ref)
      {^mref, reply} ->
        Process.demonitor(mref, [:flush])
        reply
      {:DOWN, ^mref, _, _, reason} ->
        {:exit, reason}
    end
  end

  defp checkout_result(holder, pool_ref) do
    try do
      :ets.lookup(holder, :conn)
    rescue
      ArgumentError ->
        # Deadline could hit and be handled pool before using connection
        msg = "connection not available because deadline reached while in queue"
        {:error, DBConnection.ConnectionError.exception(msg)}
    else
      [conn(module: mod, state: state)] ->
        {:ok, pool_ref, mod, state}
    end
  end

  defp done(pool_ref(pool: pool, reference: ref, deadline: deadline, holder: holder), tag, state, info) do
    cancel_deadline(deadline)
    try do
      :ets.update_element(holder, :conn, [{conn(:deadline)+1, nil}, {conn(:state)+1, state}])
      :ets.give_away(holder, pool, {tag, ref, info})
    rescue
      ArgumentError ->
        :ok
    else
      true ->
        :ok
    end
  end

  defp handle_done(holder, stop, err) do
    :ets.lookup(holder, :conn)
  rescue
    ArgumentError ->
      false
  else
    [conn(connection: pid, deadline: deadline, state: state)] ->
      cancel_deadline(deadline)
      :ets.delete(holder)
      stop.({pid, holder}, err, state, [])
      true
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
    :ets.update_element(holder, :conn, {conn(:deadline)+1, deadline})
    deadline
  end

  defp cancel_deadline(nil) do
    :ok
  end

  defp cancel_deadline(deadline) do
    :erlang.cancel_timer(deadline, [async: true, info: false])
  end
end