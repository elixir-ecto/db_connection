defmodule DBConnection.MetricsTest do
  use ExUnit.Case, async: false

  defmodule DummyDB do
    use DBConnection

    def start_link(opts), do: DBConnection.start_link(__MODULE__, opts)

    def connect(_opts), do: {:ok, %{}}
    def disconnect(_reason, _state), do: :ok
    def checkout(state), do: {:ok, state}
    def checkin(_, state), do: {:ok, state}
    def handle_execute(_request, _state, _opts), do: {:ok, %{result: "dummy result"}}

    def handle_begin(_, _), do: :ok
    def handle_close(_, _, _), do: :ok
    def handle_commit(_, _), do: :ok
    def handle_deallocate(_, _, _, _), do: :ok
    def handle_declare(_, _, _, _), do: :ok
    def handle_fetch(_, _, _, _), do: :ok
    def handle_prepare(_, _, _), do: :ok
    def handle_rollback(_, _), do: :ok
    def handle_status(_, _), do: :ok
    def handle_execute(_, _, _, _), do: :ok
    def ping(_), do: :ok
  end

  defmodule DelayedDummyDB do
    # Delegate to DummyDB for all functions, except for checkout/1
    defdelegate start_link(opts), to: DummyDB
    defdelegate disconnect(reason, state), to: DummyDB
    defdelegate checkin(state1, state2), to: DummyDB
    defdelegate handle_execute(request, state, opts), to: DummyDB
    defdelegate handle_begin(arg1, arg2), to: DummyDB
    defdelegate handle_close(arg1, arg2, arg3), to: DummyDB
    defdelegate handle_commit(arg1, arg2), to: DummyDB
    defdelegate handle_deallocate(arg1, arg2, arg3, arg4), to: DummyDB
    defdelegate handle_declare(arg1, arg2, arg3, arg4), to: DummyDB
    defdelegate handle_fetch(arg1, arg2, arg3, arg4), to: DummyDB
    defdelegate handle_prepare(arg1, arg2, arg3), to: DummyDB
    defdelegate handle_rollback(arg1, arg2), to: DummyDB
    defdelegate handle_status(arg1, arg2), to: DummyDB
    defdelegate handle_execute(arg1, arg2, arg3, arg4), to: DummyDB
    defdelegate ping(arg1), to: DummyDB

    def connect(opts), do: {:ok, %{test_pid: Keyword.get(opts, :test_pid)}}

    def checkout(%{test_pid: test_pid} = state) do
      process_pid = self()
      send(test_pid, {:checkout_request, process_pid})

      receive do
        :checkout -> :ok
      after
        1_000 -> throw("Checkout timeout")
      end

      DummyDB.checkout(state)
    end
  end

  test "pool starts up with correct number of metrics" do
    opts = [pool_size: 300]
    {:ok, pool} = DBConnection.ConnectionPool.start_link({DummyDB, opts})

    %{active: active, waiting: waiting} = DBConnection.ConnectionPool.get_connection_metrics(pool)
    assert active == 0
    assert waiting == 0
  end

  test "pool increments when a connection is checked out" do
    opts = [pool_size: 300]
    {:ok, pool} = DBConnection.ConnectionPool.start_link({DummyDB, opts})

    DBConnection.ConnectionPool.checkout(pool, [self()], [])
    %{active: active, waiting: waiting} = DBConnection.ConnectionPool.get_connection_metrics(pool)

    assert active == 1
    assert waiting == 0

    for _ <- 1..150, do: DBConnection.ConnectionPool.checkout(pool, [self()], [])

    %{active: active, waiting: waiting} = DBConnection.ConnectionPool.get_connection_metrics(pool)
    assert active == 151
    assert waiting == 0
  end

  test "pool increments and decrements when all connections are in use" do
    opts = [pool_size: 10]
    {:ok, pool} = DBConnection.ConnectionPool.start_link({DummyDB, opts})

    %{active: 0, waiting: 0} = DBConnection.ConnectionPool.get_connection_metrics(pool)

    test_pid = self()

    for _i <- 1..15 do
      spawn_with_delay(pool, test_pid)
    end

    # We should wait becouse &DBConnection.Holder.checkout/3 send message to pool server in unpredictable time
    # In most cases sleep time less than 1 ms, but it depends on hardware
    await_while(build_active_predicate(pool, 10), 1, 100)
    %{active: 10, waiting: 5} = DBConnection.ConnectionPool.get_connection_metrics(pool)
    checkin(7)

    await_while(build_active_predicate(pool, 8), 1, 100)
    %{active: 8, waiting: 0} = DBConnection.ConnectionPool.get_connection_metrics(pool)
    checkin(8)

    await_while(build_active_predicate(pool, 0), 1, 100)
    %{active: 0, waiting: 0} = DBConnection.ConnectionPool.get_connection_metrics(pool)
  end

  test "pool increments and decrements when the checkout function is slow" do
    test_pid = self()
    opts = [pool_size: 10, test_pid: test_pid]
    {:ok, pool} = DBConnection.ConnectionPool.start_link({DelayedDummyDB, opts})

    %{active: 0, waiting: 0} = DBConnection.ConnectionPool.get_connection_metrics(pool)

    for _i <- 1..15 do
      spawn_with_delay(pool, test_pid)
    end

    await_while(build_waiting_predicate(pool, 15), 1, 100)

    # We should checkout (pool_size = 10 = 6 + 4) connections when pool is starting
    checkout(4)
    await_while(build_waiting_predicate(pool, 11), 1, 100)
    %{active: 4, waiting: 11} = DBConnection.ConnectionPool.get_connection_metrics(pool)
    checkout(6)
    await_while(build_waiting_predicate(pool, 5), 1, 100)
    %{active: 10, waiting: 5} = DBConnection.ConnectionPool.get_connection_metrics(pool)

    # We should checkout and checkin as many connection as many task we have (in our case 15)
    # checkins 7 + 6 + 2 = 15
    # checkouts 3 + 3 + 2 + 7 = 15
    checkin(7)
    checkout(3)
    await_while(build_waiting_predicate(pool, 2), 1, 100)
    %{active: 10, waiting: 2} = DBConnection.ConnectionPool.get_connection_metrics(pool)
    checkin(6)
    checkout(3)
    checkout(2)
    await_while(build_waiting_predicate(pool, 0), 1, 100)
    %{active: 7, waiting: 0} = DBConnection.ConnectionPool.get_connection_metrics(pool)
    checkin(2)
    checkout(7)
    await_while(build_active_predicate(pool, 0), 1, 100)
    %{active: 0, waiting: 0} = DBConnection.ConnectionPool.get_connection_metrics(pool)
  end

  defp build_active_predicate(pool, expected_active) do
    fn ->
      %{active: active} = DBConnection.ConnectionPool.get_connection_metrics(pool)
      active == expected_active
    end
  end

  defp build_waiting_predicate(pool, expected_waiting) do
    fn ->
      %{waiting: waiting} = DBConnection.ConnectionPool.get_connection_metrics(pool)
      waiting == expected_waiting
    end
  end

  defp await_while(predicate, frequency, retrys) do
    if retrys == 0, do: throw("await timeout")
    Process.sleep(frequency)
    if predicate.(), do: :ok, else: await_while(predicate, frequency, retrys - 1)
  end

  defp checkout(n), do: Enum.each(1..n, fn _ -> checkout() end)

  defp checkout() do
    receive do
      {:checkout_request, process_pid} ->
        send(process_pid, :checkout)
    after
      1_000 -> throw("Not checked in")
    end
  end

  defp checkin(n), do: Enum.each(1..n, fn _ -> checkin() end)

  defp checkin() do
    process_pid =
      receive do
        {:checked_out, process_pid} ->
          send(process_pid, :checkin)
          process_pid
      after
        1_000 -> throw("Not checked in")
      end

    receive do
      {:DOWN, _, :process, ^process_pid, :normal} ->
        :ok
    after
      1_000 -> throw("Process hasn't finished yet")
    end
  end

  defp spawn_with_delay(pool, test_pid) do
    spawn_monitor(fn ->
      process_pid = self()
      DBConnection.ConnectionPool.checkout(pool, [self()], [])
      send(test_pid, {:checked_out, process_pid})

      receive do
        :checkin -> :ok
      after
        1_000 -> throw("Checkin timeout")
      end
    end)
  end
end
