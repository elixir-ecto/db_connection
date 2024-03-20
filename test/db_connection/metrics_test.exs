defmodule DBConnection.MetricsTest do
  use ExUnit.Case, async: false
  import DBConnection.ConnectionPool, only: [get_connection_metrics: 1]

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
        100 -> throw("Checkout timeout")
      end

      DummyDB.checkout(state)
    end
  end

  defmodule DelayedPoolManager do
    defmodule State do
      defstruct [
        :queued,
        :free_connections,
        :checkouted,
        :checkined,
        :test_pid,
        :pool,
        :pool_size,
        :confirmed_checkouts
      ]
    end

    def new(test_pid, pool, pool_size) do
      await_checkout_requests(
        %State{
          queued: 0,
          checkouted: 0,
          checkined: 0,
          free_connections: [],
          test_pid: test_pid,
          pool: pool,
          pool_size: pool_size,
          confirmed_checkouts: []
        },
        pool_size
      )
    end

    def queue(%State{queued: queued} = state, n) do
      for _ <- 1..n, do: spawn_with_delay(state)

      # We wait for tasks to queue
      Process.sleep(5)
      %State{state | queued: queued + n}
    end

    def checkout(
          %State{free_connections: free_connections, checkouted: checkouted, queued: queued} =
            state,
          n
        ) do
      free_connections_len = length(free_connections)

      if n > free_connections_len || n > queued - checkouted do
        throw("To many checkout requests #{n}")
      end

      {processes_to_checkout, new_free_connections} = Enum.split(free_connections, n)
      Enum.each(processes_to_checkout, &send(&1, :checkout))
      new_state = await_checkout_confirmation(state, n)
      %State{new_state | free_connections: new_free_connections, checkouted: checkouted + n}
    end

    def checkin(%State{confirmed_checkouts: confirmed_checkouts, checkined: checkined} = state, n) do
      confirmed_checkouts_len = length(confirmed_checkouts)

      if n > confirmed_checkouts_len do
        throw("To many confirmed checkouts")
      end

      {processes_to_checkin, new_confirmed_checkouts} = Enum.split(confirmed_checkouts, n)
      Enum.each(processes_to_checkin, &await_checkin_with_confirmation/1)
      # Unfortunately we haven't a way to fully confirm a checkin so we have to wait
      Process.sleep(5)

      new_state = %State{
        state
        | confirmed_checkouts: new_confirmed_checkouts,
          checkined: checkined + n
      }

      await_checkout_requests(new_state, n)
    end

    defp await_checkin_with_confirmation(process_pid) do
      send(process_pid, :checkin)

      receive do
        {:DOWN, _, :process, ^process_pid, :normal} ->
          :ok
      after
        5 -> throw("Process hasn't finished yet")
      end
    end

    defp await_checkout_confirmation(%State{confirmed_checkouts: confirmed_checkouts} = state, n) do
      new_confirmed_checkouts =
        Enum.map(1..n, fn _ ->
          receive do
            {:checked_out, process_pid} ->
              process_pid
          after
            100 -> throw("Not checked in")
          end
        end)

      %State{state | confirmed_checkouts: confirmed_checkouts ++ new_confirmed_checkouts}
    end

    defp await_checkout_requests(%State{free_connections: free_connections} = state, n) do
      new_free_connections =
        Enum.map(1..n, fn i ->
          receive do
            {:checkout_request, process_pid} ->
              process_pid
          after
            100 -> throw("Not requested #{i} #{n}")
          end
        end)

      %State{state | free_connections: free_connections ++ new_free_connections}
    end

    defp spawn_with_delay(%State{pool: pool, test_pid: test_pid}) do
      spawn_monitor(fn ->
        process_pid = self()
        _conn = DBConnection.ConnectionPool.checkout(pool, [process_pid], [])
        send(test_pid, {:checked_out, process_pid})

        receive do
          :checkin -> :ok
        after
          1_000 -> throw("Checkin timeout")
        end
      end)
    end
  end

  test "pool starts up with correct number of metrics" do
    opts = [pool_size: 300]
    {:ok, pool} = DBConnection.ConnectionPool.start_link({DummyDB, opts})

    %{active: active, waiting: waiting} = get_connection_metrics(pool)
    assert active == 0
    assert waiting == 0
  end

  test "pool increments when a connection is checked out" do
    opts = [pool_size: 300]
    {:ok, pool} = DBConnection.ConnectionPool.start_link({DummyDB, opts})

    DBConnection.ConnectionPool.checkout(pool, [self()], [])
    %{active: active, waiting: waiting} = get_connection_metrics(pool)

    assert active == 1
    assert waiting == 0

    for _ <- 1..150, do: DBConnection.ConnectionPool.checkout(pool, [self()], [])

    %{active: active, waiting: waiting} = get_connection_metrics(pool)
    assert active == 151
    assert waiting == 0
  end

  test "pool increments and decrements when the checkout function is slow one connection" do
    test_pid = self()
    pool_size = 10
    opts = [pool_size: pool_size, test_pid: test_pid]
    {:ok, pool} = DBConnection.ConnectionPool.start_link({DelayedDummyDB, opts})

    manager = DelayedPoolManager.new(test_pid, pool, pool_size)

    %{active: 0, waiting: 0} = get_connection_metrics(pool)

    manager = DelayedPoolManager.queue(manager, 1)
    %{active: 0, waiting: 1} = get_connection_metrics(pool)
    manager = DelayedPoolManager.checkout(manager, 1)
    %{active: 1, waiting: 0} = get_connection_metrics(pool)
    _manager = DelayedPoolManager.checkin(manager, 1)
    %{active: 0, waiting: 0} = get_connection_metrics(pool)
  end

  test "pool increments and decrements when the checkout function is slow few connection" do
    test_pid = self()
    pool_size = 10
    opts = [pool_size: pool_size, test_pid: test_pid]
    {:ok, pool} = DBConnection.ConnectionPool.start_link({DelayedDummyDB, opts})

    manager = DelayedPoolManager.new(test_pid, pool, pool_size)

    %{active: 0, waiting: 0} = get_connection_metrics(pool)

    manager = DelayedPoolManager.queue(manager, 3)
    %{active: 0, waiting: 3} = get_connection_metrics(pool)
    manager = DelayedPoolManager.checkout(manager, 2)
    %{active: 2, waiting: 1} = get_connection_metrics(pool)
    manager = DelayedPoolManager.checkin(manager, 1)
    %{active: 1, waiting: 1} = get_connection_metrics(pool)
    manager = DelayedPoolManager.checkout(manager, 1)
    %{active: 2, waiting: 0} = get_connection_metrics(pool)
    _manager = DelayedPoolManager.checkin(manager, 2)
    %{active: 0, waiting: 0} = get_connection_metrics(pool)
  end

  test "pool increments and decrements when the checkout function is slow many connections" do
    test_pid = self()
    pool_size = 10
    opts = [pool_size: pool_size, test_pid: test_pid]
    {:ok, pool} = DBConnection.ConnectionPool.start_link({DelayedDummyDB, opts})

    manager = DelayedPoolManager.new(test_pid, pool, pool_size)

    %{active: 0, waiting: 0} = get_connection_metrics(pool)

    manager = DelayedPoolManager.queue(manager, 15)
    %{active: 0, waiting: 15} = get_connection_metrics(pool)

    # 15 = (checkouts) 4 + 6 + 3 + 2 = (checkins) 1 + 7 + 5 + 2
    manager = DelayedPoolManager.checkout(manager, 4)
    %{active: 4, waiting: 11} = get_connection_metrics(pool)
    manager = DelayedPoolManager.checkin(manager, 1)
    %{active: 3, waiting: 11} = get_connection_metrics(pool)
    manager = DelayedPoolManager.checkout(manager, 6)
    %{active: 9, waiting: 5} = get_connection_metrics(pool)
    manager = DelayedPoolManager.checkin(manager, 7)
    %{active: 2, waiting: 5} = get_connection_metrics(pool)
    manager = DelayedPoolManager.checkout(manager, 3)
    %{active: 5, waiting: 2} = get_connection_metrics(pool)
    manager = DelayedPoolManager.checkin(manager, 5)
    %{active: 0, waiting: 2} = get_connection_metrics(pool)
    manager = DelayedPoolManager.checkout(manager, 2)
    %{active: 2, waiting: 0} = get_connection_metrics(pool)
    _manager = DelayedPoolManager.checkin(manager, 2)
    %{active: 0, waiting: 0} = get_connection_metrics(pool)
  end

  test "pool increments and decrements when the checkout function is slow dynamic connections" do
    test_pid = self()
    pool_size = 10
    opts = [pool_size: pool_size, test_pid: test_pid]
    {:ok, pool} = DBConnection.ConnectionPool.start_link({DelayedDummyDB, opts})

    manager = DelayedPoolManager.new(test_pid, pool, pool_size)

    %{active: 0, waiting: 0} = get_connection_metrics(pool)

    manager = DelayedPoolManager.queue(manager, 15)
    %{active: 0, waiting: 15} = get_connection_metrics(pool)

    manager = DelayedPoolManager.checkout(manager, 4)
    %{active: 4, waiting: 11} = get_connection_metrics(pool)
    manager = DelayedPoolManager.checkin(manager, 1)
    %{active: 3, waiting: 11} = get_connection_metrics(pool)
    manager = DelayedPoolManager.checkout(manager, 6)
    %{active: 9, waiting: 5} = get_connection_metrics(pool)
    manager = DelayedPoolManager.checkin(manager, 7)
    %{active: 2, waiting: 5} = get_connection_metrics(pool)
    manager = DelayedPoolManager.checkout(manager, 3)
    %{active: 5, waiting: 2} = get_connection_metrics(pool)

    manager = DelayedPoolManager.queue(manager, 15)
    %{active: 5, waiting: 17} = get_connection_metrics(pool)
    manager = DelayedPoolManager.checkin(manager, 5)
    %{active: 0, waiting: 17} = get_connection_metrics(pool)
    manager = DelayedPoolManager.checkout(manager, 8)
    %{active: 8, waiting: 9} = get_connection_metrics(pool)
    manager = DelayedPoolManager.checkin(manager, 8)
    %{active: 0, waiting: 9} = get_connection_metrics(pool)
    manager = DelayedPoolManager.checkout(manager, 9)
    %{active: 9, waiting: 0} = get_connection_metrics(pool)
    _manager = DelayedPoolManager.checkin(manager, 9)
    %{active: 0, waiting: 0} = get_connection_metrics(pool)
  end
end
