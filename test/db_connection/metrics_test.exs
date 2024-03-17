defmodule DBConnection.MetricsTest do
  use ExUnit.Case, async: false

  defmodule DummyDB do
    use DBConnection

    def start_link(opts), do: DBConnection.start_link(__MODULE__, opts)

    def connect(_opts), do: {:ok, %{connected: true}}
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
    defdelegate connect(opts), to: DummyDB
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

    def checkout(state) do
      :timer.sleep(1000)  # Simulate a delay
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
    {:ok, pool} = DBConnection.ConnectionPool.start_link({DelayedDummyDB, opts})
    :timer.sleep(2000) # definitely enough for everything to startup
    %{active: 0, waiting: 0} = DBConnection.ConnectionPool.get_connection_metrics(pool)

    for _ <- 1..15 do
      spawn(fn ->
        DBConnection.ConnectionPool.checkout(pool, [self()], [])
      end)
    end

    # waiting queue works
    :timer.sleep(200) # still less than the timeout
    %{active: active, waiting: waiting} = DBConnection.ConnectionPool.get_connection_metrics(pool)

    assert active == 10
    assert waiting == 5

    :timer.sleep(1000)
    # active ones finish
    %{active: active, waiting: waiting} = DBConnection.ConnectionPool.get_connection_metrics(pool)

    assert active == 5
    assert waiting == 0
  end

  test "pool should work without setting" do
    opts = []
    {:ok, pool} = DBConnection.ConnectionPool.start_link({DelayedDummyDB, opts})
    :timer.sleep(2000) # definitely enough for everything to startup
    %{active: 0, waiting: 0} = DBConnection.ConnectionPool.get_connection_metrics(pool)

    for _ <- 1..15 do
      spawn(fn ->
        DBConnection.ConnectionPool.checkout(pool, [self()], [])
      end)
    end

    # waiting queue works
    :timer.sleep(200) # still less than the timeout
    %{active: active, waiting: waiting} = DBConnection.ConnectionPool.get_connection_metrics(pool)

    assert active == 1
    assert waiting == 14
  end
end
