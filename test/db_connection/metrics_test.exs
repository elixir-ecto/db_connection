defmodule MetricsTest do
  use ExUnit.Case, async: true

  defmodule DummyDB do
    use DBConnection

    def start_link(opts) do
      DBConnection.start_link(__MODULE__, opts)
    end

    def connect(_opts) do
      {:ok, %{connected: true}}
    end

    def disconnect(_reason, _state) do
      :ok
    end

    def checkout(state) do
      {:ok, state}
    end

    def checkin(_state, state) do
      {:ok, state}
    end

    def handle_execute(_request, _state, _opts) do
      {:ok, %{result: "dummy result"}}
    end
  end


  test "pool starts up with correct number of metrics" do
    tag = :test_pool
    mod = TestPool
    opts = [pool_size: 300]
    {:ok, pool} = DBConnection.ConnectionPool.start_link({DummyDB, opts})
    DBConnection.ConnectionPool.get_connection_metrics(pool) |> IO.inspect()
    :ok = GenServer.stop(pool)
  end
end
