defmodule DBConnection.ConnectionPool.PoolTest do
  use ExUnit.Case

  alias DBConnection.ConnectionPool.Pool

  describe "Pool initialization" do
    test "raises ArgumentError when pool_size is less than 1" do
      assert {:error,
              {%ArgumentError{message: "Pool size must be greater or equal to 1. Got 0."}, _stack}} =
               Pool.start_supervised(:test, YourConnectionModule, pool_size: 0)
    end

    test "does not raise error when pool_size is 1" do
      assert {:ok, _pid} = Pool.start_supervised(:test, YourConnectionModule, pool_size: 1)
    end

    test "does not raise error when pool_size is greater than 1" do
      assert {:ok, _pid} = Pool.start_supervised(:test, YourConnectionModule, pool_size: 2)
    end
  end
end
