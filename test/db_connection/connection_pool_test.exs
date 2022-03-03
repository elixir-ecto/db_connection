defmodule DBConnection.ConnectionPoolTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P

  describe "connection_module/1" do
    test "returns the connection module when given a pool pid" do
      {:ok, pool} = P.start_link([])
      assert {:ok, TestConnection} = DBConnection.ConnectionPool.connection_module(pool)
    end

    test "returns the connection module when given a pool name", %{test: name} do
      {:ok, _pool} = P.start_link(name: name)
      assert {:ok, TestConnection} = DBConnection.ConnectionPool.connection_module(name)
    end

    test "returns an error if the given process is not a pool" do
      assert :error = DBConnection.ConnectionPool.connection_module(self())
    end
  end
end
