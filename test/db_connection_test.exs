defmodule DBConnectionTest do
  use ExUnit.Case, async: true

  alias TestConnection, as: C
  alias TestAgent, as: A
  alias TestPool, as: P

  test "start_link workflow with unregistered name" do
    stack = [{:ok, :state}]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, pool_size: 1]
    {:ok, conn} = C.start_link(opts)

    {:links, links} = Process.info(self(), :links)
    assert conn in links

    _ = :sys.get_state(conn)
    assert A.record(agent) == [{:connect, [[pool_index: 1] ++ opts]}]
  end

  test "start_link workflow with registered name" do
    stack = [{:ok, :state}]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, name: :conn, pool_size: 1]
    {:ok, conn} = C.start_link(opts)

    assert Process.info(conn, :registered_name) == {:registered_name, :conn}

    _ = :sys.get_state(conn)
    assert A.record(agent) == [{:connect, [[pool_index: 1] ++ opts]}]
  end

  describe "connection_module/1" do
    test "returns the connection module when given a pool pid" do
      {:ok, pool} = P.start_link([])
      assert {:ok, TestConnection} = DBConnection.connection_module(pool)
    end

    test "returns the connection module when given a pool name", %{test: name} do
      {:ok, _pool} = P.start_link(name: name)
      assert {:ok, TestConnection} = DBConnection.connection_module(name)
    end

    test "returns the connection module when given a locked connection reference" do
      {:ok, agent} = A.start_link([{:ok, :state}, {:idle, :state}, {:idle, :state}])

      opts = [agent: agent]
      {:ok, pool} = P.start_link(opts)

      P.run(pool, fn conn ->
        assert {:ok, TestConnection} = DBConnection.connection_module(conn)
      end)
    end

    test "returns an error if the given process is not a pool" do
      assert :error = DBConnection.connection_module(self())
    end
  end
end
