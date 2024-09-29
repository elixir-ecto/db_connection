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

  describe "available_start_options/0" do
    test "returns all available start_link/2 options" do
      assert DBConnection.available_start_options() == [
               :after_connect,
               :after_connect_timeout,
               :connection_listeners,
               :backoff_max,
               :backoff_min,
               :backoff_type,
               :configure,
               :idle_interval,
               :idle_limit,
               :max_restarts,
               :max_seconds,
               :name,
               :pool,
               :pool_size,
               :queue_interval,
               :queue_target,
               :show_sensitive_data_on_connection_error
             ]
    end
  end

  describe "available_connection_options/0" do
    test "returns all available function options" do
      assert DBConnection.available_connection_options() == [:log, :queue, :timeout, :deadline]
    end
  end

  describe "connection_module/1" do
    setup do
      {:ok, agent} = A.start_link([{:ok, :state}, {:idle, :state}, {:idle, :state}])
      [agent: agent]
    end

    test "returns the connection module when given a pool pid", %{agent: agent} do
      {:ok, pool} = P.start_link(agent: agent)
      assert {:ok, TestConnection} = DBConnection.connection_module(pool)
    end

    test "returns the connection module when given a pool name", %{test: name, agent: agent} do
      {:ok, _pool} = P.start_link(name: name, agent: agent)
      assert {:ok, TestConnection} = DBConnection.connection_module(name)
    end

    test "returns the connection module when given a locked connection reference", %{agent: agent} do
      {:ok, pool} = P.start_link(agent: agent)

      P.run(pool, fn conn ->
        assert {:ok, TestConnection} = DBConnection.connection_module(conn)
      end)
    end

    test "returns an error if the given process is not a pool" do
      assert :error = DBConnection.connection_module(self())
    end
  end
end
