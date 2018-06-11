defmodule DBConnectionTest do
  use ExUnit.Case, async: true

  alias TestConnection, as: C
  alias TestAgent, as: A

  test "start_link workflow with unregistered name" do
    stack = [{:ok, :state}]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent]
    {:ok, conn} = C.start_link(opts)

    {:links, links} = Process.info(self(), :links)
    assert conn in links

    _ = :sys.get_state(conn)

    assert A.record(agent) == [{:connect, [opts]}]
  end

  test "start_link workflow with registered name" do
    stack = [{:ok, :state}]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, name: :conn]
    {:ok, conn} = C.start_link(opts)

    assert Process.info(conn, :registered_name) == {:registered_name, :conn}

    _ = :sys.get_state(conn)

    assert A.record(agent) == [{:connect, [opts]}]
  end

 test "start_link with :sync_connect and raise returns error" do
    stack = [fn(_) -> raise "oops" end]
    {:ok, agent} = A.start_link(stack)

    Process.flag(:trap_exit, true)

    opts = [agent: agent, sync_connect: true]
    assert {:error, {%RuntimeError{}, [_|_]}} =
      C.start_link(opts)

    assert A.record(agent) == [{:connect, [opts]}]
  end

 test "start_link with :sync_connect, :error and backoff :stop returns error" do
    stack = [{:error, RuntimeError.exception("oops")}]
    {:ok, agent} = A.start_link(stack)

    Process.flag(:trap_exit, true)

    opts = [agent: agent, sync_connect: true, backoff_type: :stop]
    assert {:error, {%RuntimeError{message: "oops"}, [_|_]}} =
      C.start_link(opts)

    assert A.record(agent) == [{:connect, [opts]}]
  end

  test "start_link without :sync_connect does not block" do
    parent = self()
    stack = [fn(_) ->
        assert_receive {:hi, ^parent}
        send(parent, {:hi, self()})
        {:ok, :state}
    end]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, sync_connect: false]
    assert {:ok, conn} = C.start_link(opts)

    send(conn, {:hi, self()})
    assert_receive {:hi, ^conn}

    assert A.record(agent) == [{:connect, [opts]}]
  end
end
