defmodule DBConnectionTest do
  use ExUnit.Case, async: true

  alias TestConnection, as: C
  alias TestAgent, as: A

  test "__using__ defaults" do
    defmodule Sample do
      use DBConnection
    end

    try do
      assert_raise RuntimeError, "connect/1 not implemented",
        fn() -> Sample.connect([]) end

      assert_raise RuntimeError, "disconnect/2 not implemented",
        fn() -> Sample.disconnect(RuntimeError.exception("oops"), []) end

      assert_raise RuntimeError, "checkout/1 not implemented",
        fn() -> Sample.checkout(:state) end

      assert_raise RuntimeError, "checkin/1 not implemented",
        fn() -> Sample.checkin(:state) end

      assert Sample.ping(:state) == {:ok, :state}

      assert_raise RuntimeError, "handle_begin/2 not implemented",
        fn() -> Sample.handle_begin([], :state) end

      assert_raise RuntimeError, "handle_commit/2 not implemented",
        fn() -> Sample.handle_commit([], :state) end

      assert_raise RuntimeError, "handle_rollback/2 not implemented",
        fn() -> Sample.handle_rollback([], :state) end

      assert_raise RuntimeError, "handle_prepare/3 not implemented",
        fn() -> Sample.handle_prepare(:query, [], :state) end

      assert_raise RuntimeError, "handle_execute/4 not implemented",
        fn() -> Sample.handle_execute(:query, [], [], :state) end

      assert_raise RuntimeError, "handle_close/3 not implemented",
        fn() -> Sample.handle_close(:query, [], :state) end

      assert Sample.handle_info(:msg, :state) == {:ok, :state}
    after
      :code.purge(Sample)
      :code.delete(Sample)
    end
  end

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
