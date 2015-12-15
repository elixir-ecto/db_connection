defmodule SandboxTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A

  test "start, restart and stop sandbox" do
    stack = [
      {:ok, :state},
      {:ok, :new_state},
      {:ok, :newer_state},
      {:ok, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.sandbox_link(opts)

    assert P.start_sandbox(pool) == :ok
    assert P.restart_sandbox(pool) == :ok
    assert P.stop_sandbox(pool) == :ok

     assert [
      connect: _,
      sandbox_start: [_, :state],
      sandbox_restart: [_, :new_state],
      sandbox_stop: [_, :newer_state]] = A.record(agent)
  end

  test "start raises if already started" do
    stack = [
      {:ok, :state},
      {:ok, :new_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.sandbox_link(opts)

    assert P.start_sandbox(pool) == :ok

    assert_raise RuntimeError, "sandbox already started",
      fn() -> P.start_sandbox(pool) end

     assert [
      connect: _,
      sandbox_start: [_, :state]] = A.record(agent)
  end

  test "restart raises if not started" do
    stack = [
      {:ok, :state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.sandbox_link(opts)

    assert_raise RuntimeError, "sandbox not started",
      fn() -> P.restart_sandbox(pool) end

     assert [connect: _] = A.record(agent)
  end

  test "stop does not raise if not started" do
    stack = [
      {:ok, :state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.sandbox_link(opts)

    assert P.stop_sandbox(pool) == :ok

     assert [connect: _] = A.record(agent)
  end

  test "sandboxed transaction commits" do
    stack = [
      {:ok, :state},
      {:ok, :new_state},
      {:ok, :newer_state},
      {:ok, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.sandbox_link(opts)

    assert P.start_sandbox(pool) == :ok
    assert P.transaction(pool, fn(_) -> :hi end) == {:ok, :hi}

    assert [
      connect: _,
      sandbox_start: [_, :state],
      sandbox_begin: [_, :new_state],
      sandbox_commit: [_, :newer_state]] = A.record(agent)
  end

  test "sandboxed transaction rollsback" do
    stack = [
      {:ok, :state},
      {:ok, :new_state},
      {:ok, :newer_state},
      {:ok, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.sandbox_link(opts)

    assert P.start_sandbox(pool) == :ok
    assert P.transaction(pool,
        fn(conn) -> P.rollback(conn, :hi) end) == {:error, :hi}

    assert [
      connect: _,
      sandbox_start: [_, :state],
      sandbox_begin: [_, :new_state],
      sandbox_rollback: [_, :newer_state]] = A.record(agent)
  end

  test "transaction after sandbox" do
    stack = [
      {:ok, :state},
      {:ok, :new_state},
      {:ok, :newer_state},
      {:ok, :newest_state},
      {:ok, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.sandbox_link(opts)

    assert P.start_sandbox(pool) == :ok
    assert P.stop_sandbox(pool) == :ok
    assert P.transaction(pool, fn(_) -> :hi end) == {:ok, :hi}

    assert [
      connect: _,
      sandbox_start: [_, :state],
      sandbox_stop: [_, :new_state],
      handle_begin: [_, :newer_state],
      handle_commit: [_, :newest_state]] = A.record(agent)
  end

  test "start raises inside transaction" do
    stack = [
      {:ok, :state},
      {:ok, :new_state},
      {:ok, :newer_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.sandbox_link(opts)

    assert P.transaction(pool, fn(conn) ->
      assert_raise RuntimeError, "can not start sandbox inside transaction",
        fn() -> P.start_sandbox(conn) end
      :hi
    end) == {:ok, :hi}

     assert [
      connect: _,
      handle_begin: [_, :state],
      handle_commit: [_, :new_state]] = A.record(agent)
  end

  test "stop and restart raise inside transaction" do
    stack = [
      {:ok, :state},
      {:ok, :new_state},
      {:ok, :newer_state},
      {:ok, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.sandbox_link(opts)

    assert P.start_sandbox(pool)
    assert P.transaction(pool, fn(conn) ->
      assert_raise RuntimeError, "can not restart sandbox inside transaction",
        fn() -> P.restart_sandbox(conn) end

      assert_raise RuntimeError, "can not stop sandbox inside transaction",
        fn() -> P.stop_sandbox(conn) end

      :hi
    end) == {:ok, :hi}

     assert [
      connect: _,
      sandbox_start: [_, :state],
      sandbox_begin: [_, :new_state],
      sandbox_commit: [_, :newer_state]] = A.record(agent)
  end

  test "sandbox transaction begin error raises but still sandbox" do
    stack = [
      {:ok, :state},
      {:ok, :new_state},
      {:error, RuntimeError.exception("oops"), :newer_state},
      {:ok, :newest_state},
      {:ok, :newest_state2}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.sandbox_link(opts)

    assert P.start_sandbox(pool) == :ok

    assert_raise RuntimeError, "oops",
      fn() -> P.transaction(pool, fn(_) -> flunk("transaction ran") end) end

    assert P.transaction(pool, fn(_) -> :hi end) == {:ok, :hi}


    assert [
      connect: _,
      sandbox_start: [_, :state],
      sandbox_begin: [_, :new_state],
      sandbox_begin: [_, :newer_state],
      sandbox_commit: [_, :newest_state]] = A.record(agent)
  end

  test "sandbox transaction commit error raises but still sandbox" do
    stack = [
      {:ok, :state},
      {:ok, :new_state},
      {:ok, :newer_state},
      {:error, RuntimeError.exception("oops"), :newest_state},
      {:ok, :newest_state2},
      {:ok, :newest_state3}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.sandbox_link(opts)

    assert P.start_sandbox(pool) == :ok

    assert_raise RuntimeError, "oops",
      fn() -> P.transaction(pool, fn(_) -> end) end

    assert P.transaction(pool, fn(_) -> :hi end) == {:ok, :hi}

    assert [
      connect: _,
      sandbox_start: [_, :state],
      sandbox_begin: [_, :new_state],
      sandbox_commit: [_, :newer_state],
      sandbox_begin: [_, :newest_state],
      sandbox_commit: [_, :newest_state2]] = A.record(agent)
  end

  test "sandbox transaction rollback error raises but still sandbox" do
    stack = [
      {:ok, :state},
      {:ok, :new_state},
      {:ok, :newer_state},
      {:error, RuntimeError.exception("oops"), :newest_state},
      {:ok, :newest_state2},
      {:ok, :newest_state3}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.sandbox_link(opts)

    assert P.start_sandbox(pool) == :ok

    assert_raise RuntimeError, "oops",
      fn() -> P.transaction(pool, fn(conn) -> P.rollback(conn, :oops) end) end

    assert P.transaction(pool, fn(_) -> :hi end) == {:ok, :hi}

    assert [
      connect: _,
      sandbox_start: [_, :state],
      sandbox_begin: [_, :new_state],
      sandbox_rollback: [_, :newer_state],
      sandbox_begin: [_, :newest_state],
      sandbox_commit: [_, :newest_state2]] = A.record(agent)
  end

  test "sandbox transaction begin disconnect stops connection" do
    err = RuntimeError.exception("oops")
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, :new_state},
      {:disconnect, err, :newer_state},
      :ok,
      {:ok, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    Process.flag(:trap_exit, true)
    {:ok, pool} = P.sandbox_link(opts)

    assert P.start_sandbox(pool) == :ok

    assert_raise RuntimeError, "oops",
      fn() -> P.transaction(pool, fn(_) -> flunk("transaction ran") end) end

    assert_received {:hi, conn}

    assert_receive {:EXIT, ^conn, {^err, [_|_]}}

    assert [
      {:connect, _},
      {:sandbox_start, [_, :state]},
      {:sandbox_begin, [_, :new_state]},
      {:disconnect, [^err, :newer_state]} | _] = A.record(agent)
  end
end
