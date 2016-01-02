defmodule TransactionTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A

  test "transaction returns result" do
    stack = [
      {:ok, :state},
      {:ok, :new_state},
      {:ok, :newer_state},
      {:ok, :newest_state},
      {:ok, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn(conn) ->
      assert %DBConnection{} = conn
      :result
    end) == {:ok, :result}

    assert P.transaction(pool, fn(conn) ->
      assert %DBConnection{} = conn
      :result
    end, [key: :value]) == {:ok, :result}

    assert [
      connect: [_],
      handle_begin: [ _, :state],
      handle_commit: [_, :new_state],
      handle_begin: [[{:key, :value} | _], :newer_state],
      handle_commit: [[{:key, :value} | _], :newest_state]] = A.record(agent)
  end

  test "transaction rollback returns error" do
    stack = [
      {:ok, :state},
      {:ok, :new_state},
      {:ok, :newer_state},
      {:ok, :newest_state},
      {:ok, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn(conn) ->
      P.rollback(conn, :oops)
    end) == {:error, :oops}

    assert P.transaction(pool, fn(_) -> :result end) == {:ok, :result}

    assert [
      connect: [_],
      handle_begin: [ _, :state],
      handle_rollback: [_, :new_state],
      handle_begin: [_, :newer_state],
      handle_commit: [_, :newest_state]] = A.record(agent)
  end

  test "inner transaction rollback returns error on outer transaction" do
    stack = [
      {:ok, :state},
      {:ok, :new_state},
      {:ok, :newer_state},
      {:ok, :newest_state},
      {:ok, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn(conn) ->
      assert P.transaction(conn, fn(conn2) ->
        P.rollback(conn2, :oops)
      end) == {:error, :oops}

      assert_raise DBConnection.Error, "transaction rolling back",
        fn() -> P.transaction(conn, fn(_) -> nil end) end
    end) == {:error, :rollback}

    assert P.transaction(pool, fn(_) -> :result end) == {:ok, :result}

    assert [
      connect: [_],
      handle_begin: [ _, :state],
      handle_rollback: [_, :new_state],
      handle_begin: [_, :newer_state],
      handle_commit: [_, :newest_state]] = A.record(agent)
  end

  test "outer transaction rolls back after inner rollback" do
    stack = [
      {:ok, :state},
      {:ok, :new_state},
      {:ok, :newer_state},
      {:ok, :newest_state},
      {:ok, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn(conn) ->
      assert P.transaction(conn, fn(conn2) ->
        P.rollback(conn2, :oops)
      end) == {:error, :oops}

      P.rollback(conn, :oops2)
    end) == {:error, :oops2}

    assert P.transaction(pool, fn(_) -> :result end) == {:ok, :result}

    assert [
      connect: [_],
      handle_begin: [ _, :state],
      handle_rollback: [_, :new_state],
      handle_begin: [_, :newer_state],
      handle_commit: [_, :newest_state]] = A.record(agent)
  end

  test "inner transaction raise returns error on outer transaction" do
    stack = [
      {:ok, :state},
      {:ok, :new_state},
      {:ok, :newer_state},
      {:ok, :newest_state},
      {:ok, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn(conn) ->
      assert_raise RuntimeError, "oops",
       fn() -> P.transaction(conn, fn(_) -> raise "oops" end) end

      assert_raise DBConnection.Error, "transaction rolling back",
        fn() -> P.transaction(conn, fn(_) -> nil end) end
    end) == {:error, :rollback}

    assert P.transaction(pool, fn(_) -> :result end) == {:ok, :result}

    assert [
      connect: [_],
      handle_begin: [ _, :state],
      handle_rollback: [_, :new_state],
      handle_begin: [_, :newer_state],
      handle_commit: [_, :newest_state]] = A.record(agent)
  end

  test "transaction and transaction returns result" do
    stack = [
      {:ok, :state},
      {:ok, :new_state},
      {:ok, :newer_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn(conn) ->
      assert P.transaction(conn, fn(conn2) ->
        assert %DBConnection{} = conn2
        assert conn == conn2
        :result
      end) == {:ok, :result}
      :result
    end) == {:ok, :result}

    assert [
      connect: [_],
      handle_begin: [ _, :state],
      handle_commit: [_, :new_state]] = A.record(agent)
  end

  test "transaction and run returns result" do
    stack = [
      {:ok, :state},
      {:ok, :new_state},
      {:ok, :newer_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn(conn) ->
      assert P.run(conn, fn(conn2) ->
        assert %DBConnection{} = conn2
        assert conn == conn2
        :result
      end) == :result
      :result
    end) == {:ok, :result}

    assert [
      connect: [_],
      handle_begin: [ _, :state],
      handle_commit: [_, :new_state]] = A.record(agent)
  end

  test "transaction begin error raises error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:error, err, :new_state},
      {:ok, :newer_state},
      {:ok, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_raise RuntimeError, "oops",
      fn() -> P.transaction(pool, fn(_) -> flunk("transaction ran") end) end

    assert P.transaction(pool, fn(_) -> :result end) == {:ok, :result}

    assert [
      connect: [_],
      handle_begin: [ _, :state],
      handle_begin: [_, :new_state],
      handle_commit: [_, :newer_state]] = A.record(agent)
  end

  test "transaction begin disconnect raises error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:disconnect, err, :new_state},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :newest_state}
      end
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_raise RuntimeError, "oops",
      fn() -> P.transaction(pool, fn(_) -> flunk("transaction ran") end) end

    assert_receive :reconnected

    assert [
      connect: [_],
      handle_begin: [_, :state],
      disconnect: [_, :new_state],
      connect: [_]] = A.record(agent)
  end

  test "transaction begin bad return raises and stops connection" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      :oops,
      fn(_) ->
        {:ok, :state}
      end
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}

    Process.flag(:trap_exit, true)
    assert_raise DBConnection.Error, "bad return value: :oops",
      fn() -> P.transaction(pool, fn(_) -> flunk("transaction ran") end) end

    assert_receive {:EXIT, ^conn,
      {%DBConnection.Error{message: "client stopped: " <> _}, [_|_]}}

    assert [
      {:connect, _},
      {:handle_begin, [_, :state]}| _] = A.record(agent)
  end

  test "transaction begin raise raises and stops connection" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      fn(_, _) ->
        raise "oops"
      end,
      fn(_) ->
        {:ok, :state}
      end
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}

    Process.flag(:trap_exit, true)
    assert_raise RuntimeError, "oops",
      fn() -> P.transaction(pool, fn(_) -> flunk("transaction ran") end) end

    assert_receive {:EXIT, ^conn,
      {%DBConnection.Error{message: "client stopped: " <> _}, [_|_]}}

    assert [
      {:connect, _},
      {:handle_begin, [_, :state]} | _] = A.record(agent)
  end

  test "transaction commit error raises error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, :new_state},
      {:error, err, :newer_state},
      {:ok, :newest_state},
      {:ok, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_raise RuntimeError, "oops",
      fn() -> P.transaction(pool, fn(_) -> :ok end) end

    assert P.transaction(pool, fn(_) -> :result end) == {:ok, :result}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_commit: [_, :new_state],
      handle_begin: [_, :newer_state],
      handle_commit: [_, :newest_state]] = A.record(agent)
  end

  test "transaction commit disconnect raises error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, :new_state},
      {:disconnect, err, :newer_state},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :newest_state}
      end
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_raise RuntimeError, "oops",
      fn() -> P.transaction(pool, fn(_) -> :result end) end

    assert_receive :reconnected

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_commit: [_, :new_state],
      disconnect: [_, :newer_state],
      connect: [_]] = A.record(agent)
  end

  test "transaction commit bad return raises and stops connection" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, :new_state},
      :oops,
      fn(_) ->
        {:ok, :state}
      end
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}

    Process.flag(:trap_exit, true)
    assert_raise DBConnection.Error, "bad return value: :oops",
      fn() -> P.transaction(pool, fn(_) -> :result end) end

    assert_receive {:EXIT, ^conn,
      {%DBConnection.Error{message: "client stopped: " <> _}, [_|_]}}

    assert [
      {:connect, _},
      {:handle_begin, [_, :state]},
      {:handle_commit, [_, :new_state]} | _] = A.record(agent)
  end

  test "transaction commit raise raises and stops connection" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, :new_state},
      fn(_, _) ->
        raise "oops"
      end,
      fn(_) ->
        {:ok, :state}
      end
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}

    Process.flag(:trap_exit, true)
    assert_raise RuntimeError, "oops",
      fn() -> P.transaction(pool, fn(_) -> :result end) end

    assert_receive {:EXIT, ^conn,
      {%DBConnection.Error{message: "client stopped: " <> _}, [_|_]}}

    assert [
      {:connect, _},
      {:handle_begin, [_, :state]},
      {:handle_commit, [_, :new_state]} | _] = A.record(agent)
  end

  test "transaction rollback error raises error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, :new_state},
      {:error, err, :newer_state},
      {:ok, :newest_state},
      {:ok, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_raise RuntimeError, "oops",
      fn() -> P.transaction(pool, &P.rollback(&1, :oops)) end

    assert P.transaction(pool, fn(_) -> :result end) == {:ok, :result}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_rollback: [_, :new_state],
      handle_begin: [_, :newer_state],
      handle_commit: [_, :newest_state]] = A.record(agent)
  end
end
