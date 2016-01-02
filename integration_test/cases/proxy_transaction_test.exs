defmodule ProxyTransactionTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestConnection, as: C
  alias TestProxy, as: Proxy

  test "proxy transaction returns result" do
    stack = [
      fn(opts) ->
        send(opts[:parent], :connected)
        {:ok, :state}
      end,
      {:ok, :proxy},
      {:ok, :new_state, :new_proxy},
      {:ok, :newer_state},
      {:ok, :newest_state},
      {:ok, :state2, :newer_proxy},
      {:ok, :proxy2},
      {:ok, :new_state2, :new_proxy2},
      {:ok, :newer_state2},
      {:ok, :newest_state2},
      {:ok, :state3, :newer_proxy2}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_receive :connected

    assert P.transaction(pool, fn(conn) ->
      assert %DBConnection{} = conn
      :result
    end, [proxy: Proxy]) == {:ok, :result}

    assert P.transaction(pool, fn(conn) ->
      assert %DBConnection{} = conn
      :result
    end, [key: :value, proxy: Proxy]) == {:ok, :result}

    assert [
      connect: [_],
      init: [_],
      checkout: [C, _, :state, :proxy],
      handle_begin: [_, :new_state],
      handle_commit: [_, :newer_state],
      checkin: [C, _, :newest_state, :new_proxy],
      init: [_],
      checkout: [C, _, :state2, :proxy2],
      handle_begin: [[{:key, :value} | _], :new_state2],
      handle_commit: [[{:key, :value} | _], :newer_state2],
      checkin: [C, _, :newest_state2, :new_proxy2]] = A.record(agent)
  end

  test "proxy transaction rollback returns error" do
    stack = [
      fn(opts) ->
        send(opts[:parent], :connected)
        {:ok, :state}
      end,
      {:ok, :proxy},
      {:ok, :new_state, :new_proxy},
      {:ok, :newer_state},
      {:ok, :newest_state},
      {:ok, :state2, :newer_proxy},
      {:ok, :proxy2},
      {:ok, :new_state2, :new_proxy2},
      {:ok, :newer_state2},
      {:ok, :newest_state2},
      {:ok, :state3, :newer_proxy2}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_receive :connected

    assert P.transaction(pool, fn(conn) ->
      P.rollback(conn, :oops)
    end, [proxy: Proxy]) == {:error, :oops}

    assert P.transaction(pool, fn(_) -> :result end,
      [proxy: Proxy]) == {:ok, :result}

    assert [
      connect: [_],
      init: [_],
      checkout: [C, _, :state, :proxy],
      handle_begin: [ _, :new_state],
      handle_rollback: [_, :newer_state],
      checkin: [C, _, :newest_state, :new_proxy],
      init: [_],
      checkout: [C, _, :state2, :proxy2],
      handle_begin: [_, :new_state2],
      handle_commit: [_, :newer_state2],
      checkin: [C, _, :newest_state2, :new_proxy2]] = A.record(agent)
  end

  test "proxy transaction and transaction returns result" do
    stack = [
      fn(opts) ->
        send(opts[:parent], :connected)
        {:ok, :state}
      end,
      {:ok, :proxy},
      {:ok, :new_state, :new_proxy},
      {:ok, :newer_state},
      {:ok, :newest_state},
      {:ok, :state2, :newer_proxy}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_receive :connected

    assert P.transaction(pool, fn(conn) ->
      assert P.transaction(conn, fn(conn2) ->
        assert %DBConnection{} = conn2
        assert conn == conn2
        :result
      end) == {:ok, :result}
      :result
    end, [proxy: Proxy]) == {:ok, :result}

    assert [
      connect: [_],
      init: [_],
      checkout: [C, _, :state, :proxy],
      handle_begin: [ _, :new_state],
      handle_commit: [_, :newer_state],
      checkin: [C, _, :newest_state, :new_proxy]] = A.record(agent)
  end

  test "proxy transaction and run returns result" do
    stack = [
      fn(opts) ->
        send(opts[:parent], :connected)
        {:ok, :state}
      end,
      {:ok, :proxy},
      {:ok, :new_state, :new_proxy},
      {:ok, :newer_state},
      {:ok, :newest_state},
      {:ok, :state2, :newer_proxy}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_receive :connected

    assert P.transaction(pool, fn(conn) ->
      assert P.run(conn, fn(conn2) ->
        assert %DBConnection{} = conn2
        assert conn == conn2
        :result
      end) == :result
      :result
    end, [proxy: Proxy]) == {:ok, :result}

    assert [
      connect: [_],
      init: [_],
      checkout: [C, _, :state, :proxy],
      handle_begin: [ _, :new_state],
      handle_commit: [_, :newer_state],
      checkin: [C, _, :newest_state, :new_proxy]] = A.record(agent)
  end

  test "proxy transaction begin error raises error" do
    err = RuntimeError.exception("oops")
    stack = [
      fn(opts) ->
        send(opts[:parent], :connected)
        {:ok, :state}
      end,
      {:ok, :proxy},
      {:ok, :new_state, :new_proxy},
      {:error, err, :newer_state},
      {:ok, :newest_state, :newer_proxy},
      {:ok, :proxy2},
      {:ok, :state2, :new_proxy2},
      {:ok, :new_state2},
      {:ok, :newer_state2},
      {:ok, :newest_state2, :newer_proxy2},
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_receive :connected

    assert_raise RuntimeError, "oops",
      fn() ->
        P.transaction(pool, fn(_) -> flunk("transaction ran") end,
          [proxy: Proxy])
      end

    assert P.transaction(pool, fn(_) -> :result end,
      [proxy: Proxy]) == {:ok, :result}

    assert [
      connect: [_],
      init: [_],
      checkout: [C, _, :state, :proxy],
      handle_begin: [ _, :new_state],
      checkin: [C, _, :newer_state, :new_proxy],
      init: [_],
      checkout: [C, _, :newest_state, :proxy2],
      handle_begin: [_, :state2],
      handle_commit: [_, :new_state2],
      checkin: [C, _, :newer_state2, :new_proxy2]] = A.record(agent)
  end

  test "proxy transaction begin disconnect raises error" do
    err = RuntimeError.exception("oops")
    stack = [
      fn(opts) ->
        send(opts[:parent], :connected)
        {:ok, :state}
      end,
      {:ok, :proxy},
      {:ok, :new_state, :new_proxy},
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

    assert_receive :connected

    assert_raise RuntimeError, "oops",
      fn() ->
        P.transaction(pool, fn(_) -> flunk("transaction ran") end,
          [proxy: Proxy])
      end

    assert_receive :reconnected

    assert [
      connect: [_],
      init: [_],
      checkout: [C, _, :state, :proxy],
      handle_begin: [_, :new_state],
      disconnect: [_, :newer_state],
      connect: [_]] = A.record(agent)
  end

  test "proxy transaction begin bad return raises and stops connection" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, :proxy},
      {:ok, :new_state, :new_proxy},
      :oops,
      {:ok, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}

    Process.flag(:trap_exit, true)
    assert_raise DBConnection.Error, "bad return value: :oops",
      fn() ->
        P.transaction(pool, fn(_) -> flunk("transaction ran") end,
          [proxy: Proxy])
      end

    assert_receive {:EXIT, ^conn,
      {%DBConnection.Error{message: "client stopped: " <> _}, [_|_]}}

    assert [
      {:connect, _},
      {:init, _},
      {:checkout, [C, _, :state, :proxy]},
      {:handle_begin, [_, :new_state]}| _] = A.record(agent)
  end

  test "proxy transaction begin raise raises and stops connection" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, :proxy},
      {:ok, :new_state, :new_proxy},
      fn(_, _) ->
        raise "oops"
      end,
      {:ok, :state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}

    Process.flag(:trap_exit, true)
    assert_raise RuntimeError, "oops",
      fn() ->
        P.transaction(pool, fn(_) -> flunk("transaction ran") end,
          [proxy: Proxy])
      end

    assert_receive {:EXIT, ^conn,
      {%DBConnection.Error{message: "client stopped: " <> _}, [_|_]}}

    assert [
      {:connect, _},
      {:init, _},
      {:checkout, [C, _, :state, :proxy]},
      {:handle_begin, [_, :new_state]} | _] = A.record(agent)
  end

  test "proxy transaction commit error raises error" do
    err = RuntimeError.exception("oops")
    stack = [
      fn(opts) ->
        send(opts[:parent], :connected)
        {:ok, :state}
      end,
      {:ok, :proxy},
      {:ok, :new_state, :new_proxy},
      {:ok, :newer_state},
      {:error, err, :newest_state},
      {:ok, :state2, :newer_proxy},
      {:ok, :proxy2},
      {:ok, :new_state2, :new_proxy2},
      {:ok, :newer_state2},
      {:ok, :newest_state2},
      {:ok, :state3, :newer_proxy2}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_receive :connected

    assert_raise RuntimeError, "oops",
      fn() -> P.transaction(pool, fn(_) -> :ok end, [proxy: Proxy]) end

    assert P.transaction(pool, fn(_) -> :result end,
      [proxy: Proxy]) == {:ok, :result}

    assert [
      connect: [_],
      init: [_],
      checkout: [C, _, :state, :proxy],
      handle_begin: [_, :new_state],
      handle_commit: [_, :newer_state],
      checkin: [C, _, :newest_state, :new_proxy],
      init: [_],
      checkout: [C, _, :state2, :proxy2],
      handle_begin: [_, :new_state2],
      handle_commit: [_, :newer_state2],
      checkin: [C, _, :newest_state2, :new_proxy2]] = A.record(agent)
  end

  test "proxy transaction commit disconnect raises error" do
    err = RuntimeError.exception("oops")
    stack = [
      fn(opts) ->
        send(opts[:parent], :connected)
        {:ok, :state}
      end,
      {:ok, :proxy},
      {:ok, :new_state, :new_proxy},
      {:ok, :newer_state},
      {:disconnect, err, :newest_state},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :state2}
      end
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_receive :connected

    assert_raise RuntimeError, "oops",
      fn() -> P.transaction(pool, fn(_) -> :result end, [proxy: Proxy]) end

    assert_receive :reconnected

    assert [
      connect: [_],
      init: [_],
      checkout: [C, _, :state, :proxy],
      handle_begin: [_, :new_state],
      handle_commit: [_, :newer_state],
      disconnect: [_, :newest_state],
      connect: [_]] = A.record(agent)
  end

  test "proxy transaction commit bad return raises and stops connection" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, :proxy},
      {:ok, :new_state, :new_proxy},
      {:ok, :newer_state},
      :oops,
      {:ok, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}

    Process.flag(:trap_exit, true)
    assert_raise DBConnection.Error, "bad return value: :oops",
      fn() -> P.transaction(pool, fn(_) -> :result end, [proxy: Proxy]) end

    assert_receive {:EXIT, ^conn,
      {%DBConnection.Error{message: "client stopped: " <> _}, [_|_]}}

    assert [
      {:connect, _},
      {:init, _},
      {:checkout, [C, _, :state, :proxy]},
      {:handle_begin, [_, :new_state]},
      {:handle_commit, [_, :newer_state]} | _] = A.record(agent)
  end

  test "proxy transaction commit raise raises and stops connection" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, :proxy},
      {:ok, :new_state, :new_proxy},
      {:ok, :newer_state},
      fn(_, _) ->
        raise "oops"
      end,
      {:ok, :state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}

    Process.flag(:trap_exit, true)
    assert_raise RuntimeError, "oops",
      fn() -> P.transaction(pool, fn(_) -> :result end, [proxy: Proxy]) end

    assert_receive {:EXIT, ^conn,
      {%DBConnection.Error{message: "client stopped: " <> _}, [_|_]}}

    assert [
      {:connect, _},
      {:init, _},
      {:checkout, [C, _, :state, :proxy]},
      {:handle_begin, [_, :new_state]},
      {:handle_commit, [_, :newer_state]} | _] = A.record(agent)
  end

  test "proxy transaction rollback error raises error" do
    err = RuntimeError.exception("oops")
    stack = [
      fn(opts) ->
        send(opts[:parent], :connected)
        {:ok, :state}
      end,
      {:ok, :proxy},
      {:ok, :new_state, :new_proxy},
      {:ok, :newer_state},
      {:error, err, :newest_state},
      {:ok, :state2, :newer_proxy},
      {:ok, :proxy2},
      {:ok, :new_state2, :new_proxy2},
      {:ok, :newer_state2},
      {:ok, :newest_state2},
      {:ok, :state3, :newer_proxy2}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_receive :connected

    assert_raise RuntimeError, "oops",
      fn() ->
        P.transaction(pool, &P.rollback(&1, :oops), [proxy: Proxy])
      end

    assert P.transaction(pool, fn(_) -> :result end,
      [proxy: Proxy]) == {:ok, :result}

    assert [
      connect: [_],
      init: [_],
      checkout: [C, _, :state, :proxy],
      handle_begin: [_, :new_state],
      handle_rollback: [_, :newer_state],
      checkin: [C, _, :newest_state, :new_proxy],
      init: [_],
      checkout: [C, _, :state2, :proxy2],
      handle_begin: [_, :new_state2],
      handle_commit: [_, :newer_state2],
      checkin: [C, _, :newest_state2, :new_proxy2]] = A.record(agent)
  end
end
