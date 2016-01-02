defmodule ProxyExecuteTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestResult, as: R
  alias TestConnection, as: C
  alias TestProxy, as: Proxy


  test "proxy ignore does not proxy" do
    stack = [
      fn(opts) ->
        send(opts[:parent], :connected)
        {:ok, :state}
      end,
      :ignore,
      {:ok, %R{}, :new_state}
    ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_receive :connected

    assert P.execute(pool, %Q{}, [:param], [proxy: Proxy]) == {:ok, %R{}}

    assert [
      connect: [_],
      init: [_],
      handle_execute: [%Q{}, [:param], _, :state]]= A.record(agent)
  end

  test "proxy execute returns result" do
    stack = [
      fn(opts) ->
        send(opts[:parent], :connected)
        {:ok, :state}
      end,
      {:ok, :proxy},
      {:ok, :new_state, :new_proxy},
      {:ok, %R{}, :newer_state},
      {:ok, :newest_state, :newer_proxy},
      {:ok, :proxy2},
      {:ok, :state2, :new_proxy2},
      {:ok, %R{}, :new_state2},
      {:ok, :newer_state2, :newer_proxy2}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_receive :connected

    assert P.execute(pool, %Q{}, [:param], [proxy: Proxy]) == {:ok, %R{}}
    assert P.execute(pool, %Q{}, [:param],
      [key: :value, proxy: Proxy]) == {:ok, %R{}}

    assert [
      connect: [_],
      init: [_],
      checkout: [C, _, :state, :proxy],
      handle_execute: [%Q{}, [:param], [{:proxy, Proxy} | _], :new_state],
      checkin: [C, _, :newer_state, :new_proxy],
      init: [_],
      checkout: [C, _, :newest_state, :proxy2],
      handle_execute: [%Q{}, [:param],
        [{:key, :value}, {:proxy, Proxy} | _], :state2],
      checkin: [C, _, :new_state2, :new_proxy2]]= A.record(agent)
  end

  test "proxy execute error returns error" do
    err = RuntimeError.exception("oops")
    stack = [
      fn(opts) ->
        send(opts[:parent], :connected)
        {:ok, :state}
      end,
      {:ok, :proxy},
      {:ok, :new_state, :new_proxy},
      {:error, err, :newer_state},
      {:ok, :newest_state, :newer_proxy}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_receive :connected

    assert P.execute(pool, %Q{}, [:param], [proxy: Proxy]) == {:error, err}

    assert [
      connect: [_],
      init: [_],
      checkout: [C, _, :state, :proxy],
      handle_execute: [%Q{}, [:param], _, :new_state],
      checkin: [C, _, :newer_state, :new_proxy]] = A.record(agent)
  end

  test "proxy execute! error raises error" do
    err = RuntimeError.exception("oops")
    stack = [
      fn(opts) ->
        send(opts[:parent], :connected)
        {:ok, :state}
      end,
      {:ok, :proxy},
      {:ok, :new_state, :new_proxy},
      {:error, err, :newer_state},
      {:ok, :newest_state, :newer_proxy}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_receive :connected

    assert_raise RuntimeError, "oops",
      fn() -> P.execute!(pool, %Q{}, [:param], [proxy: Proxy]) end

    assert [
      connect: [_],
      init: [_],
      checkout: [C, _, :state, :proxy],
      handle_execute: [%Q{}, [:param], _, :new_state],
      checkin: [C, _, :newer_state, :new_proxy]] = A.record(agent)
  end

  test "proxy execute disconnect returns error" do
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
        {:ok, :state}
      end
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_receive :connected

    assert P.execute(pool, %Q{}, [:param], [proxy: Proxy]) == {:error, err}

    assert_receive :reconnected

    assert [
      connect: [opts2],
      init: [_],
      checkout: [C, _, :state, :proxy],
      handle_execute: [%Q{}, [:param], _, :new_state],
      disconnect: [^err, :newer_state],
      connect: [opts2]] = A.record(agent)
  end

  test "proxy execute bad return raises DBConnection.Error and stops" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, :proxy},
      {:ok, :new_state, :new_proxy},
      :oops,
      {:ok, :state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}

    Process.flag(:trap_exit, true)
    assert_raise DBConnection.Error, "bad return value: :oops",
      fn() -> P.execute(pool, %Q{}, [:param], [proxy: Proxy]) end

    assert_receive {:EXIT, ^conn,
      {%DBConnection.Error{message: "client stopped: " <> _}, [_|_]}}

    assert [
      {:connect, _},
      {:init, _},
      {:checkout, [C, _, :state, :proxy]},
      {:handle_execute, [%Q{}, [:param], _, :new_state]} | _] = A.record(agent)
  end

  test "proxy execute raise raises and stops connection" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, :proxy},
      {:ok, :new_state, :new_proxy},
      fn(_, _, _, _) ->
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
      fn() -> P.execute(pool, %Q{}, [:param], [proxy: Proxy]) end

    assert_receive {:EXIT, ^conn,
      {%DBConnection.Error{message: "client stopped: " <> _}, [_|_]}}

    assert [
      {:connect, _},
      {:init, _},
      {:checkout, [C, _, :state, :proxy]},
      {:handle_execute, [%Q{}, [:param], _, :new_state]}| _] = A.record(agent)
  end

 test "proxy execute prepares query and then re-executes" do
   stack = [
      fn(opts) ->
        send(opts[:parent], :connected)
        {:ok, :state}
      end,
      {:ok, :proxy},
      {:ok, :new_state, :new_proxy},
      {:prepare, :newer_state},
      {:ok, %Q{}, :newest_state},
      {:ok, %R{}, :state2},
      {:ok, :new_state2, :newer_proxy}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_receive :connected

    assert P.execute(pool, %Q{}, [:param], [proxy: Proxy]) == {:ok, %R{}}

    assert [
      connect: [_],
      init: [_],
      checkout: [C, _, :state, :proxy],
      handle_execute: [%Q{}, [:param], _, :new_state],
      handle_prepare: [%Q{}, _, :newer_state],
      handle_execute: [%Q{}, [:param], _, :newest_state],
      checkin: [C, _, :state2, :new_proxy]] = A.record(agent)
  end

 test "proxy execute errors when preparing query" do
   err = RuntimeError.exception("oops")
   stack = [
      fn(opts) ->
        send(opts[:parent], :connected)
        {:ok, :state}
      end,
      {:ok, :proxy},
      {:ok, :new_state, :new_proxy},
      {:prepare, :newer_state},
      {:error, err, :newest_state},
      {:ok, :state2, :newer_proxy}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_receive :connected

    assert P.execute(pool, %Q{}, [:param], [proxy: Proxy]) == {:error, err}

    assert [
      connect: [_],
      init: [_],
      checkout: [C, _, :state, :proxy],
      handle_execute: [%Q{}, [:param], _, :new_state],
      handle_prepare: [%Q{}, _, :newer_state],
      checkin: [C, _, :newest_state, :new_proxy]] = A.record(agent)
  end

 test "proxy execute asks to prepare on retry raises" do
   stack = [
      fn(opts) ->
        send(opts[:parent], :connected)
        {:ok, :state}
      end,
      {:ok, :proxy},
      {:ok, :new_state, :new_proxy},
      {:prepare, :newer_state},
      {:ok, %Q{}, :newest_state},
      {:prepare, :state2},
      {:ok, :new_state2, :newer_proxy},
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_receive :connected

    assert_raise DBConnection.Error, "connection did not prepare query",
      fn() -> P.execute(pool, %Q{}, [:param], [proxy: Proxy]) end

    assert [
      connect: [_],
      init: [_],
      checkout: [C, _, :state, :proxy],
      handle_execute: [%Q{}, [:param], _, :new_state],
      handle_prepare: [%Q{}, _, :newer_state],
      handle_execute: [%Q{}, [:param], _, :newest_state],
      checkin: [C, _, :state2, :new_proxy]] = A.record(agent)
  end
end
