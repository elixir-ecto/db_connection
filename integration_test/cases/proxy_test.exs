defmodule ProxyTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestConnection, as: C
  alias TestProxy, as: Proxy

  test "proxy checks out and checks in" do
    stack = [
      {:ok, :state},
      {:ok, :new_state, :proxy},
      {:ok, :newer_state},
      {:ok, :newest_state, :proxy2},
      {:ok, :state2}
     ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.run(pool, fn(_) -> :hi end, [proxy_mod: Proxy]) == {:ok, :hi}
    assert P.run(pool, fn(_) -> :hi end,
      [proxy_mod: Proxy, key: :value]) == {:ok, :hi}

    assert [
      connect: [_],
      checkout: [C, _, :state],
      checkin: [C, _, :new_state, :proxy],
      checkout: [C, _, :newer_state],
      checkin: [C, _, :newest_state, :proxy2]]= A.record(agent)
  end

  test "proxy checkout error raises" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:error, err, :new_state},
      {:ok, :newer_state, :proxy2},
      {:ok, :newest_state}
     ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_raise RuntimeError, "oops",
      fn() -> P.run(pool, fn(_) -> flunk("ran") end, [proxy_mod: Proxy]) end

    assert P.run(pool, fn(_) -> :hi end, [proxy_mod: Proxy]) == {:ok, :hi}

    assert [
      connect: [_],
      checkout: [C, _, :state],
      checkout: [C, _, :new_state],
      checkin: [C, _, :newer_state, :proxy2]]= A.record(agent)
  end

  test "proxy checkout disconnect raises" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:disconnect, err, :new_state},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :state2}
      end
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_raise RuntimeError, "oops",
      fn() -> P.run(pool, fn(_) -> flunk("ran") end, [proxy_mod: Proxy]) end

    assert_receive :reconnected

    assert [
      connect: [_],
      checkout: [C, _, :state],
      disconnect: [^err, :new_state],
      connect: [_]] = A.record(agent)
  end

  test "proxy checkout bad return raises DBConnection.Error and stops" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      :oops,
      {:ok, :state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}

    Process.flag(:trap_exit, true)
    assert_raise DBConnection.Error, "bad return value: :oops",
      fn() -> P.run(pool, fn(_) -> flunk("ran") end, [proxy_mod: Proxy]) end

    assert_receive {:EXIT, ^conn,
      {%DBConnection.Error{message: "client stopped: " <> _}, [_|_]}}

    assert [
      {:connect, _},
      {:checkout, [C, _, :state]} | _] = A.record(agent)
  end

  test "proxy checkout raises and stops connection" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      fn(_, _, _) ->
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
      fn() -> P.run(pool, fn(_) -> flunk("ran") end, [proxy_mod: Proxy]) end

    assert_receive {:EXIT, ^conn,
      {%DBConnection.Error{message: "client stopped: " <> _}, [_|_]}}

    assert [
      {:connect, _},
      {:checkout, [C, _, :state]} | _] = A.record(agent)
  end

  test "proxy checkin error raises" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, :new_state, :proxy},
      {:error, err, :newer_state},
      {:ok, :newest_state, :proxy2},
      {:ok, :state2}
     ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_raise RuntimeError, "oops",
      fn() -> P.run(pool, fn(_) -> :ok end, [proxy_mod: Proxy]) end

    assert P.run(pool, fn(_) -> :hi end, [proxy_mod: Proxy]) == {:ok, :hi}

    assert [
      connect: [_],
      checkout: [C, _, :state],
      checkin: [C, _, :new_state, :proxy],
      checkout: [C, _, :newer_state],
      checkin: [C, _, :newest_state, :proxy2]]= A.record(agent)
  end

  test "proxy checkin disconnect raises" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, :new_state, :proxy},
      {:disconnect, err, :newer_state},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :state2}
      end
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_raise RuntimeError, "oops",
      fn() -> P.run(pool, fn(_) -> :ok end, [proxy_mod: Proxy]) end

    assert_receive :reconnected

    assert [
      connect: [_],
      checkout: [C, _, :state],
      checkin: [C, _, :new_state, :proxy],
      disconnect: [^err, :newer_state],
      connect: [_]] = A.record(agent)
  end

  test "proxy checkin bad return raises DBConnection.Error and stops" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, :new_state, :proxy},
      :oops,
      {:ok, :state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}

    Process.flag(:trap_exit, true)
    assert_raise DBConnection.Error, "bad return value: :oops",
      fn() -> P.run(pool, fn(_) -> :ok end, [proxy_mod: Proxy]) end

    assert_receive {:EXIT, ^conn,
      {%DBConnection.Error{message: "client stopped: " <> _}, [_|_]}}

    assert [
      {:connect, _},
      {:checkout, [C, _, :state]},
      {:checkin, [C, _, :new_state, :proxy]} | _] = A.record(agent)
  end

  test "proxy checkin raises and stops connection" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, :new_state, :proxy},
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
      fn() -> P.run(pool, fn(_) -> :ok end, [proxy_mod: Proxy]) end

    assert_receive {:EXIT, ^conn,
      {%DBConnection.Error{message: "client stopped: " <> _}, [_|_]}}

    assert [
      {:connect, _},
      {:checkout, [C, _, :state]},
      {:checkin, [C, _, :new_state, :proxy]} | _] = A.record(agent)
  end
end
