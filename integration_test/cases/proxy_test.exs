defmodule ProxyTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestConnection, as: C
  alias TestProxy, as: Proxy

  test "proxy checks out and checks in" do
    stack = [
      fn(opts) ->
        send(opts[:parent], :connected)
        {:ok, :state}
      end,
      {:ok, :proxy},
      {:ok, :new_state, :new_proxy},
      {:ok, :newer_state, :newer_proxy},
      {:ok, :proxy2},
      {:ok, :newest_state, :new_proxy2},
      {:ok, :state2, :newer_proxy2}
     ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_receive :connected

    assert P.run(pool, fn(_) -> :hi end, [proxy: Proxy]) == :hi
    assert P.run(pool, fn(_) -> :hi end,
      [proxy: Proxy, key: :value]) == :hi

    assert [
      connect: [_],
      init: [_],
      checkout: [C, _, :state, :proxy],
      checkin: [C, _, :new_state, :new_proxy],
      init: [_],
      checkout: [C, _, :newer_state, :proxy2],
      checkin: [C, _, :newest_state, :new_proxy2]]= A.record(agent)
  end

  test "proxy init error raises" do
    err = RuntimeError.exception("oops")
    stack = [
      fn(opts) ->
        send(opts[:parent], :connected)
        {:ok, :state}
      end,
      {:error, err},
      {:ok, :proxy},
      {:ok, :new_state, :new_proxy},
      {:ok, :newer_state, :newer_proxy}
     ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_receive :connected

    assert_raise RuntimeError, "oops",
      fn() -> P.run(pool, fn(_) -> flunk("ran") end, [proxy: Proxy]) end

    assert P.run(pool, fn(_) -> :hi end, [proxy: Proxy]) == :hi

    assert [
      connect: [_],
      init: [_],
      init: [_],
      checkout: [C, _, :state, :proxy],
      checkin: [C, _, :new_state, :new_proxy]]= A.record(agent)
  end

  test "proxy checkout error raises" do
    err = RuntimeError.exception("oops")
    stack = [
      fn(opts) ->
        send(opts[:parent], :connected)
        {:ok, :state}
      end,
      {:ok, :proxy},
      {:error, err, :new_state, :new_proxy},
      {:ok, :newer_state, :newer_proxy},
      {:ok, :proxy2},
      {:ok, :newest_state, :new_proxy2},
      {:ok, :state2, :newer_proxy2}
     ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_receive :connected

    assert_raise RuntimeError, "oops",
      fn() -> P.run(pool, fn(_) -> flunk("ran") end, [proxy: Proxy]) end

    assert P.run(pool, fn(_) -> :hi end, [proxy: Proxy]) == :hi

    assert [
      connect: [_],
      init: [_],
      checkout: [C, _, :state, :proxy],
      checkin: [C, _, :new_state, :new_proxy],
      init: [_],
      checkout: [C, _, :newer_state, :proxy2],
      checkin: [C, _, :newest_state, :new_proxy2]]= A.record(agent)
  end

  test "proxy checkout disconnect raises" do
    err = RuntimeError.exception("oops")
    stack = [
      fn(opts) ->
        send(opts[:parent], :connected)
        {:ok, :state}
      end,
      {:ok, :proxy},
      {:disconnect, err, :new_state, :new_proxy},
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
      fn() -> P.run(pool, fn(_) -> flunk("ran") end, [proxy: Proxy]) end

    assert_receive :reconnected

    assert [
      connect: [_],
      init: [_],
      checkout: [C, _, :state, :proxy],
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
      {:ok, :proxy},
      :oops,
      {:ok, :state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}

    Process.flag(:trap_exit, true)
    assert_raise DBConnection.Error, "bad return value: :oops",
      fn() -> P.run(pool, fn(_) -> flunk("ran") end, [proxy: Proxy]) end

    assert_receive {:EXIT, ^conn,
      {%DBConnection.Error{message: "client stopped: " <> _}, [_|_]}}

    assert [
      {:connect, _},
      {:init, [_]},
      {:checkout, [C, _, :state, :proxy]} | _] = A.record(agent)
  end

  test "proxy checkout raises and stops connection" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, :proxy},
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
      fn() -> P.run(pool, fn(_) -> flunk("ran") end, [proxy: Proxy]) end

    assert_receive {:EXIT, ^conn,
      {%DBConnection.Error{message: "client stopped: " <> _}, [_|_]}}

    assert [
      {:connect, _},
      {:init, _},
      {:checkout, [C, _, :state, :proxy]} | _] = A.record(agent)
  end

  test "proxy checkin error raises" do
    err = RuntimeError.exception("oops")
    stack = [
      fn(opts) ->
        send(opts[:parent], :connected)
        {:ok, :state}
      end,
      {:ok, :proxy},
      {:ok, :new_state, :new_proxy},
      {:error, err, :newer_state, :newer_proxy},
      {:ok, :proxy2},
      {:ok, :newest_state, :new_proxy2},
      {:ok, :state2, :newer_proxy2}
     ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_receive :connected

    assert_raise RuntimeError, "oops",
      fn() -> P.run(pool, fn(_) -> :ok end, [proxy: Proxy]) end

    assert P.run(pool, fn(_) -> :hi end, [proxy: Proxy]) == :hi

    assert [
      connect: [_],
      init: [_],
      checkout: [C, _, :state, :proxy],
      checkin: [C, _, :new_state, :new_proxy],
      init: [_],
      checkout: [C, _, :newer_state, :proxy2],
      checkin: [C, _, :newest_state, :new_proxy2]]= A.record(agent)
  end

  test "proxy checkin disconnect raises" do
    err = RuntimeError.exception("oops")
    stack = [
      fn(opts) ->
        send(opts[:parent], :connected)
        {:ok, :state}
      end,
      {:ok, :proxy},
      {:ok, :new_state, :new_proxy},
      {:disconnect, err, :newer_state, :newer_proxy},
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
      fn() -> P.run(pool, fn(_) -> :ok end, [proxy: Proxy]) end

    assert_receive :reconnected

    assert [
      connect: [_],
      init: [_],
      checkout: [C, _, :state, :proxy],
      checkin: [C, _, :new_state, :new_proxy],
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
      fn() -> P.run(pool, fn(_) -> :ok end, [proxy: Proxy]) end

    assert_receive {:EXIT, ^conn,
      {%DBConnection.Error{message: "client stopped: " <> _}, [_|_]}}

    assert [
      {:connect, _},
      {:init, _},
      {:checkout, [C, _, :state, :proxy]},
      {:checkin, [C, _, :new_state, :new_proxy]} | _] = A.record(agent)
  end

  test "proxy checkin raises and stops connection" do
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
      fn() -> P.run(pool, fn(_) -> :ok end, [proxy: Proxy]) end

    assert_receive {:EXIT, ^conn,
      {%DBConnection.Error{message: "client stopped: " <> _}, [_|_]}}

    assert [
      {:connect, _},
      {:init, _},
      {:checkout, [C, _, :state, :proxy]},
      {:checkin, [C, _, :new_state, :new_proxy]} | _] = A.record(agent)
  end
end
