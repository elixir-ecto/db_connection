defmodule ExecuteTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestResult, as: R

  test "execute returns result" do
    stack = [
      {:ok, :state},
      {:ok, %R{}, :new_state},
      {:ok, %R{}, :new_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.execute(pool, %Q{}, [:param]) == {:ok, %R{}}
    assert P.execute(pool, %Q{}, [:param], [key: :value]) == {:ok, %R{}}

    assert [
      connect: [_],
      handle_execute: [%Q{}, [:param], _, :state],
      handle_execute: [%Q{}, [:param], [{:key, :value} | _], :new_state]
      ] = A.record(agent)
  end

  test "execute encodes params and decodes result" do
    stack = [
      {:ok, :state},
      {:ok, %R{}, :new_state},
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    opts2 = [encode: fn([:param]) -> :encoded end,
             decode: fn(%R{}) -> :decoded end]
    assert P.execute(pool, %Q{}, [:param], opts2) == {:ok, :decoded}

    assert [
      connect: [_],
      handle_execute: [_, :encoded, _, :state]] = A.record(agent)
  end

  test "execute logs result" do
    stack = [
      {:ok, :state},
      {:ok, %R{}, :new_state},
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    log = fn(entry) ->
      assert %DBConnection.LogEntry{call: :execute, query: %Q{},
                                    params: [:param],
                                    result: {:ok, %R{}}} = entry
      assert is_integer(entry.pool_time)
      assert entry.pool_time >= 0
      assert is_integer(entry.connection_time)
      assert entry.connection_time >= 0
      assert is_integer(entry.decode_time)
      assert entry.decode_time >= 0
      send(parent, :logged)
    end
    assert P.execute(pool, %Q{}, [:param], [log: log]) == {:ok, %R{}}
    assert_received :logged

    assert [
      connect: [_],
      handle_execute: [%Q{}, [:param], _, :state]] = A.record(agent)
  end

  test "execute error returns error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:error, err, :new_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.execute(pool, %Q{}, [:param]) == {:error, err}

    assert [
      connect: [_],
      handle_execute: [%Q{}, [:param], _, :state]] = A.record(agent)
  end

  test "execute logs error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:error, err, :new_state}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)
    log = fn(entry) ->
      assert %DBConnection.LogEntry{call: :execute, query: %Q{},
                                    params: [:param],
                                    result: {:error, ^err}} = entry
      assert is_integer(entry.pool_time)
      assert entry.pool_time >= 0
      assert is_integer(entry.connection_time)
      assert entry.connection_time >= 0
      assert is_nil(entry.decode_time)
      send(parent, :logged)
    end
    assert P.execute(pool, %Q{}, [:param], [log: log]) == {:error, err}
    assert_received :logged

    assert [
      connect: [_],
      handle_execute: [%Q{}, [:param], _, :state]] = A.record(agent)
  end

  test "execute! error raises error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:error, err, :new_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert_raise RuntimeError, "oops",
      fn() -> P.execute!(pool, %Q{}, [:param]) end

    assert [
      connect: [_],
      handle_execute: [%Q{}, [:param], _, :state]] = A.record(agent)
  end

  test "execute disconnect returns error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:disconnect, err, :new_state},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :state}
      end
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.execute(pool, %Q{}, [:param]) == {:error, err}

    assert_receive :reconnected

    assert [
      connect: [opts2],
      handle_execute: [%Q{}, [:param], _, :state],
      disconnect: [^err, :new_state],
      connect: [opts2]] = A.record(agent)
  end

  test "execute bad return raises DBConnection.Error and stops connection" do
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
      fn() -> P.execute(pool, %Q{}, [:param]) end

    message = "client #{inspect self()} stopped: bad return value: :oops"
    assert_receive {:EXIT, ^conn,
      {%DBConnection.Error{message: ^message}, [_|_]}}

    assert [
      {:connect, _},
      {:handle_execute, [%Q{}, [:param], _, :state]}| _] = A.record(agent)
  end

  test "execute raise raises and stops connection" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
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
      fn() -> P.execute(pool, %Q{}, [:param]) end

    prefix = "client #{inspect self()} stopped: an exception was raised"
    len = byte_size(prefix)
    assert_receive {:EXIT, ^conn,
      {%DBConnection.Error{message: <<^prefix::binary-size(len), _::binary>>},
       [_|_]}}

    assert [
      {:connect, _},
      {:handle_execute, [%Q{}, [:param], _, :state]}| _] = A.record(agent)
  end
end
