defmodule StreamTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestCursor, as: C
  alias TestResult, as: R

  test "stream returns result" do
    stack = [
      {:ok, :state},
      {:ok, %C{}, :new_state},
      {:cont, %R{}, :newer_state},
      {:done, %R{}, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    stream = P.stream(pool, %Q{}, [:param])
    assert %DBConnection.Stream{} = stream
    assert Enum.to_list(stream) == [%R{}, %R{}]

    assert [
      connect: [_],
      handle_open: [%Q{}, [:param], _, :state],
      handle_fetch: [%Q{}, %C{}, _, :new_state],
      handle_fetch: [%Q{}, %C{}, _, :newer_state]
      ] = A.record(agent)
  end

  test "halted stream closes and returns result" do
    stack = [
      {:ok, :state},
      {:ok, %C{}, :new_state},
      {:cont, %R{}, :newer_state},
      {:ok, :result, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    stream = P.stream(pool, %Q{}, [:param])
    assert Enum.take(stream, 1) == [%R{}]

    assert [
      connect: [_],
      handle_open: [%Q{}, [:param], _, :state],
      handle_fetch: [%Q{}, %C{}, _, :new_state],
      handle_close: [%Q{}, %C{}, _, :newer_state]
      ] = A.record(agent)
  end

  test "stream encodes params and decodes result" do
    stack = [
      {:ok, :state},
      {:ok, %C{}, :new_state},
      {:done, %R{}, :newer_state},
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    opts2 = [encode: fn([:param]) -> :encoded end,
             decode: fn(%R{}) -> :decoded end]
    stream = P.stream(pool, %Q{}, [:param], opts2)
    assert Enum.to_list(stream) == [:decoded]

    assert [
      connect: [_],
      handle_open: [_, :encoded, _, :state],
      handle_fetch: [%Q{}, %C{}, _, :new_state]
      ] = A.record(agent)
  end

  test "stream logs result" do
    stack = [
      {:ok, :state},
      {:ok, %C{}, :new_state},
      {:cont, %R{}, :newer_state},
      {:ok, :result, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    stream = P.stream(pool, %Q{}, [:param], [log: &send(parent, &1)])
    assert Enum.take(stream, 1) == [%R{}]

    assert_received %DBConnection.LogEntry{call: :open} = entry
    assert %{query: %Q{}, params: [:param], result: {:ok, %C{}}} = entry
    assert is_integer(entry.pool_time)
    assert entry.pool_time >= 0
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert_received %DBConnection.LogEntry{call: :fetch} = entry
    assert %{query: %Q{}, params: %C{}, result: {:ok, %R{}}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_integer(entry.decode_time)
    assert entry.decode_time >= 0

    assert_received %DBConnection.LogEntry{call: :close} = entry
    assert %{query: %Q{}, params: %C{}, result: {:ok, :result}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert [
      connect: [_],
      handle_open: [%Q{}, [:param], _, :state],
      handle_fetch: [%Q{}, %C{}, _, :new_state],
      handle_close: [%Q{}, %C{}, _, :newer_state]
      ] = A.record(agent)
  end

  test "stream logs open error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:error, err, :new_state}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    stream = P.stream(pool, %Q{}, [:param], [log: &send(parent, &1)])
    assert_raise RuntimeError, "oops", fn() -> Enum.to_list(stream) end

    assert_received %DBConnection.LogEntry{call: :open} = entry
    assert %{query: %Q{}, params: [:param], result: {:error, ^err}} = entry
    assert is_integer(entry.pool_time)
    assert entry.pool_time >= 0
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert [
      connect: [_],
      handle_open: [%Q{}, [:param], _, :state]
      ] = A.record(agent)
  end

  test "stream logs fetch disconnect" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, %C{}, :new_state},
      {:disconnect, err, :newer_state},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :state2}
      end
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    stream = P.stream(pool, %Q{}, [:param], [log: &send(parent, &1)])
    assert_raise RuntimeError, "oops", fn() -> Enum.to_list(stream) end

    assert_received %DBConnection.LogEntry{call: :open}

    assert_received %DBConnection.LogEntry{call: :fetch} = entry
    assert %{query: %Q{}, params: %C{}, result: {:error, ^err}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    refute_received %DBConnection.LogEntry{call: :close}

    assert_receive :reconnected

    assert [
      connect: [_],
      handle_open: [%Q{}, [:param], _, :state],
      handle_fetch: [%Q{}, %C{}, _, :new_state],
      disconnect: [^err, :newer_state],
      connect: [_]
      ] = A.record(agent)
  end

  test "stream logs close disconnect" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, %C{}, :new_state},
      {:cont, %R{}, :newer_state},
      {:disconnect, err, :newest_state},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :state2}
      end
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    stream = P.stream(pool, %Q{}, [:param], [log: &send(parent, &1)])
    assert_raise RuntimeError, "oops", fn() -> Enum.take(stream, 1) end

    assert_received %DBConnection.LogEntry{call: :open}
    assert_received %DBConnection.LogEntry{call: :fetch}

    assert_received %DBConnection.LogEntry{call: :close} = entry
    assert %{query: %Q{}, params: %C{}, result: {:error, ^err}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert_receive :reconnected

    assert [
      connect: [_],
      handle_open: [%Q{}, [:param], _, :state],
      handle_fetch: [%Q{}, %C{}, _, :new_state],
      handle_close: [%Q{}, %C{}, _, :newer_state],
      disconnect: [^err, :newest_state],
      connect: [_]
      ] = A.record(agent)
  end

  test "stream open bad return raises and stops" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      :oops,
      {:ok, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}

    Process.flag(:trap_exit, true)
    stream = P.stream(pool, %Q{}, [:param], [])
    assert_raise DBConnection.ConnectionError, "bad return value: :oops",
      fn() -> Enum.to_list(stream) end

    prefix = "client #{inspect self()} stopped: " <>
      "** (DBConnection.ConnectionError) bad return value: :oops"
    len = byte_size(prefix)
    assert_receive {:EXIT, ^conn,
      {%DBConnection.ConnectionError{message: <<^prefix::binary-size(len), _::binary>>},
        [_|_]}}

    assert [
      {:connect, _},
      {:handle_open, [%Q{}, [:param], _, :state]} | _] = A.record(agent)
  end

  test "stream open raise raises and stops connection" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      fn(_, _, _, _) ->
        raise "oops"
      end,
      {:ok, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}

    Process.flag(:trap_exit, true)
    stream = P.stream(pool, %Q{}, [:param], [])
    assert_raise RuntimeError, "oops", fn() -> Enum.to_list(stream) end

    prefix = "client #{inspect self()} stopped: ** (RuntimeError) oops"
    len = byte_size(prefix)
    assert_receive {:EXIT, ^conn,
      {%DBConnection.ConnectionError{message: <<^prefix::binary-size(len), _::binary>>},
        [_|_]}}

    assert [
      {:connect, _},
      {:handle_open, [%Q{}, [:param], _, :state]} | _] = A.record(agent)
  end

  test "stream fetch bad return raises and stops" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, %C{}, :new_state},
      :oops,
      {:ok, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}

    Process.flag(:trap_exit, true)
    stream = P.stream(pool, %Q{}, [:param], [])
    assert_raise DBConnection.ConnectionError, "bad return value: :oops",
      fn() -> Enum.to_list(stream) end

    prefix = "client #{inspect self()} stopped: " <>
      "** (DBConnection.ConnectionError) bad return value: :oops"
    len = byte_size(prefix)
    assert_receive {:EXIT, ^conn,
      {%DBConnection.ConnectionError{message: <<^prefix::binary-size(len), _::binary>>},
        [_|_]}}

    assert [
      {:connect, _},
      {:handle_open, [%Q{}, [:param], _, :state]},
      {:handle_fetch, [%Q{}, %C{}, _, :new_state]} | _] = A.record(agent)
  end

  test "stream close raise raises and stops connection" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, %C{}, :new_state},
      {:cont, %R{}, :newer_state},
      fn(_, _, _, _) ->
        raise "oops"
      end,
      {:ok, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}

    Process.flag(:trap_exit, true)
    stream = P.stream(pool, %Q{}, [:param], [])
    assert_raise RuntimeError, "oops", fn() -> Enum.take(stream, 1) end

    prefix = "client #{inspect self()} stopped: ** (RuntimeError) oops"
    len = byte_size(prefix)
    assert_receive {:EXIT, ^conn,
      {%DBConnection.ConnectionError{message: <<^prefix::binary-size(len), _::binary>>},
        [_|_]}}

    assert [
      {:connect, _},
      {:handle_open, [%Q{}, [:param], _, :state]},
      {:handle_fetch, [%Q{}, %C{}, _, :new_state]},
      {:handle_close, [%Q{}, %C{}, _, :newer_state]} | _] = A.record(agent)
  end
end
