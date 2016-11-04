defmodule PrepareStreamTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestCursor, as: C
  alias TestResult, as: R

  test "prepare_stream returns result" do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state},
      {:ok, %C{}, :newer_state},
      {:cont, %R{}, :newest_state},
      {:done, %R{}, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    stream = P.prepare_stream(pool, %Q{}, [:param])
    assert %DBConnection.PrepareStream{} = stream
    assert Enum.to_list(stream) == [%R{}, %R{}]

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state],
      handle_open: [%Q{}, [:param], _, :new_state],
      handle_fetch: [%Q{}, %C{}, _, :newer_state],
      handle_fetch: [%Q{}, %C{}, _, :newest_state]
      ] = A.record(agent)
  end

  test "halted prepare_stream closes and returns result" do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state},
      {:ok, %C{}, :newer_state},
      {:cont, %R{}, :newest_state},
      {:ok, :result, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    stream = P.prepare_stream(pool, %Q{}, [:param])
    assert Enum.take(stream, 1) == [%R{}]

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state],
      handle_open: [%Q{}, [:param], _, :new_state],
      handle_fetch: [%Q{}, %C{}, _, :newer_state],
      handle_close: [%Q{}, %C{}, _, :newest_state]
      ] = A.record(agent)
  end

  test "prepare_stream parses/describes/encodes/decodes" do
    stack = [
      {:ok, :state},
      {:ok, %Q{state: :prepared}, :new_state},
      {:ok, %C{}, :newer_state},
      {:done, %R{}, :newest_state},
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    opts2 = [parse: fn(%Q{state: nil}) -> %Q{state: :parsed} end,
             describe: fn(%Q{state: :prepared}) -> %Q{state: :described} end,
             encode: fn([:param]) -> :encoded end,
             decode: fn(%R{}) -> :decoded end]
    stream = P.prepare_stream(pool, %Q{}, [:param], opts2)
    assert Enum.to_list(stream) == [:decoded]

    assert [
      connect: [_],
      handle_prepare: [%Q{state: :parsed}, _, :state],
      handle_open: [%Q{state: :described}, :encoded, _, :new_state],
      handle_fetch: [%Q{state: :described}, %C{}, _, :newer_state]
      ] = A.record(agent)
  end

  test "prepare_stream logs result" do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state},
      {:ok, %C{}, :newer_state},
      {:cont, %R{}, :newest_state},
      {:ok, :result, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    stream = P.prepare_stream(pool, %Q{}, [:param], [log: &send(parent, &1)])
    assert Enum.take(stream, 1) == [%R{}]

    assert_received %DBConnection.LogEntry{call: :prepare_open} = entry
    assert %{query: %Q{}, params: [:param], result: {:ok, %Q{}, %C{}}} = entry
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
      handle_prepare: [%Q{}, _, :state],
      handle_open: [%Q{}, [:param], _, :new_state],
      handle_fetch: [%Q{}, %C{}, _, :newer_state],
      handle_close: [%Q{}, %C{}, _, :newest_state]
      ] = A.record(agent)
  end

  test "prepare_stream logs prepare error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:error, err, :new_state}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    stream = P.prepare_stream(pool, %Q{}, [:param], [log: &send(parent, &1)])
    assert_raise RuntimeError, "oops", fn() -> Enum.to_list(stream) end

    assert_received %DBConnection.LogEntry{call: :prepare_open} = entry
    assert %{query: %Q{}, params: [:param], result: {:error, ^err}} = entry
    assert is_integer(entry.pool_time)
    assert entry.pool_time >= 0
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state],
      ] = A.record(agent)
  end

  test "prepare_stream logs open error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state},
      {:error, err, :newer_state}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    stream = P.prepare_stream(pool, %Q{}, [:param], [log: &send(parent, &1)])
    assert_raise RuntimeError, "oops", fn() -> Enum.to_list(stream) end

    assert_received %DBConnection.LogEntry{call: :prepare_open} = entry
    assert %{query: %Q{}, params: [:param], result: {:error, ^err}} = entry
    assert is_integer(entry.pool_time)
    assert entry.pool_time >= 0
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state],
      handle_open: [%Q{}, [:param], _, :new_state]
      ] = A.record(agent)
  end

  test "prepare_stream logs fetch disconnect" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state},
      {:ok, %C{}, :newer_state},
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

    stream = P.prepare_stream(pool, %Q{}, [:param], [log: &send(parent, &1)])
    assert_raise RuntimeError, "oops", fn() -> Enum.to_list(stream) end

    assert_received %DBConnection.LogEntry{call: :prepare_open}

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
      handle_prepare: [%Q{}, _, :state],
      handle_open: [%Q{}, [:param], _, :new_state],
      handle_fetch: [%Q{}, %C{}, _, :newer_state],
      disconnect: [^err, :newest_state],
      connect: [_]
      ] = A.record(agent)
  end

  test "prepare_stream logs close disconnect" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state},
      {:ok, %C{}, :newer_state},
      {:cont, %R{}, :newest_state},
      {:disconnect, err, :state2},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :state3}
      end
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    stream = P.prepare_stream(pool, %Q{}, [:param], [log: &send(parent, &1)])
    assert_raise RuntimeError, "oops", fn() -> Enum.take(stream, 1) end

    assert_received %DBConnection.LogEntry{call: :prepare_open}
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
      handle_prepare: [%Q{}, _, :state],
      handle_open: [%Q{}, [:param], _, :new_state],
      handle_fetch: [%Q{}, %C{}, _, :newer_state],
      handle_close: [%Q{}, %C{}, _, :newest_state],
      disconnect: [^err, :state2],
      connect: [_]
      ] = A.record(agent)
  end

  test "prepare_stream open bad return raises and stops" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, %Q{}, :new_state},
      :oops,
      {:ok, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}

    Process.flag(:trap_exit, true)
    stream = P.prepare_stream(pool, %Q{}, [:param], [])
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
      {:handle_prepare, [%Q{}, _, :state]},
      {:handle_open, [%Q{}, [:param], _, :new_state]} | _] = A.record(agent)
  end

  test "prepare_stream open raise raises and stops connection" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, %Q{}, :new_state},
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
    stream = P.prepare_stream(pool, %Q{}, [:param], [])
    assert_raise RuntimeError, "oops", fn() -> Enum.to_list(stream) end

    prefix = "client #{inspect self()} stopped: ** (RuntimeError) oops"
    len = byte_size(prefix)
    assert_receive {:EXIT, ^conn,
      {%DBConnection.ConnectionError{message: <<^prefix::binary-size(len), _::binary>>},
        [_|_]}}

    assert [
      {:connect, _},
      {:handle_prepare, [%Q{}, _, :state]},
      {:handle_open, [%Q{}, [:param], _, :new_state]} | _] = A.record(agent)
  end

  test "prepare_stream fetch bad return raises and stops" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, %Q{}, :new_state},
      {:ok, %C{}, :newer_state},
      :oops,
      {:ok, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}

    Process.flag(:trap_exit, true)
    stream = P.prepare_stream(pool, %Q{}, [:param], [])
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
      {:handle_prepare, [%Q{}, _, :state]},
      {:handle_open, [%Q{}, [:param], _, :new_state]},
      {:handle_fetch, [%Q{}, %C{}, _, :newer_state]} | _] = A.record(agent)
  end

  test "prepare_stream close raise raises and stops connection" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, %Q{}, :new_state},
      {:ok, %C{}, :newer_state},
      {:cont, %R{}, :newest_state},
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
    stream = P.prepare_stream(pool, %Q{}, [:param], [])
    assert_raise RuntimeError, "oops", fn() -> Enum.take(stream, 1) end

    prefix = "client #{inspect self()} stopped: ** (RuntimeError) oops"
    len = byte_size(prefix)
    assert_receive {:EXIT, ^conn,
      {%DBConnection.ConnectionError{message: <<^prefix::binary-size(len), _::binary>>},
        [_|_]}}

    assert [
      {:connect, _},
      {:handle_prepare, [%Q{}, _, :state]},
      {:handle_open, [%Q{}, [:param], _, :new_state]},
      {:handle_fetch, [%Q{}, %C{}, _, :newer_state]},
      {:handle_close, [%Q{}, %C{}, _, :newest_state]} | _] = A.record(agent)
  end

  test "prepare_stream describe or encode raises and closes query" do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state},
      {:ok, :result, :newer_state},
      {:ok, %Q{}, :newest_state},
      {:ok, :result, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    describe = fn(%Q{}) -> raise "oops" end
    stream = P.prepare_stream(pool, %Q{}, [:param], [describe: describe])
    assert_raise RuntimeError,  "oops", fn() -> Enum.to_list(stream) end

    encode = fn([:param]) -> raise "oops" end
    stream = P.prepare_stream(pool, %Q{}, [:param], [encode: encode])
    assert_raise RuntimeError, "oops", fn() -> Enum.to_list(stream) end

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state],
      handle_close: [%Q{}, _, :new_state],
      handle_prepare: [%Q{}, _, :newer_state],
      handle_close: [%Q{}, _, :newest_state]] = A.record(agent)
  end
end
