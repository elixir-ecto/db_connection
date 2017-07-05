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
      {:ok, %R{}, :newest_state},
      {:deallocate, %R{}, :state2},
      {:ok, :deallocated, :new_state2}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.run(pool, fn(conn) ->
      stream = P.prepare_stream(conn, %Q{}, [:param])
      assert %DBConnection.PrepareStream{} = stream
      assert Enum.to_list(stream) == [%R{}, %R{}]
      :hi
    end) == :hi

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state],
      handle_declare: [%Q{}, [:param], _, :new_state],
      handle_first: [%Q{}, %C{}, _, :newer_state],
      handle_next: [%Q{}, %C{}, _, :newest_state],
      handle_deallocate: [%Q{}, %C{}, _, :state2],
      ] = A.record(agent)
  end

  test "prepare_stream parses/describes/encodes/decodes" do
    stack = [
      {:ok, :state},
      {:ok, %Q{state: :prepared}, :new_state},
      {:ok, %C{}, :newer_state},
      {:deallocate, %R{}, :newest_state},
      {:ok, :deallocated, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.run(pool, fn(conn) ->
      opts2 = [parse: fn(%Q{state: nil}) -> %Q{state: :parsed} end,
               describe: fn(%Q{state: :prepared}) -> %Q{state: :described} end,
               encode: fn([:param]) -> :encoded end,
               decode: fn(%R{}) -> :decoded end]
      stream = P.prepare_stream(conn, %Q{}, [:param], opts2)
      assert Enum.to_list(stream) == [:decoded]
      :hi
    end) == :hi

    assert [
      connect: [_],
      handle_prepare: [%Q{state: :parsed}, _, :state],
      handle_declare: [%Q{state: :described}, :encoded, _, :new_state],
      handle_first: [%Q{state: :described}, %C{}, _, :newer_state],
      handle_deallocate: [%Q{}, %C{}, _, :newest_state]
      ] = A.record(agent)
  end

  test "prepare_stream logs result" do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state},
      {:ok, %C{}, :newer_state},
      {:ok, %R{}, :newest_state},
      {:ok, :deallocated, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)
    assert P.run(pool, fn(conn) ->
      stream = P.prepare_stream(conn, %Q{}, [:param], [log: &send(parent, &1)])
      assert Enum.take(stream, 1) == [%R{}]
      :hi
    end) == :hi

    assert_received %DBConnection.LogEntry{call: :prepare_declare} = entry
    assert %{query: %Q{}, params: [:param], result: {:ok, %Q{}, %C{}}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert_received %DBConnection.LogEntry{call: :first} = entry
    assert %{query: %Q{}, params: %C{}, result: {:ok, %R{}}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_integer(entry.decode_time)
    assert entry.decode_time >= 0

    assert_received %DBConnection.LogEntry{call: :deallocate} = entry
    assert %{query: %Q{}, params: %C{}, result: {:ok, :deallocated}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state],
      handle_declare: [%Q{}, [:param], _, :new_state],
      handle_first: [%Q{}, %C{}, _, :newer_state],
      handle_deallocate: [%Q{}, %C{}, _, :newest_state]
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

    assert P.run(pool, fn(conn) ->
      stream = P.prepare_stream(conn, %Q{}, [:param], [log: &send(parent, &1)])
      assert_raise RuntimeError, "oops",  fn() -> Enum.take(stream, 1) end
      :hi
    end) == :hi

    assert_received %DBConnection.LogEntry{call: :prepare_declare} = entry
    assert %{query: %Q{}, params: [:param], result: {:error, ^err}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state]
      ] = A.record(agent)
  end

  test "prepare_stream logs declare error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state},
      {:error, err, :newer_state},
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    assert P.run(pool, fn(conn) ->
      stream = P.prepare_stream(conn, %Q{}, [:param], [log: &send(parent, &1)])
      assert_raise RuntimeError, "oops",  fn() -> Enum.take(stream, 1) end
      :hi
    end) == :hi

    assert_received %DBConnection.LogEntry{call: :prepare_declare} = entry
    assert %{query: %Q{}, params: [:param], result: {:error, ^err}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state],
      handle_declare: [%Q{}, [:param], _, :new_state]
      ] = A.record(agent)
  end

  test "prepare_stream declare disconnects" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state},
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

    assert P.run(pool, fn(conn) ->
      stream = P.prepare_stream(conn, %Q{}, [:param])
      assert_raise RuntimeError, "oops",  fn() -> Enum.take(stream, 1) end
      :hi
    end) == :hi

    assert_receive :reconnected

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state],
      handle_declare: [%Q{}, [:param], _, :new_state],
      disconnect: [^err, :newer_state],
      connect: [_]
      ] = A.record(agent)
  end

  test "prepare_stream prepare bad return raises and stops" do
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
    assert P.run(pool, fn(conn) ->
      stream = P.prepare_stream(conn, %Q{}, [:param])
      assert_raise DBConnection.ConnectionError, "bad return value: :oops",
        fn() -> Enum.to_list(stream) end
      :hi
    end) == :hi

    prefix = "client #{inspect self()} stopped: " <>
      "** (DBConnection.ConnectionError) bad return value: :oops"
    len = byte_size(prefix)
    assert_receive {:EXIT, ^conn,
      {%DBConnection.ConnectionError{message: <<^prefix::binary-size(len), _::binary>>},
        [_|_]}}

    assert [
      {:connect, _},
      {:handle_prepare, [%Q{}, _, :state]} | _] = A.record(agent)
  end

  test "prepare_stream declare raise raises and stops connection" do
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
    assert P.run(pool, fn(conn) ->
      stream = P.prepare_stream(conn, %Q{}, [:param])
      assert_raise RuntimeError, "oops", fn() -> Enum.to_list(stream) end
      :hi
    end) == :hi

    prefix = "client #{inspect self()} stopped: ** (RuntimeError) oops"
    len = byte_size(prefix)
    assert_receive {:EXIT, ^conn,
      {%DBConnection.ConnectionError{message: <<^prefix::binary-size(len), _::binary>>},
        [_|_]}}

    assert [
      {:connect, _},
      {:handle_prepare, [%Q{}, _, :state]},
      {:handle_declare, [%Q{}, [:param], _, :new_state]} | _] = A.record(agent)
  end

  test "prepare_stream describe or encode raises and closes query" do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state},
      {:ok, :closed, :newer_state},
      {:ok, %Q{}, :newest_state},
      {:ok, :result, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    assert P.run(pool, fn(conn) ->
      describe = fn(%Q{}) -> raise "oops" end
      stream = P.prepare_stream(conn, %Q{}, [:param], [describe: describe])
      assert_raise RuntimeError,  "oops", fn() -> Enum.to_list(stream) end

      encode = fn([:param]) -> raise "oops" end
      stream = P.prepare_stream(conn, %Q{}, [:param], [encode: encode])
      assert_raise RuntimeError, "oops", fn() -> Enum.to_list(stream) end

      :hi
    end) == :hi

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state],
      handle_close: [%Q{}, _, :new_state],
      handle_prepare: [%Q{}, _, :newer_state],
      handle_close: [%Q{}, _, :newest_state]
      ] = A.record(agent)
  end
end
