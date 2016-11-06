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
      {:ok, :began, :new_state},
      {:ok, %C{}, :newer_state},
      {:ok, %R{}, :newest_state},
      {:deallocate, %R{}, :state2},
      {:ok, :deallocated, :new_state2},
      {:ok, :commited, :newer_state2}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.transaction(pool, fn(conn) ->
      stream = P.stream(conn, %Q{}, [:param])
      assert %DBConnection.Stream{} = stream
      assert Enum.to_list(stream) == [%R{}, %R{}]
      :hi
    end) == {:ok, :hi}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_declare: [%Q{}, [:param], _, :new_state],
      handle_first: [%Q{}, %C{}, _, :newer_state],
      handle_next: [%Q{}, %C{}, _, :newest_state],
      handle_deallocate: [%Q{}, %C{}, _, :state2],
      handle_commit: [_, :new_state2]
      ] = A.record(agent)
  end

  test "stream encodes params and decodes result" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %C{}, :newer_state},
      {:deallocate, %R{}, :newest_state},
      {:ok, :deallocated, :state2},
      {:ok, :committed, :new_state2}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn(conn) ->
      opts2 = [encode: fn([:param]) -> :encoded end,
               decode: fn(%R{}) -> :decoded end]
      stream = P.stream(conn, %Q{}, [:param], opts2)
      assert Enum.to_list(stream) == [:decoded]
      :hi
    end) == {:ok, :hi}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_declare: [_, :encoded, _, :new_state],
      handle_first: [%Q{}, %C{}, _, :newer_state],
      handle_deallocate: [%Q{}, %C{}, _, :newest_state],
      handle_commit: [_, :state2]
      ] = A.record(agent)
  end

  test "stream logs result" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %C{}, :newer_state},
      {:ok, %R{}, :newest_state},
      {:ok, :result, :state2},
      {:ok, :committed, :new_state2}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)
    assert P.transaction(pool, fn(conn) ->
      stream = P.stream(conn, %Q{}, [:param], [log: &send(parent, &1)])
      assert Enum.take(stream, 1) == [%R{}]
      :hi
    end) == {:ok, :hi}

    assert_received %DBConnection.LogEntry{call: :declare} = entry
    assert %{query: %Q{}, params: [:param], result: {:ok, %C{}}} = entry
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
    assert %{query: %Q{}, params: %C{}, result: {:ok, :result}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_declare: [%Q{}, [:param], _, :new_state],
      handle_first: [%Q{}, %C{}, _, :newer_state],
      handle_deallocate: [%Q{}, %C{}, _, :newest_state],
      handle_commit: [_, :state2]
      ] = A.record(agent)
  end

  test "stream logs declare error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:error, err, :newer_state},
      {:ok, :comitted, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn(conn) ->
      stream = P.stream(conn, %Q{}, [:param], [log: &send(parent, &1)])
      assert_raise RuntimeError, "oops",  fn() -> Enum.take(stream, 1) end
      :hi
    end) == {:ok, :hi}

    assert_received %DBConnection.LogEntry{call: :declare} = entry
    assert %{query: %Q{}, params: [:param], result: {:error, ^err}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_declare: [%Q{}, [:param], _, :new_state],
      handle_commit: [_, :newer_state],
      ] = A.record(agent)
  end

  test "stream declare disconnects" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
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

    assert P.transaction(pool, fn(conn) ->
      stream = P.stream(conn, %Q{}, [:param])
      assert_raise RuntimeError, "oops",  fn() -> Enum.take(stream, 1) end
      :hi
    end) == {:error, :rollback}

    assert_receive :reconnected

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_declare: [%Q{}, [:param], _, :new_state],
      disconnect: [^err, :newer_state],
      connect: [_]
      ] = A.record(agent)
  end

  test "stream logs first disconnects" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
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

    assert P.transaction(pool, fn(conn) ->
      stream = P.stream(conn, %Q{}, [:param], [log: &send(parent, &1)])
      assert_raise RuntimeError, "oops",  fn() -> Enum.take(stream, 1) end
      :hi
    end) == {:error, :rollback}

    assert_received %DBConnection.LogEntry{call: :declare}

    assert_received %DBConnection.LogEntry{call: :first} = entry
    assert %{query: %Q{}, params: %C{}, result: {:error, ^err}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    refute_received %DBConnection.LogEntry{call: :deallocate}

    assert_receive :reconnected

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_declare: [%Q{}, [:param], _, :new_state],
      handle_first: [%Q{}, %C{}, _, :newer_state],
      disconnect: [^err, :newest_state],
      connect: [_]
      ] = A.record(agent)
  end

  test "stream logs deallocate disconnect" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %C{}, :newer_state},
      {:ok, %R{}, :newest_state},
      {:disconnect, err, :state2},
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

    assert P.transaction(pool, fn(conn) ->
      stream = P.stream(conn, %Q{}, [:param], [log: &send(parent, &1)])
      assert_raise RuntimeError, "oops",  fn() -> Enum.take(stream, 1) end
      :hi
    end) == {:error, :rollback}

    assert_received %DBConnection.LogEntry{call: :declare}
    assert_received %DBConnection.LogEntry{call: :first}

    assert_received %DBConnection.LogEntry{call: :deallocate} = entry
    assert %{query: %Q{}, params: %C{}, result: {:error, ^err}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert_receive :reconnected

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_declare: [%Q{}, [:param], _, :new_state],
      handle_first: [%Q{}, %C{}, _, :newer_state],
      handle_deallocate: [%Q{}, %C{}, _, :newest_state],
      disconnect: [^err, :state2],
      connect: [_]
      ] = A.record(agent)
  end

  test "stream declare bad return raises and stops" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, :began, :new_state},
      :oops,
      {:ok, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}

    Process.flag(:trap_exit, true)
    assert P.transaction(pool, fn(conn) ->
      stream = P.stream(conn, %Q{}, [:param])
      assert_raise DBConnection.ConnectionError, "bad return value: :oops",
        fn() -> Enum.to_list(stream) end
      :hi
    end) == {:error, :rollback}

    prefix = "client #{inspect self()} stopped: " <>
      "** (DBConnection.ConnectionError) bad return value: :oops"
    len = byte_size(prefix)
    assert_receive {:EXIT, ^conn,
      {%DBConnection.ConnectionError{message: <<^prefix::binary-size(len), _::binary>>},
        [_|_]}}

    assert [
      {:connect, _},
      {:handle_begin, [_, :state]},
      {:handle_declare, [%Q{}, [:param], _, :new_state]} | _] = A.record(agent)
  end

  test "stream declare raise raises and stops connection" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, :began, :new_state},
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
    assert P.transaction(pool, fn(conn) ->
      stream = P.stream(conn, %Q{}, [:param])
      assert_raise RuntimeError, "oops", fn() -> Enum.to_list(stream) end
      :hi
    end) == {:error, :rollback}

    prefix = "client #{inspect self()} stopped: ** (RuntimeError) oops"
    len = byte_size(prefix)
    assert_receive {:EXIT, ^conn,
      {%DBConnection.ConnectionError{message: <<^prefix::binary-size(len), _::binary>>},
        [_|_]}}

    assert [
      {:connect, _},
      {:handle_begin, [_, :state]},
      {:handle_declare, [%Q{}, [:param], _, :new_state]} | _] = A.record(agent)
  end

  test "stream declare log raises and deallocates" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %C{}, :newer_state},
      {:ok, :deallocated, :newest_state},
      {:ok, :commited, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    Process.flag(:trap_exit, true)
    assert P.transaction(pool, fn(conn) ->
      opts2 = [log: fn(_) -> raise "oops" end]
      stream = P.stream(conn, %Q{}, [:param], opts2)
      assert_raise RuntimeError, "oops", fn() -> Enum.to_list(stream) end
      :hi
    end) == {:ok, :hi}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_declare: [%Q{}, [:param], _, :new_state],
      handle_deallocate: [%Q{}, %C{}, _, :newer_state],
      handle_commit: [_, :newest_state]
      ] = A.record(agent)
  end

  test "stream first bad return raises and stops" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, :began, :new_state},
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
    assert P.transaction(pool, fn(conn) ->
      stream = P.stream(conn, %Q{}, [:param])
      assert_raise DBConnection.ConnectionError, "bad return value: :oops",
        fn() -> Enum.to_list(stream) end
      :hi
    end) == {:error, :rollback}

    prefix = "client #{inspect self()} stopped: " <>
      "** (DBConnection.ConnectionError) bad return value: :oops"
    len = byte_size(prefix)
    assert_receive {:EXIT, ^conn,
      {%DBConnection.ConnectionError{message: <<^prefix::binary-size(len), _::binary>>},
        [_|_]}}

    assert [
      {:connect, _},
      {:handle_begin, [_, :state]},
      {:handle_declare, [%Q{}, [:param], _, :new_state]},
      {:handle_first, [%Q{}, %C{}, _, :newer_state]} | _] = A.record(agent)
  end

  test "stream deallocate raise raises and stops connection" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, :began, :new_state},
      {:ok, %C{}, :newer_state},
      {:ok, %R{}, :newest_state},
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
    assert P.transaction(pool, fn(conn) ->
      stream = P.stream(conn, %Q{}, [:param])
      assert_raise RuntimeError, "oops", fn() -> Enum.take(stream, 1) end
      :hi
    end) == {:error, :rollback}

    prefix = "client #{inspect self()} stopped: ** (RuntimeError) oops"
    len = byte_size(prefix)
    assert_receive {:EXIT, ^conn,
      {%DBConnection.ConnectionError{message: <<^prefix::binary-size(len), _::binary>>},
        [_|_]}}

    assert [
      {:connect, _},
      {:handle_begin, [_, :state]},
      {:handle_declare, [%Q{}, [:param], _, :new_state]},
      {:handle_first, [%Q{}, %C{}, _, :newer_state]},
      {:handle_deallocate, [%Q{}, %C{}, _, :newest_state]} | _] = A.record(agent)
  end
end
