defmodule StreamTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestCursor, as: C
  alias TestResult, as: R

  test "stream returns result" do
    stack = [
      {:ok, :state},
      {:ok, %C{}, :new_state},
      {:ok, %R{}, :newer_state},
      {:deallocate, %R{}, :newest_state},
      {:ok, :deallocated, :state2},
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.run(pool, fn(conn) ->
      stream = P.stream(conn, %Q{}, [:param])
      assert %DBConnection.Stream{} = stream
      assert Enum.to_list(stream) == [%R{}, %R{}]
      :hi
    end) == :hi

    assert [
      connect: [_],
      handle_declare: [%Q{}, [:param], _, :state],
      handle_first: [%Q{}, %C{}, _, :new_state],
      handle_next: [%Q{}, %C{}, _, :newer_state],
      handle_deallocate: [%Q{}, %C{}, _, :newest_state]
      ] = A.record(agent)
  end

  test "stream encodes params and decodes result" do
    stack = [
      {:ok, :state},
      {:ok, %C{}, :new_state},
      {:deallocate, %R{}, :newer_state},
      {:ok, :deallocated, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.run(pool, fn(conn) ->
      opts2 = [encode: fn([:param]) -> :encoded end,
               decode: fn(%R{}) -> :decoded end]
      stream = P.stream(conn, %Q{}, [:param], opts2)
      assert Enum.to_list(stream) == [:decoded]
      :hi
    end) == :hi

    assert [
      connect: [_],
      handle_declare: [_, :encoded, _, :state],
      handle_first: [%Q{}, %C{}, _, :new_state],
      handle_deallocate: [%Q{}, %C{}, _, :newer_state]
      ] = A.record(agent)
  end

  test "stream logs result" do
    stack = [
      {:ok, :state},
      {:ok, %C{}, :new_state},
      {:ok, %R{}, :newer_state},
      {:ok, :result, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)
    assert P.run(pool, fn(conn) ->
      stream = P.stream(conn, %Q{}, [:param], [log: &send(parent, &1)])
      assert Enum.take(stream, 1) == [%R{}]
      :hi
    end) == :hi

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
      handle_declare: [%Q{}, [:param], _, :state],
      handle_first: [%Q{}, %C{}, _, :new_state],
      handle_deallocate: [%Q{}, %C{}, _, :newer_state]
      ] = A.record(agent)
  end

  test "stream logs declare error" do
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
      stream = P.stream(conn, %Q{}, [:param], [log: &send(parent, &1)])
      assert_raise RuntimeError, "oops",  fn() -> Enum.take(stream, 1) end
      :hi
    end) == :hi

    assert_received %DBConnection.LogEntry{call: :declare} = entry
    assert %{query: %Q{}, params: [:param], result: {:error, ^err}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert [
      connect: [_],
      handle_declare: [%Q{}, [:param], _, :state]
      ] = A.record(agent)
  end

  test "stream declare disconnects" do
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

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    assert P.run(pool, fn(conn) ->
      stream = P.stream(conn, %Q{}, [:param])
      assert_raise RuntimeError, "oops",  fn() -> Enum.take(stream, 1) end
      :hi
    end) == :hi

    assert_receive :reconnected

    assert [
      connect: [_],
      handle_declare: [%Q{}, [:param], _, :state],
      disconnect: [^err, :new_state],
      connect: [_]
      ] = A.record(agent)
  end

  test "stream logs first disconnects" do
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

    assert P.run(pool, fn(conn) ->
      stream = P.stream(conn, %Q{}, [:param], [log: &send(parent, &1)])
      assert_raise RuntimeError, "oops",  fn() -> Enum.take(stream, 1) end
      :hi
    end) == :hi

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
      handle_declare: [%Q{}, [:param], _, :state],
      handle_first: [%Q{}, %C{}, _, :new_state],
      disconnect: [^err, :newer_state],
      connect: [_]
      ] = A.record(agent)
  end

  test "stream logs deallocate disconnect" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, %C{}, :new_state},
      {:ok, %R{}, :newer_state},
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

    assert P.run(pool, fn(conn) ->
      stream = P.stream(conn, %Q{}, [:param], [log: &send(parent, &1)])
      assert_raise RuntimeError, "oops",  fn() -> Enum.take(stream, 1) end
      :hi
    end) == :hi

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
      handle_declare: [%Q{}, [:param], _, :state],
      handle_first: [%Q{}, %C{}, _, :new_state],
      handle_deallocate: [%Q{}, %C{}, _, :newer_state],
      disconnect: [^err, :newest_state],
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
      stream = P.stream(conn, %Q{}, [:param])
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
      {:handle_declare, [%Q{}, [:param], _, :state]} | _] = A.record(agent)
  end

  test "stream declare raise raises and stops connection" do
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
    assert P.run(pool, fn(conn) ->
      stream = P.stream(conn, %Q{}, [:param])
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
      {:handle_declare, [%Q{}, [:param], _, :state]} | _] = A.record(agent)
  end

  test "stream declare log raises and continues" do
    stack = [
      {:ok, :state},
      {:ok, %C{}, :new_state},
      {:deallocate, %R{}, :newer_state},
      {:ok, :deallocated, :newest_state},
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    assert P.run(pool, fn(conn) ->
      opts2 = [log: fn(_) -> raise "logging oops" end]
      stream = P.stream(conn, %Q{}, [:param], opts2)
      assert capture_log(fn() ->
        assert Enum.to_list(stream) == [%R{}]
      end) =~ "logging oops"
      :hi
    end) == :hi

    assert [
      connect: [_],
      handle_declare: [%Q{}, [:param], _, :state],
      handle_first: [%Q{}, %C{}, _, :new_state],
      handle_deallocate: [%Q{}, %C{}, _, :newer_state]
      ] = A.record(agent)
  end

  test "stream first bad return raises and stops" do
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
    assert P.run(pool, fn(conn) ->
      stream = P.stream(conn, %Q{}, [:param])
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
      {:handle_declare, [%Q{}, [:param], _, :state]},
      {:handle_first, [%Q{}, %C{}, _, :new_state]} | _] = A.record(agent)
  end

  test "stream deallocate raise raises and stops connection" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, %C{}, :new_state},
      {:ok, %R{}, :newer_state},
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
      stream = P.stream(conn, %Q{}, [:param])
      assert_raise RuntimeError, "oops", fn() -> Enum.take(stream, 1) end
      :hi
    end) == :hi

    prefix = "client #{inspect self()} stopped: ** (RuntimeError) oops"
    len = byte_size(prefix)
    assert_receive {:EXIT, ^conn,
      {%DBConnection.ConnectionError{message: <<^prefix::binary-size(len), _::binary>>},
        [_|_]}}

    assert [
      {:connect, _},
      {:handle_declare, [%Q{}, [:param], _, :state]},
      {:handle_first, [%Q{}, %C{}, _, :new_state]},
      {:handle_deallocate, [%Q{}, %C{}, _, :newer_state]} | _] = A.record(agent)
  end
end
