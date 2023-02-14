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
      {:ok, :began, :new_state},
      {:ok, %Q{}, :newer_state},
      {:ok, %Q{}, %C{}, :newest_state},
      {:cont, %R{}, :state2},
      {:halt, %R{}, :new_state2},
      {:ok, :deallocated, :newer_state2},
      {:ok, :committed, :newest_state2}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn conn ->
             stream = P.prepare_stream(conn, %Q{}, [:param])
             assert %DBConnection.PrepareStream{} = stream
             assert Enum.to_list(stream) == [%R{}, %R{}]
             :hi
           end) == {:ok, :hi}

    assert [
             connect: [_],
             handle_begin: [_, :state],
             handle_prepare: [%Q{}, _, :new_state],
             handle_declare: [%Q{}, [:param], _, :newer_state],
             handle_fetch: [%Q{}, %C{}, _, :newest_state],
             handle_fetch: [%Q{}, %C{}, _, :state2],
             handle_deallocate: [%Q{}, %C{}, _, :new_state2],
             handle_commit: [_, :newer_state2]
           ] = A.record(agent)
  end

  test "prepare_stream parses/describes/encodes/decodes" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %Q{state: :prepared}, :newer_state},
      {:ok, %Q{state: :declared}, %C{}, :newest_state},
      {:halt, %R{}, :state2},
      {:ok, :deallocated, :new_state2},
      {:ok, :committed, :newer_state2}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn conn ->
             opts2 = [
               parse: fn %Q{state: nil} -> %Q{state: :parsed} end,
               describe: fn %Q{state: :prepared} -> %Q{state: :described} end,
               encode: fn [:param] -> :encoded end,
               decode: fn %R{} -> :decoded end
             ]

             stream = P.prepare_stream(conn, %Q{}, [:param], opts2)
             assert Enum.to_list(stream) == [:decoded]
             :hi
           end) == {:ok, :hi}

    assert [
             connect: [_],
             handle_begin: [_, :state],
             handle_prepare: [%Q{state: :parsed}, _, :new_state],
             handle_declare: [%Q{state: :described}, :encoded, _, :newer_state],
             handle_fetch: [%Q{state: :declared}, %C{}, _, :newest_state],
             handle_deallocate: [%Q{}, %C{}, _, :state2],
             handle_commit: [_, :new_state2]
           ] = A.record(agent)
  end

  test "prepare_stream logs result" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %Q{}, :newer_state},
      {:ok, %Q{}, %C{}, :newest_state},
      {:halt, %R{}, :state2},
      {:ok, :deallocated, :new_state2},
      {:ok, :committed, :newest_state2}
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn conn ->
             stream = P.prepare_stream(conn, %Q{}, [:param], log: &send(parent, &1))
             assert Enum.take(stream, 1) == [%R{}]
             :hi
           end) == {:ok, :hi}

    assert_received %DBConnection.LogEntry{call: :prepare_declare} = entry
    assert %{query: %Q{}, params: [:param], result: {:ok, %Q{}, %C{}}} = entry
    assert is_nil(entry.pool_time)
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

    assert_received %DBConnection.LogEntry{call: :deallocate} = entry
    assert %{query: %Q{}, params: %C{}, result: {:ok, :deallocated}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert [
             connect: [_],
             handle_begin: [_, :state],
             handle_prepare: [%Q{}, _, :new_state],
             handle_declare: [%Q{}, [:param], _, :newer_state],
             handle_fetch: [%Q{}, %C{}, _, :newest_state],
             handle_deallocate: [%Q{}, %C{}, _, :state2],
             handle_commit: [_, :new_state2]
           ] = A.record(agent)
  end

  test "prepare_stream logs prepare error" do
    err = RuntimeError.exception("oops")

    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:error, err, :newer_state},
      {:ok, :committed, :newest_state}
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn conn ->
             stream = P.prepare_stream(conn, %Q{}, [:param], log: &send(parent, &1))
             assert_raise RuntimeError, "oops", fn -> Enum.take(stream, 1) end
             :hi
           end) == {:ok, :hi}

    assert_received %DBConnection.LogEntry{call: :prepare_declare} = entry
    assert %{query: %Q{}, params: [:param], result: {:error, ^err}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert [
             connect: [_],
             handle_begin: [_, :state],
             handle_prepare: [%Q{}, _, :new_state],
             handle_commit: [_, :newer_state]
           ] = A.record(agent)
  end

  test "prepare_stream logs declare error" do
    err = RuntimeError.exception("oops")

    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %Q{}, :newer_state},
      {:error, err, :newest_state},
      {:ok, :committed, :state2}
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn conn ->
             stream = P.prepare_stream(conn, %Q{}, [:param], log: &send(parent, &1))
             assert_raise RuntimeError, "oops", fn -> Enum.take(stream, 1) end
             :hi
           end) == {:ok, :hi}

    assert_received %DBConnection.LogEntry{call: :prepare_declare} = entry
    assert %{query: %Q{}, params: [:param], result: {:error, ^err}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert [
             connect: [_],
             handle_begin: [_, :state],
             handle_prepare: [%Q{}, _, :new_state],
             handle_declare: [%Q{}, [:param], _, :newer_state],
             handle_commit: [_, :newest_state]
           ] = A.record(agent)
  end

  test "prepare_stream declare disconnects" do
    err = RuntimeError.exception("oops")

    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %Q{}, :newer_state},
      {:disconnect, err, :newest_state},
      :ok,
      fn opts ->
        send(opts[:parent], :reconnected)
        {:ok, :state2}
      end
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn conn ->
             stream = P.prepare_stream(conn, %Q{}, [:param])
             assert_raise RuntimeError, "oops", fn -> Enum.take(stream, 1) end
             :hi
           end) == {:error, :rollback}

    assert_receive :reconnected

    assert [
             connect: [_],
             handle_begin: [_, :state],
             handle_prepare: [%Q{}, _, :new_state],
             handle_declare: [%Q{}, [:param], _, :newer_state],
             disconnect: [^err, :newest_state],
             connect: [_]
           ] = A.record(agent)
  end

  test "prepare_stream prepare bad return raises and stops" do
    stack = [
      fn opts ->
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

    assert P.transaction(pool, fn conn ->
             stream = P.prepare_stream(conn, %Q{}, [:param])

             assert_raise DBConnection.ConnectionError, "bad return value: :oops", fn ->
               Enum.to_list(stream)
             end

             :hi
           end) == {:error, :rollback}

    prefix =
      "client #{inspect(self())} stopped: " <>
        "** (DBConnection.ConnectionError) bad return value: :oops"

    len = byte_size(prefix)

    assert_receive {:EXIT, ^conn,
                    {%DBConnection.ConnectionError{
                       message: <<^prefix::binary-size(len), _::binary>>
                     }, [_ | _]}}

    assert [
             {:connect, _},
             {:handle_begin, [_, :state]},
             {:handle_prepare, [%Q{}, _, :new_state]} | _
           ] = A.record(agent)
  end

  test "prepare_stream declare raise raises and stops connection" do
    stack = [
      fn opts ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, :began, :new_state},
      {:ok, %Q{}, :newer_state},
      fn _, _, _, _ ->
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

    assert P.transaction(pool, fn conn ->
             stream = P.prepare_stream(conn, %Q{}, [:param])
             assert_raise RuntimeError, "oops", fn -> Enum.to_list(stream) end
             :hi
           end) == {:error, :rollback}

    prefix = "client #{inspect(self())} stopped: ** (RuntimeError) oops"
    len = byte_size(prefix)

    assert_receive {:EXIT, ^conn,
                    {%DBConnection.ConnectionError{
                       message: <<^prefix::binary-size(len), _::binary>>
                     }, [_ | _]}}

    assert [
             {:connect, _},
             {:handle_begin, [_, :state]},
             {:handle_prepare, [%Q{}, _, :new_state]},
             {:handle_declare, [%Q{}, [:param], _, :newer_state]} | _
           ] = A.record(agent)
  end

  test "prepare_stream describe or encode raises and closes query" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %Q{}, :newer_state},
      {:ok, :closed, :newest_state},
      {:ok, %Q{}, :state2},
      {:ok, :result, :new_state2},
      {:ok, :committed, :newer_state2}
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn conn ->
             describe = fn %Q{} -> raise "oops" end
             stream = P.prepare_stream(conn, %Q{}, [:param], describe: describe)
             assert_raise RuntimeError, "oops", fn -> Enum.to_list(stream) end

             encode = fn [:param] -> raise "oops" end
             stream = P.prepare_stream(conn, %Q{}, [:param], encode: encode)
             assert_raise RuntimeError, "oops", fn -> Enum.to_list(stream) end

             :hi
           end) == {:ok, :hi}

    assert [
             connect: [_],
             handle_begin: [_, :state],
             handle_prepare: [%Q{}, _, :new_state],
             handle_close: [%Q{}, _, :newer_state],
             handle_prepare: [%Q{}, _, :newest_state],
             handle_close: [%Q{}, _, :state2],
             handle_commit: [_, :new_state2]
           ] = A.record(agent)
  end
end
