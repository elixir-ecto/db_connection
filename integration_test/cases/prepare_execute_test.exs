defmodule PrepareExecuteTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestResult, as: R

  test "prepare_execute returns query and result" do
    stack = [
      {:ok, :state},
      {:ok, %Q{state: :prepared}, :new_state},
      {:ok, %Q{state: :executed}, %R{}, :newer_state},
      {:ok, %Q{state: :prepared}, :newest_state},
      {:ok, %Q{state: :executed}, %R{}, :state2}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.prepare_execute(pool, %Q{}, [:param]) ==
             {:ok, %Q{state: :executed}, %R{}}

    assert P.prepare_execute(pool, %Q{}, [:param], key: :value) ==
             {:ok, %Q{state: :executed}, %R{}}

    assert [
             connect: [_],
             handle_prepare: [%Q{}, _, :state],
             handle_execute: [%Q{state: :prepared}, [:param], _, :new_state],
             handle_prepare: [%Q{}, [{:key, :value} | _], :newer_state],
             handle_execute: [%Q{state: :prepared}, [:param], _, :newest_state]
           ] = A.record(agent)
  end

  test "prepare_execute parses query" do
    stack = [
      {:ok, :state},
      {:ok, %Q{state: :prepared}, :new_state},
      {:ok, %Q{state: :executed}, %R{}, :newer_stater}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    opts2 = [parse: fn %Q{} -> %Q{state: :parsed} end]
    assert P.prepare_execute(pool, %Q{}, [:param], opts2) == {:ok, %Q{state: :executed}, %R{}}

    assert [
             connect: [_],
             handle_prepare: [%Q{state: :parsed}, _, :state],
             handle_execute: [%Q{state: :prepared}, [:param], _, :new_state]
           ] = A.record(agent)
  end

  test "prepare_execute describes query" do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state},
      {:ok, %Q{state: :executed}, %R{}, :newer_state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    opts2 = [describe: fn %Q{} -> %Q{state: :described} end]
    assert P.prepare_execute(pool, %Q{}, [:param], opts2) == {:ok, %Q{state: :executed}, %R{}}

    assert [
             connect: [_],
             handle_prepare: [%Q{}, _, :state],
             handle_execute: [%Q{state: :described}, [:param], _, :new_state]
           ] = A.record(agent)
  end

  test "prepare_execute encodes params and decodes result" do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state},
      {:ok, %Q{state: :executed}, %R{}, :newer_state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    opts2 = [encode: fn [:param] -> :encoded end, decode: fn %R{} -> :decoded end]
    assert P.prepare_execute(pool, %Q{}, [:param], opts2) == {:ok, %Q{state: :executed}, :decoded}

    assert [
             connect: [_],
             handle_prepare: [%Q{}, _, :state],
             handle_execute: [_, :encoded, _, :new_state]
           ] = A.record(agent)
  end

  test "prepare_execute logs result" do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state},
      {:ok, %Q{}, %R{}, :newer_state}
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    log = fn entry ->
      assert %DBConnection.LogEntry{
               call: :prepare_execute,
               query: %Q{},
               params: [:param],
               result: {:ok, %Q{}, %R{}}
             } = entry

      assert is_integer(entry.pool_time)
      assert entry.pool_time >= 0
      assert is_integer(entry.connection_time)
      assert entry.connection_time >= 0
      assert is_integer(entry.decode_time)
      assert entry.decode_time >= 0
      send(parent, :logged)
    end

    assert P.prepare_execute(pool, %Q{}, [:param], log: log) ==
             {:ok, %Q{}, %R{}}

    assert_received :logged

    assert [
             connect: [_],
             handle_prepare: [%Q{}, _, :state],
             handle_execute: [%Q{}, [:param], _, :new_state]
           ] = A.record(agent)
  end

  test "prepare_execute prepare error returns error" do
    err = RuntimeError.exception("oops")

    stack = [
      {:ok, :state},
      {:error, err, :new_state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.prepare_execute(pool, %Q{}, [:param]) == {:error, err}

    assert [
             connect: [_],
             handle_prepare: [%Q{}, _, :state]
           ] = A.record(agent)
  end

  test "prepare_execute logs error" do
    err = RuntimeError.exception("oops")

    stack = [
      {:ok, :state},
      {:error, err, :new_state}
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    log = fn entry ->
      assert %DBConnection.LogEntry{
               call: :prepare_execute,
               query: %Q{},
               params: [:param],
               result: {:error, ^err}
             } = entry

      assert is_integer(entry.pool_time)
      assert entry.pool_time >= 0
      assert is_integer(entry.connection_time)
      assert entry.connection_time >= 0
      assert is_nil(entry.decode_time)
      send(parent, :logged)
    end

    assert P.prepare_execute(pool, %Q{}, [:param], log: log) == {:error, err}
    assert_received :logged

    assert [
             connect: [_],
             handle_prepare: [%Q{}, _, :state]
           ] = A.record(agent)
  end

  test "prepare_execute logs prepare raise" do
    stack = [
      fn opts ->
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      fn _, _, _ ->
        raise "oops"
      end,
      :ok,
      {:ok, :state2}
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    _ = Process.flag(:trap_exit, true)
    {:ok, pool} = P.start_link(opts)

    log = fn entry ->
      assert %DBConnection.LogEntry{
               call: :prepare_execute,
               query: %Q{},
               params: [:param],
               result: {:error, err}
             } = entry

      assert %DBConnection.ConnectionError{
               message: "an exception was raised: ** (RuntimeError) oops" <> _
             } = err

      assert is_integer(entry.pool_time)
      assert entry.pool_time >= 0
      assert is_integer(entry.connection_time)
      assert entry.connection_time >= 0
      assert is_nil(entry.decode_time)
      send(parent, :logged)
    end

    assert_raise RuntimeError, "oops", fn -> P.prepare_execute(pool, %Q{}, [:param], log: log) end
    assert_received :logged
    assert_receive {:EXIT, _, {%DBConnection.ConnectionError{}, [_ | _]}}

    assert [
             {:connect, [_]},
             {:handle_prepare, [%Q{}, _, :state]},
             {:disconnect, _} | _
           ] = A.record(agent)
  end

  test "execute logs parse and describe raise" do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state},
      {:ok, :closed, :newer_state}
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    log = fn entry ->
      assert %DBConnection.LogEntry{
               call: :prepare_execute,
               query: %Q{},
               params: [:param],
               result: {:error, err}
             } = entry

      assert %DBConnection.ConnectionError{
               message: "an exception was raised: ** (RuntimeError) oops" <> _
             } = err

      assert is_nil(entry.pool_time)
      assert is_nil(entry.connection_time)
      assert is_nil(entry.decode_time)
      send(parent, :logged)
    end

    opts2 = [parse: fn %Q{} -> raise "oops" end, log: log]
    assert_raise RuntimeError, "oops", fn -> P.prepare_execute(pool, %Q{}, [:param], opts2) end
    assert_received :logged

    log = fn entry ->
      assert %DBConnection.LogEntry{
               call: :prepare_execute,
               query: %Q{},
               params: [:param],
               result: {:error, err}
             } = entry

      assert %DBConnection.ConnectionError{
               message: "an exception was raised: ** (RuntimeError) oops" <> _
             } = err

      assert is_integer(entry.pool_time)
      assert entry.pool_time >= 0
      assert is_integer(entry.connection_time)
      assert entry.connection_time >= 0
      assert is_nil(entry.decode_time)
      send(parent, :logged)
    end

    opts3 = [describe: fn %Q{} -> raise "oops" end, log: log]
    assert_raise RuntimeError, "oops", fn -> P.prepare_execute(pool, %Q{}, [:param], opts3) end
    assert_received :logged

    assert [
             connect: [_],
             handle_prepare: [%Q{}, _, :state],
             handle_close: [%Q{}, _, :new_state]
           ] = A.record(agent)
  end

  test "prepare_execute logs encode and decode raise" do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state},
      {:ok, :closed, :newer_state},
      {:ok, %Q{}, :newest_state},
      {:ok, %Q{}, %R{}, :state2}
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    log = fn entry ->
      assert %DBConnection.LogEntry{
               call: :prepare_execute,
               query: %Q{},
               params: [:param],
               result: {:error, err}
             } = entry

      assert %DBConnection.ConnectionError{
               message: "an exception was raised: ** (RuntimeError) oops" <> _
             } = err

      assert is_integer(entry.pool_time)
      assert entry.pool_time >= 0
      assert is_integer(entry.connection_time)
      assert is_nil(entry.decode_time)
      send(parent, :logged)
    end

    opts2 = [encode: fn [:param] -> raise "oops" end, log: log]
    assert_raise RuntimeError, "oops", fn -> P.prepare_execute(pool, %Q{}, [:param], opts2) end
    assert_received :logged

    log = fn entry ->
      assert %DBConnection.LogEntry{
               call: :prepare_execute,
               query: %Q{},
               params: [:param],
               result: {:error, err}
             } = entry

      assert %DBConnection.ConnectionError{
               message: "an exception was raised: ** (RuntimeError) oops" <> _
             } = err

      assert is_integer(entry.pool_time)
      assert entry.pool_time >= 0
      assert is_integer(entry.connection_time)
      assert entry.connection_time >= 0
      assert is_integer(entry.decode_time)
      assert entry.decode_time >= 0
      send(parent, :logged)
    end

    opts3 = [decode: fn %R{} -> raise "oops" end, log: log]
    assert_raise RuntimeError, "oops", fn -> P.prepare_execute(pool, %Q{}, [:param], opts3) end
    assert_received :logged

    assert [
             connect: [_],
             handle_prepare: [%Q{}, _, :state],
             handle_close: [%Q{}, _, :new_state],
             handle_prepare: [%Q{}, _, :newer_state],
             handle_execute: [%Q{}, [:param], _, :newest_state]
           ] = A.record(agent)
  end

  test "prepare_execute logs execute raise" do
    stack = [
      fn opts ->
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, %Q{}, :new_state},
      fn _, _, _, _ ->
        raise "oops"
      end,
      :ok,
      {:ok, :state2}
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    _ = Process.flag(:trap_exit, true)
    {:ok, pool} = P.start_link(opts)

    log = fn entry ->
      assert %DBConnection.LogEntry{
               call: :prepare_execute,
               query: %Q{},
               params: [:param],
               result: {:error, err}
             } = entry

      assert %DBConnection.ConnectionError{
               message: "an exception was raised: ** (RuntimeError) oops" <> _
             } = err

      assert is_integer(entry.pool_time)
      assert entry.pool_time >= 0
      assert is_integer(entry.connection_time)
      assert entry.connection_time >= 0
      assert is_nil(entry.decode_time)
      send(parent, :logged)
    end

    assert_raise RuntimeError, "oops", fn -> P.prepare_execute(pool, %Q{}, [:param], log: log) end
    assert_received :logged
    assert_receive {:EXIT, _, {%DBConnection.ConnectionError{}, [_ | _]}}

    assert [
             {:connect, [_]},
             {:handle_prepare, [%Q{}, _, :state]},
             {:handle_execute, [%Q{}, [:param], _, :new_state]},
             {:disconnect, _} | _
           ] = A.record(agent)
  end

  test "prepare_execute! error raises error" do
    err = RuntimeError.exception("oops")

    stack = [
      {:ok, :state},
      {:error, err, :new_state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_raise RuntimeError, "oops", fn -> P.prepare_execute!(pool, %Q{}, [:param]) end

    assert [
             connect: [_],
             handle_prepare: [%Q{}, _, :state]
           ] = A.record(agent)
  end

  test "prepare_execute execute disconnect returns error" do
    err = RuntimeError.exception("oops")

    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state},
      {:disconnect, err, :newer_state},
      :ok,
      fn opts ->
        send(opts[:parent], :reconnected)
        {:ok, :state}
      end
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.prepare_execute(pool, %Q{}, [:param]) == {:error, err}

    assert_receive :reconnected

    assert [
             connect: [opts2],
             handle_prepare: [%Q{}, _, :state],
             handle_execute: [%Q{}, [:param], _, :new_state],
             disconnect: [^err, :newer_state],
             connect: [opts2]
           ] = A.record(agent)
  end

  test "prepare_execute describe or encode raises and closes query" do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state},
      {:ok, %R{}, :newer_state},
      {:ok, %Q{}, :newest_state},
      {:ok, %R{}, :state2}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    describe = fn %Q{} -> raise "oops" end

    assert_raise RuntimeError, "oops", fn ->
      P.prepare_execute(pool, %Q{}, [:param], describe: describe)
    end

    encode = fn [:param] -> raise "oops" end

    assert_raise RuntimeError, "oops", fn ->
      P.prepare_execute(pool, %Q{}, [:param], encode: encode)
    end

    assert [
             connect: [_],
             handle_prepare: [%Q{}, _, :state],
             handle_close: [%Q{}, _, :new_state],
             handle_prepare: [%Q{}, _, :newer_state],
             handle_close: [%Q{}, _, :newest_state]
           ] = A.record(agent)
  end
end
