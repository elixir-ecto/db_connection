defmodule ExecuteTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestResult, as: R

  test "execute returns result" do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, %R{}, :new_state},
      {:ok, %Q{}, %R{}, :new_state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.execute(pool, %Q{}, [:param]) == {:ok, %Q{}, %R{}}
    assert P.execute(pool, %Q{}, [:param], key: :value) == {:ok, %Q{}, %R{}}

    assert [
             connect: [_],
             handle_execute: [%Q{}, [:param], _, :state],
             handle_execute: [%Q{}, [:param], [{:key, :value} | _], :new_state]
           ] = A.record(agent)
  end

  test "execute encodes params and decodes result" do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, %R{}, :new_state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    opts2 = [encode: fn [:param] -> :encoded end, decode: fn %R{} -> :decoded end]
    assert P.execute(pool, %Q{}, [:param], opts2) == {:ok, %Q{}, :decoded}

    assert [
             connect: [_],
             handle_execute: [_, :encoded, _, :state]
           ] = A.record(agent)
  end

  test "execute reprepares query on encode error" do
    stack = [
      {:ok, :state},
      {:ok, %Q{state: :prepared}, :new_state},
      {:ok, %Q{state: :executed}, %R{}, :newer_state}
    ]

    {:ok, agent} = A.start_link(stack)

    parse_once = fn query ->
      if Process.put(DBConnection.Query, true) do
        query
      else
        raise "parsing twice"
      end
    end

    fail_encode_once = fn [:param] ->
      if Process.put(DBConnection.EncodeError, true) do
        :encoded
      else
        raise DBConnection.EncodeError, "oops"
      end
    end

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    opts = [parse: parse_once, encode: fail_encode_once, decode: fn %R{} -> :decoded end]
    assert P.execute(pool, %Q{}, [:param], opts) == {:ok, %Q{state: :executed}, :decoded}

    assert [
             connect: [_],
             handle_prepare: [%Q{state: nil}, _, :state],
             handle_execute: [%Q{state: :prepared}, :encoded, _, :new_state]
           ] = A.record(agent)
  end

  test "execute logs result" do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, %R{}, :new_state}
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    log = fn entry ->
      assert %DBConnection.LogEntry{
               call: :execute,
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

    assert P.execute(pool, %Q{}, [:param], log: log) == {:ok, %Q{}, %R{}}
    assert_received :logged

    assert [
             connect: [_],
             handle_execute: [%Q{}, [:param], _, :state]
           ] = A.record(agent)
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
             handle_execute: [%Q{}, [:param], _, :state]
           ] = A.record(agent)
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

    log = fn entry ->
      assert %DBConnection.LogEntry{
               call: :execute,
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

    assert P.execute(pool, %Q{}, [:param], log: log) == {:error, err}
    assert_received :logged

    assert [
             connect: [_],
             handle_execute: [%Q{}, [:param], _, :state]
           ] = A.record(agent)
  end

  test "execute logs raise" do
    stack = [
      fn opts ->
        Process.link(opts[:parent])
        {:ok, :state}
      end,
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
               call: :execute,
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

    assert_raise RuntimeError, "oops", fn -> P.execute(pool, %Q{}, [:param], log: log) end
    assert_received :logged
    assert_receive {:EXIT, _, {%DBConnection.ConnectionError{}, [_ | _]}}

    assert [
             {:connect, [_]},
             {:handle_execute, [%Q{}, [:param], _, :state]},
             {:disconnect, _} | _
           ] = A.record(agent)
  end

  test "execute logs encode and decode raise" do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, %R{}, :new_state}
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    _ = Process.flag(:trap_exit, true)
    {:ok, pool} = P.start_link(opts)

    log = fn entry ->
      assert %DBConnection.LogEntry{
               call: :execute,
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

    opts2 = [encode: fn [:param] -> raise "oops" end, log: log]
    assert_raise RuntimeError, "oops", fn -> P.execute(pool, %Q{}, [:param], opts2) end
    assert_received :logged

    log = fn entry ->
      assert %DBConnection.LogEntry{
               call: :execute,
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
    assert_raise RuntimeError, "oops", fn -> P.execute(pool, %Q{}, [:param], opts3) end
    assert_received :logged

    assert [
             connect: [_],
             handle_execute: [%Q{}, [:param], _, :state]
           ] = A.record(agent)
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
    assert_raise RuntimeError, "oops", fn -> P.execute!(pool, %Q{}, [:param]) end

    assert [
             connect: [_],
             handle_execute: [%Q{}, [:param], _, :state]
           ] = A.record(agent)
  end

  test "execute disconnect returns error" do
    err = RuntimeError.exception("oops")

    stack = [
      {:ok, :state},
      {:disconnect, err, :new_state},
      :ok,
      fn opts ->
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
             connect: [opts2]
           ] = A.record(agent)
  end

  test "execute bad return raises DBConnection.ConnectionError and stops" do
    stack = [
      fn opts ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      :oops,
      :ok,
      {:ok, :state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}

    Process.flag(:trap_exit, true)

    assert_raise DBConnection.ConnectionError, "bad return value: :oops", fn ->
      P.execute(pool, %Q{}, [:param])
    end

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
             {:handle_execute, [%Q{}, [:param], _, :state]},
             {:disconnect, _} | _
           ] = A.record(agent)
  end

  test "execute raise raises and stops connection" do
    stack = [
      fn opts ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      fn _, _, _, _ ->
        raise "oops"
      end,
      :ok,
      {:ok, :state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}

    Process.flag(:trap_exit, true)
    assert_raise RuntimeError, "oops", fn -> P.execute(pool, %Q{}, [:param]) end

    prefix = "client #{inspect(self())} stopped: ** (RuntimeError) oops"
    len = byte_size(prefix)

    assert_receive {:EXIT, ^conn,
                    {%DBConnection.ConnectionError{
                       message: <<^prefix::binary-size(len), _::binary>>
                     }, [_ | _]}}

    assert [
             {:connect, _},
             {:handle_execute, [%Q{}, [:param], _, :state]},
             {:disconnect, _} | _
           ] = A.record(agent)
  end
end
