defmodule RunTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestResult, as: R

  test "run execute returns result" do
    stack = [
      {:ok, :state},
      {:ok, %R{}, :new_state},
      {:ok, %R{}, :newer_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.run(pool, fn(conn) ->
      assert P.execute(conn, %Q{}, [:param]) == {:ok, %R{}}
      assert P.execute(conn, %Q{}, [:param], [key: :value]) == {:ok, %R{}}
      :hi
    end) == :hi

    assert [
      connect: [_],
      handle_execute: [%Q{}, [:param], _, :state],
      handle_execute: [%Q{}, [:param], [{:key, :value} | _], :new_state]
      ] = A.record(agent)
  end

  test "run execute logs result" do
    stack = [
      {:ok, :state},
      {:ok, %R{}, :new_state}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    log = fn(entry) ->
      assert %DBConnection.LogEntry{call: :execute, query: %Q{},
                                    params: [:param],
                                    result: {:ok, %R{}}} = entry
      assert is_nil(entry.pool_time)
      assert is_integer(entry.connection_time)
      assert entry.connection_time >= 0
      assert is_integer(entry.decode_time)
      assert entry.decode_time >= 0
      send(parent, :logged)
    end
    assert P.run(pool, fn(conn) ->
      assert P.execute(conn, %Q{}, [:param], [log: log]) == {:ok, %R{}}
      :hi
    end) == :hi

    assert_received :logged
    assert [
      connect: [_],
      handle_execute: [%Q{}, [:param], _, :state]
      ] = A.record(agent)
  end

  test "run execute error returns error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:error, err, :newer_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.run(pool, fn(conn) ->
      assert P.execute(conn, %Q{}, [:param]) == {:error, err}
      :hi
    end) == :hi

    assert [
      connect: [_],
      handle_execute: [%Q{}, [:param], _, :state]
      ] = A.record(agent)
  end

  test "run execute! error raises error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:error, err, :new_state},
      {:ok,  %R{}, :newer_state},
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.run(pool, fn(conn) ->
      assert_raise RuntimeError, "oops",
        fn() -> P.execute!(conn, %Q{}, [:param]) end

      assert P.execute(conn, %Q{}, [:param]) == {:ok, %R{}}
      :hi
    end) == :hi

    assert [
      connect: [_],
      handle_execute: [%Q{}, [:param], _, :state],
      handle_execute: [%Q{}, [:param], _, :new_state],
      ] = A.record(agent)
  end

  test "run execute disconnect returns error" do
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

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.run(pool, fn(conn) ->
      assert P.execute(conn, %Q{}, [:param]) == {:error, err}

      assert_raise DBConnection.ConnectionError, "connection is closed",
        fn() -> P.execute!(conn, %Q{}, [:param]) end

      :closed
    end) == :closed

    assert_receive :reconnected

    assert [
      connect: [opts2],
      handle_execute: [%Q{}, [:param], _, :state],
      disconnect: [^err, :new_state],
      connect: [opts2]] = A.record(agent)
  end

  test "run execute bad return raises DBConnection.ConnectionError and stops" do
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

    assert P.run(pool, fn(conn) ->
      assert_raise DBConnection.ConnectionError, "bad return value: :oops",
        fn() -> P.execute(conn, %Q{}, [:param]) end

      assert_raise DBConnection.ConnectionError, "connection is closed",
        fn() -> P.execute!(conn, %Q{}, [:param]) end

      :closed
    end) == :closed

    prefix = "client #{inspect self()} stopped: " <>
      "** (DBConnection.ConnectionError) bad return value: :oops"
    len = byte_size(prefix)
    assert_receive {:EXIT, ^conn,
      {%DBConnection.ConnectionError{message: <<^prefix::binary-size(len), _::binary>>},
        [_|_]}}

    assert [
      {:connect, _},
      {:handle_execute, [%Q{}, [:param], _, :state]} | _] = A.record(agent)
  end

  test "run execute raise raises and stops connection" do
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

    assert P.run(pool, fn(conn) ->
      assert_raise RuntimeError, "oops",
        fn() -> P.execute(conn, %Q{}, [:param]) end

      assert_raise DBConnection.ConnectionError, "connection is closed",
        fn() -> P.execute!(conn, %Q{}, [:param]) end

      :closed
    end) == :closed


    prefix = "client #{inspect self()} stopped: ** (RuntimeError) oops"
    len = byte_size(prefix)
    assert_receive {:EXIT, ^conn,
      {%DBConnection.ConnectionError{message: <<^prefix::binary-size(len), _::binary>>},
       [_|_]}}

    assert [
      {:connect, _},
      {:handle_execute, [%Q{}, [:param], _, :state]}| _] = A.record(agent)
  end
end
