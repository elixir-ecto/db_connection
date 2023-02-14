defmodule TransactionExecuteTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestResult, as: R

  test "execute returns result" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %Q{}, %R{}, :newer_state},
      {:ok, %Q{}, %R{}, :newest_state},
      {:ok, :committed, :newest_state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn conn ->
             assert P.execute(conn, %Q{}, [:param]) == {:ok, %Q{}, %R{}}
             assert P.execute(conn, %Q{}, [:param], key: :value) == {:ok, %Q{}, %R{}}
             :hi
           end) == {:ok, :hi}

    assert [
             connect: [_],
             handle_begin: [_, :state],
             handle_execute: [%Q{}, [:param], _, :new_state],
             handle_execute: [%Q{}, [:param], [{:key, :value} | _], :newer_state],
             handle_commit: [_, :newest_state]
           ] = A.record(agent)
  end

  test "execute logs result" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %Q{}, %R{}, :newer_state},
      {:ok, :committed, :newest_state}
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

      assert is_nil(entry.pool_time)
      assert is_integer(entry.connection_time)
      assert entry.connection_time >= 0
      assert is_integer(entry.decode_time)
      assert entry.decode_time >= 0
      send(parent, :logged)
    end

    assert P.transaction(pool, fn conn ->
             assert P.execute(conn, %Q{}, [:param], log: log) == {:ok, %Q{}, %R{}}
             :hi
           end) == {:ok, :hi}

    assert_received :logged

    assert [
             connect: [_],
             handle_begin: [_, :state],
             handle_execute: [%Q{}, [:param], _, :new_state],
             handle_commit: [_, :newer_state]
           ] = A.record(agent)
  end

  test "execute error returns error" do
    err = RuntimeError.exception("oops")

    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:error, err, :newer_state},
      {:ok, :committed, :newest_state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn conn ->
             assert P.execute(conn, %Q{}, [:param]) == {:error, err}
             :hi
           end) == {:ok, :hi}

    assert [
             connect: [_],
             handle_begin: [_, :state],
             handle_execute: [%Q{}, [:param], _, :new_state],
             handle_commit: [_, :newer_state]
           ] = A.record(agent)
  end

  test "execute! error raises error" do
    err = RuntimeError.exception("oops")

    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:error, err, :newer_state},
      {:ok, %Q{}, %R{}, :newest_state},
      {:ok, :committed, :newest_state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn conn ->
             assert_raise RuntimeError, "oops", fn -> P.execute!(conn, %Q{}, [:param]) end

             assert P.execute(conn, %Q{}, [:param]) == {:ok, %Q{}, %R{}}
             :hi
           end) == {:ok, :hi}

    assert [
             connect: [_],
             handle_begin: [_, :state],
             handle_execute: [%Q{}, [:param], _, :new_state],
             handle_execute: [%Q{}, [:param], _, :newer_state],
             handle_commit: [_, :newest_state]
           ] = A.record(agent)
  end

  test "execute disconnect returns error" do
    err = RuntimeError.exception("oops")

    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
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

    assert P.transaction(pool, fn conn ->
             assert P.execute(conn, %Q{}, [:param]) == {:error, err}

             assert_raise DBConnection.ConnectionError,
                          "connection is closed because of an error, disconnect or timeout",
                          fn -> P.execute!(conn, %Q{}, [:param]) end

             :closed
           end) == {:error, :rollback}

    assert_receive :reconnected

    assert [
             connect: [opts2],
             handle_begin: [_, :state],
             handle_execute: [%Q{}, [:param], _, :new_state],
             disconnect: [^err, :newer_state],
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
      {:ok, :began, :new_state},
      :oops,
      {:ok, :state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}

    Process.flag(:trap_exit, true)

    assert P.transaction(pool, fn conn ->
             assert_raise DBConnection.ConnectionError,
                          "bad return value: :oops",
                          fn -> P.execute(conn, %Q{}, [:param]) end

             assert_raise DBConnection.ConnectionError,
                          "connection is closed because of an error, disconnect or timeout",
                          fn -> P.execute!(conn, %Q{}, [:param]) end

             :closed
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
             {:handle_execute, [%Q{}, [:param], _, :new_state]} | _
           ] = A.record(agent)
  end

  test "execute raise raises and stops connection" do
    stack = [
      fn opts ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, :began, :new_state},
      fn _, _, _, _ ->
        raise "oops"
      end,
      {:ok, :state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}

    Process.flag(:trap_exit, true)

    assert P.transaction(pool, fn conn ->
             assert_raise RuntimeError,
                          "oops",
                          fn -> P.execute(conn, %Q{}, [:param]) end

             assert_raise DBConnection.ConnectionError,
                          "connection is closed because of an error, disconnect or timeout",
                          fn -> P.execute!(conn, %Q{}, [:param]) end

             :closed
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
             {:handle_execute, [%Q{}, [:param], _, :new_state]} | _
           ] = A.record(agent)
  end

  test "execute raises after inner transaction rollback" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      :ok,
      fn opts ->
        send(opts[:parent], :reconnected)
        {:ok, :state}
      end
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn conn ->
             assert P.transaction(conn, fn conn2 ->
                      P.rollback(conn2, :oops)
                    end) == {:error, :oops}

             assert_raise DBConnection.ConnectionError,
                          "transaction rolling back",
                          fn -> P.execute!(conn, %Q{}, [:param]) end
           end) == {:error, :rollback}

    assert_receive :reconnected

    assert [
             connect: [_],
             handle_begin: [_, :state],
             disconnect: _,
             connect: _
           ] = A.record(agent)
  end

  test "transaction logs rollback if closed" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      :oops,
      {:ok, :state2}
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    Process.flag(:trap_exit, true)
    {:ok, pool} = P.start_link(opts)

    log = &send(parent, &1)

    P.transaction(
      pool,
      fn conn ->
        assert_received %DBConnection.LogEntry{call: :begin, query: :begin}

        assert_raise DBConnection.ConnectionError,
                     fn -> P.execute(conn, %Q{}, [:param]) end
      end,
      log: log
    )

    assert_received %DBConnection.LogEntry{call: :rollback, query: :rollback}

    assert [
             {:connect, [_]},
             {:handle_begin, [_, :state]},
             {:handle_execute, [%Q{}, [:param], _, :new_state]}
             | _
           ] = A.record(agent)
  end
end
