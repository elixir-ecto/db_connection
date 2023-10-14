defmodule TransactionTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q

  test "transaction returns result" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, :committed, :newer_state},
      {:ok, :began, :newest_state},
      {:ok, :committed, :newest_state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn conn ->
             assert %DBConnection{} = conn
             :result
           end) == {:ok, :result}

    assert P.transaction(
             pool,
             fn conn ->
               assert %DBConnection{} = conn
               :result
             end,
             key: :value
           ) == {:ok, :result}

    assert [
             connect: [_],
             handle_begin: [_, :state],
             handle_commit: [_, :new_state],
             handle_begin: [[{:key, :value} | _], :newer_state],
             handle_commit: [[{:key, :value} | _], :newest_state]
           ] = A.record(agent)
  end

  test "transaction logs begin/commit/rollback" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, :committed, :newer_state},
      {:ok, :began, :newest_state},
      {:ok, :rolledback, :newest_state}
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    log = &send(parent, &1)

    assert P.transaction(
             pool,
             fn _ ->
               assert_received %DBConnection.LogEntry{call: :begin} = entry
               assert %{query: :begin, params: nil, result: {:ok, _, :began}} = entry
               assert is_integer(entry.pool_time)
               assert entry.pool_time >= 0
               assert is_integer(entry.connection_time)
               assert entry.connection_time >= 0
               assert is_nil(entry.decode_time)

               :result
             end,
             log: log
           ) == {:ok, :result}

    assert_received %DBConnection.LogEntry{call: :commit} = entry
    assert %{query: :commit, params: nil, result: {:ok, :committed}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert P.transaction(
             pool,
             fn conn ->
               assert_received %DBConnection.LogEntry{call: :begin}
               P.rollback(conn, :result)
             end,
             log: log
           ) == {:error, :result}

    assert_received %DBConnection.LogEntry{call: :rollback} = entry
    assert %{query: :rollback, params: nil, result: {:ok, :rolledback}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert [
             connect: [_],
             handle_begin: [_, :state],
             handle_commit: [_, :new_state],
             handle_begin: [_, :newer_state],
             handle_rollback: [_, :newest_state]
           ] = A.record(agent)
  end

  test "transaction rollback returns error" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, :rolledback, :newer_state},
      {:ok, :began, :newest_state},
      {:ok, :rolledback, :newest_state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn conn ->
             P.rollback(conn, :oops)
           end) == {:error, :oops}

    assert P.transaction(pool, fn _ -> :result end) == {:ok, :result}

    assert [
             connect: [_],
             handle_begin: [_, :state],
             handle_rollback: [_, :new_state],
             handle_begin: [_, :newer_state],
             handle_commit: [_, :newest_state]
           ] = A.record(agent)
  end

  test "inner transaction rollback returns error on other transactions" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, :rolledback, :newer_state},
      {:ok, :began, :newest_state},
      {:ok, :comittted, :newest_state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn conn ->
             assert P.transaction(conn, fn conn2 ->
                      P.rollback(conn2, :oops)
                    end) == {:error, :oops}

             assert P.transaction(conn, fn _ -> nil end) == {:error, :rollback}
           end) == {:error, :rollback}

    assert P.transaction(pool, fn _ -> :result end) == {:ok, :result}

    assert [
             connect: [_],
             handle_begin: [_, :state],
             handle_rollback: [_, :new_state],
             handle_begin: [_, :newer_state],
             handle_commit: [_, :newest_state]
           ] = A.record(agent)
  end

  test "outer transaction rolls back after inner rollback" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, :rolledback, :newer_state},
      {:ok, :began, :newest_state},
      {:ok, :committed, :newest_state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn conn ->
             assert P.transaction(conn, fn conn2 ->
                      P.rollback(conn2, :oops)
                    end) == {:error, :oops}

             P.rollback(conn, :oops2)
           end) == {:error, :oops2}

    assert P.transaction(pool, fn _ -> :result end) == {:ok, :result}

    assert [
             connect: [_],
             handle_begin: [_, :state],
             handle_rollback: [_, :new_state],
             handle_begin: [_, :newer_state],
             handle_commit: [_, :newest_state]
           ] = A.record(agent)
  end

  test "inner transaction raise returns error on other transactions" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, :rolledback, :newer_state},
      {:ok, :began, :newest_state},
      {:ok, :committed, :newest_state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn conn ->
             assert_raise RuntimeError, "oops", fn ->
               P.transaction(conn, fn _ -> raise "oops" end)
             end

             assert P.transaction(conn, fn _ -> nil end) == {:error, :rollback}
           end) == {:error, :rollback}

    assert P.transaction(pool, fn _ -> :result end) == {:ok, :result}

    assert [
             connect: [_],
             handle_begin: [_, :state],
             handle_rollback: [_, :new_state],
             handle_begin: [_, :newer_state],
             handle_commit: [_, :newest_state]
           ] = A.record(agent)
  end

  test "transaction and transaction returns result" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, :committed, :newer_state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn conn ->
             assert P.transaction(conn, fn conn2 ->
                      assert %DBConnection{} = conn2
                      assert conn == conn2
                      :result
                    end) == {:ok, :result}

             :result
           end) == {:ok, :result}

    assert [
             connect: [_],
             handle_begin: [_, :state],
             handle_commit: [_, :new_state]
           ] = A.record(agent)
  end

  test "transaction and run returns result" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, :committed, :newer_state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn conn ->
             assert P.run(conn, fn conn2 ->
                      assert %DBConnection{} = conn2
                      assert conn == conn2
                      :result
                    end) == :result

             :result
           end) == {:ok, :result}

    assert [
             connect: [_],
             handle_begin: [_, :state],
             handle_commit: [_, :new_state]
           ] = A.record(agent)
  end

  test "transaction begin error raises error" do
    err = RuntimeError.exception("oops")

    stack = [
      {:ok, :state},
      {:error, err, :new_state},
      {:ok, :began, :newer_state},
      {:ok, :committed, :newest_state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_raise RuntimeError, "oops", fn ->
      P.transaction(pool, fn _ -> flunk("transaction ran") end)
    end

    assert P.transaction(pool, fn _ -> :result end) == {:ok, :result}

    assert [
             connect: [_],
             handle_begin: [_, :state],
             handle_begin: [_, :new_state],
             handle_commit: [_, :newer_state]
           ] = A.record(agent)
  end

  test "transaction logs begin error" do
    err = RuntimeError.exception("oops")

    stack = [
      {:ok, :state},
      {:error, err, :new_state}
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    log = &send(parent, &1)

    assert_raise RuntimeError, "oops", fn ->
      P.transaction(pool, fn _ -> flunk("transaction ran") end, log: log)
    end

    assert_received %DBConnection.LogEntry{call: :begin} = entry
    assert %{query: :begin, params: nil, result: {:error, ^err}} = entry
    assert is_integer(entry.pool_time)
    assert entry.pool_time >= 0
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert [
             connect: [_],
             handle_begin: [_, :state]
           ] = A.record(agent)
  end

  test "transaction logs begin raise" do
    stack = [
      fn opts ->
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      fn _, _ ->
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

    log = &send(parent, &1)

    assert_raise RuntimeError, "oops", fn ->
      P.transaction(pool, fn _ -> flunk("transaction ran") end, log: log)
    end

    assert_received %DBConnection.LogEntry{call: :begin} = entry
    assert %{query: :begin, params: nil, result: {:error, err}} = entry

    assert %DBConnection.ConnectionError{
             message: "an exception was raised: ** (RuntimeError) oops" <> _
           } = err

    assert is_integer(entry.pool_time)
    assert entry.pool_time >= 0
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)
    assert_receive {:EXIT, _, {%DBConnection.ConnectionError{}, [_ | _]}}

    assert [
             {:connect, [_]},
             {:handle_begin, [_, :state]},
             {:disconnect, _} | _
           ] = A.record(agent)
  end

  test "transaction begin disconnect raises error" do
    err = RuntimeError.exception("oops")

    stack = [
      {:ok, :state},
      {:disconnect, err, :new_state},
      :ok,
      fn opts ->
        send(opts[:parent], :reconnected)
        {:ok, :newest_state}
      end
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_raise RuntimeError, "oops", fn ->
      P.transaction(pool, fn _ -> flunk("transaction ran") end)
    end

    assert_receive :reconnected

    assert [
             connect: [_],
             handle_begin: [_, :state],
             disconnect: [_, :new_state],
             connect: [_]
           ] = A.record(agent)
  end

  test "transaction begin bad return raises and stops connection" do
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
      P.transaction(pool, fn _ -> flunk("transaction ran") end)
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
             {:handle_begin, [_, :state]},
             {:disconnect, _} | _
           ] = A.record(agent)
  end

  test "transaction begin raise raises and stops connection" do
    stack = [
      fn opts ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      fn _, _ ->
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

    assert_raise RuntimeError, "oops", fn ->
      P.transaction(pool, fn _ -> flunk("transaction ran") end)
    end

    prefix = "client #{inspect(self())} stopped: ** (RuntimeError) oops"
    len = byte_size(prefix)

    assert_receive {:EXIT, ^conn,
                    {%DBConnection.ConnectionError{
                       message: <<^prefix::binary-size(len), _::binary>>
                     }, [_ | _]}}

    assert [
             {:connect, _},
             {:handle_begin, [_, :state]},
             {:disconnect, _} | _
           ] = A.record(agent)
  end

  test "transaction commit error raises error" do
    err = RuntimeError.exception("oops")

    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:error, err, :newer_state},
      {:ok, :began, :newest_state},
      {:ok, :committed, :newest_state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_raise RuntimeError, "oops", fn -> P.transaction(pool, fn _ -> :ok end) end

    assert P.transaction(pool, fn _ -> :result end) == {:ok, :result}

    assert [
             connect: [_],
             handle_begin: [_, :state],
             handle_commit: [_, :new_state],
             handle_begin: [_, :newer_state],
             handle_commit: [_, :newest_state]
           ] = A.record(agent)
  end

  test "transaction logs commit error" do
    err = RuntimeError.exception("oops")

    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:error, err, :newer_state}
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    log = &send(parent, &1)

    assert_raise RuntimeError, "oops", fn ->
      P.transaction(
        pool,
        fn _ ->
          assert_received %DBConnection.LogEntry{call: :begin}
        end,
        log: log
      )
    end

    assert_received %DBConnection.LogEntry{call: :commit} = entry
    assert %{query: :commit, params: nil, result: {:error, ^err}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert [
             connect: [_],
             handle_begin: [_, :state],
             handle_commit: [_, :new_state]
           ] = A.record(agent)
  end

  test "transaction commit disconnect raises error" do
    err = RuntimeError.exception("oops")

    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:disconnect, err, :newer_state},
      :ok,
      fn opts ->
        send(opts[:parent], :reconnected)
        {:ok, :newest_state}
      end
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_raise RuntimeError, "oops", fn -> P.transaction(pool, fn _ -> :result end) end

    assert_receive :reconnected

    assert [
             connect: [_],
             handle_begin: [_, :state],
             handle_commit: [_, :new_state],
             disconnect: [_, :newer_state],
             connect: [_]
           ] = A.record(agent)
  end

  test "transaction commit bad return raises and stops connection" do
    stack = [
      fn opts ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, :began, :new_state},
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
      P.transaction(pool, fn _ -> :result end)
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
             {:handle_begin, [_, :state]},
             {:handle_commit, [_, :new_state]},
             {:disconnect, _} | _
           ] = A.record(agent)
  end

  test "transaction commit raise raises and stops connection" do
    stack = [
      fn opts ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, :began, :new_state},
      fn _, _ ->
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
    assert_raise RuntimeError, "oops", fn -> P.transaction(pool, fn _ -> :result end) end

    prefix = "client #{inspect(self())} stopped: ** (RuntimeError) oops"
    len = byte_size(prefix)

    assert_receive {:EXIT, ^conn,
                    {%DBConnection.ConnectionError{
                       message: <<^prefix::binary-size(len), _::binary>>
                     }, [_ | _]}}

    assert [
             {:connect, _},
             {:handle_begin, [_, :state]},
             {:handle_commit, [_, :new_state]},
             {:disconnect, _} | _
           ] = A.record(agent)
  end

  test "transaction logs commit raise" do
    stack = [
      fn opts ->
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, :began, :new_state},
      fn _, _ ->
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

    log = &send(parent, &1)

    assert_raise RuntimeError, "oops", fn ->
      P.transaction(pool, fn _ -> :ok end, log: log)
    end

    assert_received %DBConnection.LogEntry{call: :begin} = entry
    assert %{query: :begin, params: nil, result: {:ok, _, :began}} = entry

    assert_received %DBConnection.LogEntry{call: :commit} = entry
    assert %{query: :commit, params: nil, result: {:error, err}} = entry

    assert %DBConnection.ConnectionError{
             message: "an exception was raised: ** (RuntimeError) oops" <> _
           } = err

    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)
    assert_receive {:EXIT, _, {%DBConnection.ConnectionError{}, [_ | _]}}

    assert [
             {:connect, [_]},
             {:handle_begin, [_, :state]},
             {:handle_commit, [_, :new_state]},
             {:disconnect, _} | _
           ] = A.record(agent)
  end

  test "transaction rollback error raises error" do
    err = RuntimeError.exception("oops")

    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:error, err, :newer_state},
      {:ok, :began, :newest_state},
      {:ok, :rolledback, :newest_state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_raise RuntimeError, "oops", fn -> P.transaction(pool, &P.rollback(&1, :oops)) end

    assert P.transaction(pool, fn _ -> :result end) == {:ok, :result}

    assert [
             connect: [_],
             handle_begin: [_, :state],
             handle_rollback: [_, :new_state],
             handle_begin: [_, :newer_state],
             handle_commit: [_, :newest_state]
           ] = A.record(agent)
  end

  test "transaction fun raise rolls back and re-raises" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, :rolledback, :newer_state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_raise RuntimeError, "oops", fn -> P.transaction(pool, fn _ -> raise "oops" end) end

    assert [
             connect: [_],
             handle_begin: [_, :state],
             handle_rollback: [_, :new_state]
           ] = A.record(agent)
  end

  test "transaction logs rollback raise" do
    stack = [
      fn opts ->
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, :began, :new_state},
      fn _, _ ->
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

    log = &send(parent, &1)

    assert_raise RuntimeError, "oops", fn ->
      P.transaction(pool, &P.rollback(&1, :oops), log: log)
    end

    assert_received %DBConnection.LogEntry{call: :begin} = entry
    assert %{query: :begin, params: nil, result: {:ok, _, :began}} = entry

    assert_received %DBConnection.LogEntry{call: :rollback} = entry
    assert %{query: :rollback, params: nil, result: {:error, err}} = entry

    assert %DBConnection.ConnectionError{
             message: "an exception was raised: ** (RuntimeError) oops" <> _
           } = err

    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)
    assert_receive {:EXIT, _, {%DBConnection.ConnectionError{}, [_ | _]}}

    assert [
             {:connect, [_]},
             {:handle_begin, [_, :state]},
             {:handle_rollback, [_, :new_state]},
             {:disconnect, _} | _
           ] = A.record(agent)
  end

  test "transaction logs on fun raise" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, :rolledback, :newer_state}
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    log = &send(parent, &1)

    assert_raise RuntimeError, "oops", fn ->
      P.transaction(
        pool,
        fn _ ->
          assert_received %DBConnection.LogEntry{call: :begin, query: :begin}
          raise "oops"
        end,
        log: log
      )
    end

    assert_received %DBConnection.LogEntry{call: :rollback, query: :rollback}

    assert [
             connect: [_],
             handle_begin: [_, :state],
             handle_rollback: [_, :new_state]
           ] = A.record(agent)
  end

  test "transaction logs begin status errors and disconnects" do
    stack = [
      {:ok, :state},
      {:transaction, :new_state},
      :ok,
      fn opts ->
        send(opts[:parent], :reconnected)
        {:ok, :newer_state}
      end,
      {:error, :newest_state},
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

    log = &send(parent, &1)

    assert P.transaction(pool, fn _ -> flunk("transaction ran") end, log: log) ==
             {:error, :rollback}

    assert_received %DBConnection.LogEntry{call: :begin} = entry
    err = DBConnection.TransactionError.exception(:transaction)
    assert %{query: :begin, params: nil, result: {:error, ^err}} = entry
    assert is_integer(entry.pool_time)
    assert entry.pool_time >= 0
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    refute_received %DBConnection.LogEntry{}

    assert_receive :reconnected

    assert P.transaction(pool, fn _ -> flunk("transaction ran") end, log: log) ==
             {:error, :rollback}

    assert_received %DBConnection.LogEntry{call: :begin} = entry
    err = DBConnection.TransactionError.exception(:error)
    assert %{query: :begin, params: nil, result: {:error, ^err}} = entry
    assert is_integer(entry.pool_time)
    assert entry.pool_time >= 0
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    refute_received %DBConnection.LogEntry{}
    assert_receive :reconnected

    assert [
             connect: [_],
             handle_begin: [_, :state],
             disconnect: [%DBConnection.TransactionError{status: :transaction}, :new_state],
             connect: [_],
             handle_begin: [_, :newer_state],
             disconnect: [%DBConnection.TransactionError{status: :error}, :newest_state],
             connect: [_]
           ] = A.record(agent)
  end

  test "transaction logs commit status errors and disconnects" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:idle, :newer_state},
      :ok,
      fn opts ->
        send(opts[:parent], :reconnected)
        {:ok, :newest_state}
      end,
      {:ok, :began, :state2},
      {:error, :new_state2},
      {:ok, :rolledback, :newer_state2}
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    log = &send(parent, &1)

    assert P.transaction(
             pool,
             fn _ -> assert_receive %DBConnection.LogEntry{call: :begin} end,
             log: log
           ) == {:error, :rollback}

    assert_received %DBConnection.LogEntry{call: :commit} = entry
    err = DBConnection.TransactionError.exception(:idle)
    assert %{query: :commit, params: nil, result: {:error, ^err}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    refute_received %DBConnection.LogEntry{}
    assert_receive :reconnected

    assert P.transaction(
             pool,
             fn _ -> assert_receive %DBConnection.LogEntry{call: :begin} end,
             log: log
           ) == {:error, :rollback}

    assert_received %DBConnection.LogEntry{call: :commit} = entry
    assert %{query: :rollback, params: nil, result: {:ok, :rolledback}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    refute_received %DBConnection.LogEntry{}

    assert [
             connect: [_],
             handle_begin: [_, :state],
             handle_commit: [_, :new_state],
             disconnect: [%DBConnection.TransactionError{status: :idle}, :newer_state],
             connect: [_],
             handle_begin: [_, :newest_state],
             handle_commit: [_, :state2],
             handle_rollback: [_, :new_state2]
           ] = A.record(agent)
  end

  test "transaction logs rollback status error and disconnects" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:idle, :newer_state},
      :ok,
      fn opts ->
        send(opts[:parent], :reconnected)
        {:ok, :newest_state}
      end
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    log = &send(parent, &1)

    assert P.transaction(
             pool,
             fn conn ->
               assert_receive %DBConnection.LogEntry{call: :begin}
               P.rollback(conn, :oops)
             end,
             log: log
           ) == {:error, :oops}

    assert_received %DBConnection.LogEntry{call: :rollback} = entry
    err = DBConnection.TransactionError.exception(:idle)
    assert %{query: :rollback, params: nil, result: {:error, ^err}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    refute_received %DBConnection.LogEntry{}
    assert_receive :reconnected

    assert [
             connect: [_],
             handle_begin: [_, :state],
             handle_rollback: [_, :new_state],
             disconnect: [%DBConnection.TransactionError{status: :idle}, :newer_state],
             connect: [_]
           ] = A.record(agent)
  end

  test "status returns result" do
    err = RuntimeError.exception("oops")

    stack = [
      {:ok, :state},
      {:idle, :new_state},
      {:transaction, :newer_state},
      {:error, :newest_state},
      {:disconnect, err, :state2},
      :ok,
      fn opts ->
        send(opts[:parent], :reconnected)
        {:ok, :new_state2}
      end
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.status(pool, opts) == :idle
    assert P.status(pool, opts) == :transaction
    assert P.status(pool, [queue: false] ++ opts) == :error
    assert P.status(pool, opts) == :error

    assert_receive :reconnected

    assert [
             connect: [_],
             handle_status: [_, :state],
             handle_status: [_, :new_state],
             handle_status: [_, :newer_state],
             handle_status: [_, :newest_state],
             disconnect: [^err, :state2],
             connect: [_]
           ] = A.record(agent)
  end

  test "status returns result on successful run" do
    stack = [
      {:ok, :state},
      {:transaction, :new_state},
      {:transaction, :newest_state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.run(pool, fn _ -> :ok end, opts) == :ok

    assert [
             connect: [_],
             handle_status: [_, :state],
             handle_status: [_, :new_state]
           ] = A.record(agent)
  end

  test "status returns result on disconnect run" do
    err = RuntimeError.exception("oops")

    stack = [
      {:ok, :state},
      {:error, :newest_state},
      {:disconnect, err, :state2},
      :ok,
      fn opts ->
        send(opts[:parent], :reconnected)
        {:ok, :new_state2}
      end
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.run(
             pool,
             fn conn ->
               assert P.status(pool, [queue: false] ++ opts) == :error
               assert P.status(conn, opts)
             end,
             opts
           )

    assert_receive :reconnected

    assert [
             connect: [_],
             handle_status: [_, :state],
             handle_status: [_, :newest_state],
             disconnect: [^err, :state2],
             connect: [_]
           ] = A.record(agent)
  end

  test "status errors on unmatched run" do
    stack = [
      {:ok, :state},
      {:idle, :new_state},
      {:transaction, :newest_state},
      :ok,
      fn opts ->
        send(opts[:parent], :reconnected)
        {:ok, :last_state}
      end
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_raise DBConnection.ConnectionError,
                 "connection was checked out with status :idle but it was checked in with status :transaction",
                 fn ->
                   P.run(pool, fn _ -> :ok end)
                 end

    assert_receive :reconnected

    assert [
             connect: [_],
             handle_status: [_, :state],
             handle_status: [_, :new_state],
             disconnect: [_, :newest_state],
             connect: _
           ] = A.record(agent)
  end

  test "run inside transaction" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, :committed, :newer_state},
      {:ok, :began, :newest_state},
      {:ok, :committed, :newest_state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn conn ->
             assert %DBConnection{} = conn

             assert P.run(conn, fn conn ->
                      assert %DBConnection{} = conn
                      :result
                    end)
           end) == {:ok, :result}

    assert [
             connect: [_],
             handle_begin: [_, :state],
             handle_commit: [_, :new_state]
           ] = A.record(agent)
  end

  test "transaction inside run" do
    stack = [
      {:ok, :state},
      {:idle, :new_state},
      {:ok, :began, :newer_state},
      {:ok, :committed, :newest_state},
      {:idle, :newest_state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.run(pool, fn conn ->
             assert %DBConnection{} = conn

             assert P.transaction(conn, fn conn ->
                      assert %DBConnection{} = conn
                      :result
                    end)
           end) == {:ok, :result}

    assert [
             connect: [_],
             handle_status: [_, :state],
             handle_begin: [_, :new_state],
             handle_commit: [_, :newer_state],
             handle_status: [_, :newest_state]
           ] = A.record(agent)
  end

  test "log query from handle_begin" do
    stack = [
      {:ok, :state},
      {:ok, %Q{statement: "custom begin"}, :begin, :new_state},
      {:ok, :committed, :newest_state}
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    log = &send(parent, &1)

    assert P.transaction(pool, fn _ -> :result end, log: log) == {:ok, :result}

    assert_received %DBConnection.LogEntry{call: :begin} = entry
    assert %{query: "custom begin"} = entry

    assert [
             connect: [_],
             handle_begin: [_, :state],
             handle_commit: [_, :new_state]
           ] = A.record(agent)
  end

  test "log query from handle_begin: transaction inside run" do
    stack = [
      {:ok, :state},
      {:idle, :new_state},
      {:ok, %Q{statement: "custom begin"}, :begin, :newer_state},
      {:ok, :committed, :newest_state},
      {:idle, :newest_state}
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    log = &send(parent, &1)

    assert P.run(pool, fn conn ->
             P.transaction(conn, fn _ -> :result end, log: log)
           end) == {:ok, :result}

    assert_received %DBConnection.LogEntry{call: :begin} = entry
    assert %{query: "custom begin"} = entry

    assert [
             connect: [_],
             handle_status: [_, :state],
             handle_begin: [_, :new_state],
             handle_commit: [_, :newer_state],
             handle_status: [_, :newest_state]
           ] = A.record(agent)
  end
end
