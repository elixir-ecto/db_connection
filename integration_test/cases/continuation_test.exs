defmodule ContinuationTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestResult, as: R

  test "transaction commits after stream resource reduced" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %R{}, :newer_state},
      {:ok, :committed, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    start = fn() -> P.checkout_begin(pool, opts) end
    next = fn(conn) ->
      {:ok, res} = P.transaction(conn, fn(conn2) ->
        P.execute!(conn2, %Q{}, [:param], opts)
      end, opts)
      {[res], conn}
    end
    stop = &P.commit_checkin/1

    assert Stream.resource(start, next, stop) |> Enum.take(1) == [%R{}]

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_execute: [%Q{}, [:param], _, :new_state],
      handle_commit: [_, :newer_state]
      ] = A.record(agent)
  end

  test "transaction commits per trigger inside Flow pipeline" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %R{}, :newer_state},
      {:ok, :committed, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert [[:param]]
      |> Flow.from_enumerable()
      |> Flow.partition(stages: 1)
      |> Flow.reduce(fn() -> {[], P.checkout_begin(pool, opts)} end,
                     fn(params, {acc, conn}) ->
                       {:ok, res} = P.transaction(conn, fn(conn2) ->
                         P.execute!(conn2, %Q{}, params, opts)
                       end)
                       {[res | acc], conn}
                     end)
      |> Flow.map_state(fn({acc, conn}) ->
        P.commit_checkin(conn, opts)
        Enum.reverse(acc)
      end)
      |> Enum.to_list() == [%R{}]

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_execute: [%Q{}, [:param], _, :new_state],
      handle_commit: [_, :newer_state]
      ] = A.record(agent)
  end

  test "checkout_begin raises on checkin" do
    stack = [
      fn(opts) ->
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, :began, :new_state},
      {:ok, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    Process.flag(:trap_exit, true)
    {:ok, pool} = P.start_link(opts)

    conn = P.checkout_begin(pool, opts)

    assert_raise RuntimeError, "inside transaction",
      fn() -> P.checkin(conn, opts) end

    assert P.commit_checkin(conn, opts) == {:error, :rollback}

    assert_receive {:EXIT, _, {%DBConnection.ConnectionError{}, [_|_]}}

    assert [
      {:connect, [_]},
      {:handle_begin, [_, :state]} | _] = A.record(agent)
  end

  test "transaction raises on checkin" do
    stack = [
      fn(opts) ->
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, :began, :new_state},
      {:ok, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    Process.flag(:trap_exit, true)
    {:ok, pool} = P.start_link(opts)

    conn = P.checkout_begin(pool, opts)

    assert_raise RuntimeError, "inside transaction",
      fn() -> P.checkin(conn, opts) end

    assert_receive {:EXIT, _, {%DBConnection.ConnectionError{}, [_|_]}}

    assert P.transaction(conn, fn(conn2) ->
      assert_raise RuntimeError, "inside transaction",
        fn() -> P.checkin(conn2, opts) end
      :hello
    end) == {:error, :rollback}

    assert P.commit_checkin(conn, opts) == {:error, :rollback}

    assert [
      {:connect, [_]},
      {:handle_begin, [_, :state]} | _] = A.record(agent)
  end

  test "transaction runs inside transaction" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %R{}, :newer_state},
      {:ok, :commited, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn(conn) ->
      assert P.transaction(conn, &P.execute!(&1, %Q{}, [:param], opts),
      opts) == {:ok, %R{}}
      :hello
    end) == {:ok, :hello}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_execute: [%Q{}, [:param], _, :new_state],
      handle_commit: [_, :newer_state]
      ] = A.record(agent)
  end

  test "transaction rolls back and returns error" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, :rolledback, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    conn = P.checkout_begin(pool, opts)

    assert P.rollback_checkin(conn, :oops, opts) == {:error, :oops}
    assert P.commit_checkin(conn, opts) == {:error, :rollback}
    assert P.rollback_checkin(conn, :oops, opts) == {:error, :oops}
    assert P.transaction(conn, fn(_) ->
      flunk "should not fun"
    end, opts) == {:error, :rollback}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_rollback: [_, :new_state]
      ] = A.record(agent)
  end

  test "transaction runs inside resource_transaction" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %R{}, :newer_state},
      {:ok, :commited, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    conn = P.checkout_begin(pool, opts)

    assert P.transaction(conn, fn(conn2) ->
      assert P.transaction(conn2, fn(conn3) ->
          P.execute!(conn3, %Q{}, [:param], opts)
        end, opts) == {:ok, %R{}}
      :hello
    end) == {:ok, :hello}

    assert P.commit_checkin(conn, opts) == :ok

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_execute: [%Q{}, [:param], _, :new_state],
      handle_commit: [_, :newer_state]
      ] = A.record(agent)
  end

  test "checkout_begin logs error" do
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
    assert_raise RuntimeError, "oops",
      fn() -> P.checkout_begin(pool, [log: log]) end

    assert_received %DBConnection.LogEntry{call: :checkout_begin} = entry
    assert %{query: :begin, params: nil, result: {:error, ^err}} = entry
    assert is_integer(entry.pool_time)
    assert entry.pool_time >= 0
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert [
      connect: [_],
      handle_begin: [ _, :state]] = A.record(agent)
  end

  test "checkout_begin logs raises and rolls back" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, :raise, :new_state},
      {:ok, :rolledback, :newer_state}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    log = fn
      %DBConnection.LogEntry{result: {:ok, :raise}} -> raise err
      entry -> send(parent, entry)
    end

    assert_raise RuntimeError, "oops",
      fn() -> P.checkout_begin(pool, [log: log]) end

    assert_received %DBConnection.LogEntry{call: :checkout_begin} = entry
    assert %{query: :rollback, params: nil, result: {:ok, :rolledback}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert [
      connect: [_],
      handle_begin: [ _, :state],
      handle_rollback: [_, :new_state]
      ] = A.record(agent)
  end

  test "transaction logs on rollback" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, :rolledback, :newer_state}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    conn = P.checkout_begin(pool, opts)

    log = &send(parent, &1)
    assert P.transaction(conn, fn(conn2) ->
      P.rollback(conn2, :oops)
    end, [log: log]) == {:error, :oops}

    assert_received %DBConnection.LogEntry{call: :transaction} = entry
    assert %{query: :rollback, params: nil, result: {:ok, :rolledback}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert [
      connect: [_],
      handle_begin: [ _, :state],
      handle_rollback: [_, :new_state]
      ] = A.record(agent)
  end

  test "transaction rolls back on failed transaction" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, :rolledback, :newer_state}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    conn = P.checkout_begin(pool, opts)

    assert P.transaction(conn, fn(conn2) ->
      assert P.transaction(conn2, &P.rollback(&1, :oops), opts) == {:error, :oops}
    end, opts) == {:error, :rollback}

    assert P.transaction(conn, fn(_) ->
      flunk "should not run"
    end, opts) == {:error, :rollback}

    assert [
      connect: [_],
      handle_begin: [ _, :state],
      handle_rollback: [_, :new_state]
      ] = A.record(agent)
  end

  test "commit_checkin logs" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, :committed, :newer_state}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    conn = P.checkout_begin(pool, opts)

    log = &send(parent, &1)
    assert P.commit_checkin(conn, [log: log]) == :ok

    assert_received %DBConnection.LogEntry{call: :commit_checkin} = entry
    assert %{query: :commit, params: nil, result: {:ok, :committed}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert [
      connect: [_],
      handle_begin: [ _, :state],
      handle_commit: [_, :new_state]
      ] = A.record(agent)
  end

  test "rollback_checkin logs" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, :rolledback, :newer_state}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    conn = P.checkout_begin(pool, opts)

    log = &send(parent, &1)
    assert P.rollback_checkin(conn, :oops, [log: log]) == {:error, :oops}

    assert_received %DBConnection.LogEntry{call: :rollback_checkin} = entry
    assert %{query: :rollback, params: nil, result: {:ok, :rolledback}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert [
      connect: [_],
      handle_begin: [ _, :state],
      handle_rollback: [_, :new_state]
      ] = A.record(agent)
  end
end
