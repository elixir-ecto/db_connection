defmodule ResourceTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestResult, as: R

  test "resource transaction commits after stream resource reduced" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %R{}, :newer_state},
      {:ok, :committed, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    start = fn() -> P.resource_begin(pool, opts) end
    next = fn(conn) ->
      {:ok, res} = P.resource_transaction(conn, fn(conn2) ->
        P.execute!(conn2, %Q{}, [:param], opts)
      end, opts)
      {[res], conn}
    end
    stop = &P.resource_commit/1

    assert Stream.resource(start, next, stop) |> Enum.take(1) == [%R{}]

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_execute: [%Q{}, [:param], _, :new_state],
      handle_commit: [_, :newer_state]
      ] = A.record(agent)
  end

  test "resource transaction commits per trigger inside Flow pipeline" do
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
      |> Flow.reduce(fn() -> {[], P.resource_begin(pool, opts)} end,
                     fn(params, {acc, conn}) ->
                       {:ok, res} = P.resource_transaction(conn, fn(conn2) ->
                         P.execute!(conn2, %Q{}, params, opts)
                       end)
                       {[res | acc], conn}
                     end)
      |> Flow.map_state(fn({acc, conn}) ->
        P.resource_commit(conn, opts)
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

  test "resource_transaction raises inside run" do
    stack = [{:ok, :state}]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.run(pool, fn(conn) ->
      assert_raise RuntimeError, "not inside transaction",
        fn ->
          P.resource_transaction(conn, fn(_) -> flunk "should not run" end, opts)
        end
      :hello
    end, opts) == :hello

    assert [
      connect: [_]
      ] = A.record(agent)
  end

  test "resource_transaction runs inside transaction" do
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
      assert P.resource_transaction(conn, &P.execute!(&1, %Q{}, [:param], opts),
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

  test "resource_transaction rolls back and functions error" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, :rolledback, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    conn = P.resource_begin(pool, opts)

    assert P.resource_rollback(conn, :oops, opts) == {:error, :oops}
    assert P.resource_commit(conn, opts) == {:error, :rollback}
    assert P.resource_rollback(conn, :oops, opts) == {:error, :oops}
    assert P.resource_transaction(conn, fn(_) ->
      flunk "should not fun"
    end, opts) == {:error, :rollback}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_rollback: [_, :new_state]
      ] = A.record(agent)
  end

  test "resource_transaction runs inside resource_transaction" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %R{}, :newer_state},
      {:ok, :commited, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    conn = P.resource_begin(pool, opts)

    assert P.resource_transaction(conn, fn(conn2) ->
      assert P.resource_transaction(conn2, fn(conn3) ->
          P.execute!(conn3, %Q{}, [:param], opts)
        end, opts) == {:ok, %R{}}
      :hello
    end) == {:ok, :hello}

    assert P.resource_commit(conn, opts) == :ok

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_execute: [%Q{}, [:param], _, :new_state],
      handle_commit: [_, :newer_state]
      ] = A.record(agent)
  end

  test "resource_begin logs error" do
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
      fn() -> P.resource_begin(pool, [log: log]) end

    assert_received %DBConnection.LogEntry{call: :resource_begin} = entry
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

  test "resource_begin logs raises and rolls back" do
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
      fn() -> P.resource_begin(pool, [log: log]) end

    assert_received %DBConnection.LogEntry{call: :resource_begin} = entry
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

  test "resource_transaction logs on rollback" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, :rolledback, :newer_state}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    conn = P.resource_begin(pool, opts)

    log = &send(parent, &1)
    assert P.resource_transaction(conn, fn(conn2) ->
      P.rollback(conn2, :oops)
    end, [log: log]) == {:error, :oops}

    assert_received %DBConnection.LogEntry{call: :resource_transaction} = entry
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

  test "resource_transaction rolls back on failed transaction" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, :rolledback, :newer_state}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    conn = P.resource_begin(pool, opts)

    assert P.resource_transaction(conn, fn(conn2) ->
      assert P.transaction(conn2, &P.rollback(&1, :oops), opts) == {:error, :oops}
    end, opts) == {:error, :rollback}

    assert P.resource_transaction(conn, fn(_) ->
      flunk "should not run"
    end, opts) == {:error, :rollback}

    assert [
      connect: [_],
      handle_begin: [ _, :state],
      handle_rollback: [_, :new_state]
      ] = A.record(agent)
  end

  test "resource_commit logs" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, :committed, :newer_state}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    conn = P.resource_begin(pool, opts)

    log = &send(parent, &1)
    assert P.resource_commit(conn, [log: log]) == :ok

    assert_received %DBConnection.LogEntry{call: :resource_commit} = entry
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

  test "resource_rollback logs" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, :rolledback, :newer_state}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    conn = P.resource_begin(pool, opts)

    log = &send(parent, &1)
    assert P.resource_rollback(conn, :oops, [log: log]) == {:error, :oops}

    assert_received %DBConnection.LogEntry{call: :resource_rollback} = entry
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
