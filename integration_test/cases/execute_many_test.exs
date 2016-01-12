defmodule ExecuteManyTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestResult, as: R

  test "execute_many returns result" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, [%R{}], :newer_state},
      {:ok, :committed, :newest_state},
      {:ok, :began, :state2},
      {:ok, [%R{}], :new_state2},
      {:ok, :committed, :newer_state2}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.execute_many(pool, [{%Q{}, [:param], [key: :value]}]) ==
      {:ok, [%R{}]}
    assert P.execute_many(pool, [{%Q{}, [:param], []}], [key: :value]) ==
      {:ok, [%R{}]}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_execute_many: [[{%Q{}, [:param], [key: :value]}], _, :new_state],
      handle_commit: [_, :newer_state],
      handle_begin: [[{:key, :value} | _], :newest_state],
      handle_execute_many: [[{%Q{}, [:param], []}],
        [{:key, :value} | _], :state2],
      handle_commit: [[{:key, :value} | _], :new_state2]] = A.record(agent)
  end

  test "execute_many logs result" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, [%R{}], :newer_state},
      {:ok, :committed, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    log = &send(parent, &1)
    assert P.execute_many(pool, [{%Q{}, [:param], []}], [log: log]) ==
      {:ok, [%R{}]}

    assert_received %DBConnection.LogEntry{} = entry
    assert %{call: :transaction, query: :begin, params: nil,
             result: {:ok, :began}} = entry
    assert is_integer(entry.pool_time)
    assert entry.pool_time >= 0
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert_received %DBConnection.LogEntry{} = entry
    assert %{call: :execute_many, query: :batch, params: [{%Q{}, [:param], []}],
             result: {:ok, [%R{}]}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_integer(entry.decode_time)
    assert entry.decode_time >= 0

    assert_received %DBConnection.LogEntry{} = entry
    assert %{call: :transaction, query: :commit, params: nil,
             result: {:ok, :committed}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_execute_many: [[{%Q{}, [:param], []}], _, :new_state],
      handle_commit: [_, :newer_state]] = A.record(agent)
  end

  test "execute_many error returns error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:error, err, :newer_state},
      {:ok, :rolledback, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.execute_many(pool, [{%Q{}, [:param], []}]) == {:error, err}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_execute_many: [[{%Q{}, [:param], []}], _, :new_state],
      handle_rollback: [_, :newer_state]] = A.record(agent)
  end

  test "execute_many! error raises error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:error, err, :newer_state},
      {:ok, :rolledback, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_raise RuntimeError, "oops",
      fn() -> P.execute_many!(pool, [{%Q{}, [:param], []}]) end

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_execute_many: [[{%Q{}, [:param], []}], _, :new_state],
      handle_rollback: [_, :newer_state]] = A.record(agent)
  end

  test "execute_many logs error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:error, err, :newer_state},
      {:ok, :rolledback, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    log = &send(parent, &1)
    assert P.execute_many(pool, [{%Q{}, [:param], []}], [log: log]) ==
      {:error, err}

    assert_received %DBConnection.LogEntry{} = entry
    assert %{call: :transaction, query: :begin, params: nil,
             result: {:ok, :began}} = entry

    assert_received %DBConnection.LogEntry{} = entry
    assert %{call: :execute_many, query: :batch, params: [{%Q{}, [:param], []}],
             result: {:error, ^err}} = entry
    assert is_nil(entry.decode_time)

    assert_received %DBConnection.LogEntry{} = entry
    assert %{call: :transaction, query: :rollback, params: nil,
             result: {:ok, :rolledback}} = entry

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_execute_many: [[{%Q{}, [:param], []}], _, :new_state],
      handle_rollback: [_, :newer_state]] = A.record(agent)
  end

  test "execute_many disconnect returns error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:disconnect, err, :newer_state},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :state}
      end
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    assert P.execute_many(pool, [{%Q{}, [:param], []}]) == {:error, err}

    assert_receive :reconnected

    assert [
      connect: [opts2],
      handle_begin: [_, :state],
      handle_execute_many: [[{%Q{}, [:param], []}], _, :new_state],
      disconnect: [^err, :newer_state],
      connect: [opts2]] = A.record(agent)
  end

  test "execute_many disconnect logs error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:disconnect, err, :newer_state},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :state}
      end
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    log = &send(parent, &1)
    assert P.execute_many(pool, [{%Q{}, [:param], []}], [log: log]) ==
      {:error, err}

    assert_received %DBConnection.LogEntry{} = entry
    assert %{call: :transaction, query: :begin, params: nil,
             result: {:ok, :began}} = entry

    assert_received %DBConnection.LogEntry{} = entry
    assert %{call: :execute_many, query: :batch, params: [{%Q{}, [:param], []}],
             result: {:error, ^err}} = entry
    assert is_nil(entry.decode_time)

    refute_received %DBConnection.LogEntry{} = entry

    assert_receive :reconnected

    assert [
      connect: [opts2],
      handle_begin: [_, :state],
      handle_execute_many: [[{%Q{}, [:param], []}], _, :new_state],
      disconnect: [^err, :newer_state],
      connect: [opts2]] = A.record(agent)
  end

  test "execute_many bad return raises DBConnection.Error and stops" do
    stack = [
      fn(opts) ->
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

    assert_raise DBConnection.Error, "bad return value: :oops",
      fn() -> P.execute_many(pool, [{%Q{}, [:param], []}]) end

    assert_receive {:EXIT, ^conn,
      {%DBConnection.Error{message: "client stopped: " <> _}, [_|_]}}

    assert [
      {:connect, _},
      {:handle_begin, [_, :state]},
      {:handle_execute_many, [[{%Q{}, [:param], []}], _, :new_state]} |
      _] = A.record(agent)
  end

  test "execute_many raise raises and stops connection but no log" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, :began, :new_state},
      fn(_, _, _) ->
        raise "oops"
      end,
      {:ok, :state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}

    Process.flag(:trap_exit, true)

    log = fn(_) -> flunk "logged" end
    assert_raise RuntimeError, "oops",
      fn() -> P.execute_many(pool, [{%Q{}, [:param], []}], [log: log]) end

    assert_receive {:EXIT, ^conn,
      {%DBConnection.Error{message: "client stopped: " <> _}, [_|_]}}

    assert [
      {:connect, _},
      {:handle_begin, [_, :state]},
      {:handle_execute_many, [[{%Q{}, [:param], []}], _, :new_state]} |
      _] = A.record(agent)
  end
end
