defmodule CursorTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestCursor, as: C
  alias TestResult, as: R

  test "declare/fetch/deallocate return result" do
    stack = [
      {:ok, :state},
      {:ok, %C{}, :new_state},
      {:ok, %C{}, :newer_state},
      {:cont, %R{}, :newest_state},
      {:halt, %R{}, :state2},
      {:ok, :deallocated, :new_state2},
      {:ok, :deallocated, :newer_state2}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.declare(pool, %Q{}, [:param]) == {:ok, %C{}}
    assert P.declare!(pool, %Q{}, [:param], [key: :value]) == %C{}

    assert P.fetch(pool, %Q{}, %C{}) == {:cont, %R{}}
    assert P.fetch!(pool, %Q{}, %C{}, [key: :value]) == {:halt, %R{}}

    assert P.deallocate(pool, %Q{}, %C{}) == {:ok, :deallocated}
    assert P.deallocate!(pool, %Q{}, %C{}, [key: :value]) == :deallocated

    assert [
      connect: [_],
      handle_declare: [%Q{}, [:param], _, :state],
      handle_declare: [%Q{}, [:param], [{:key, :value} | _], :new_state],
      handle_fetch: [%Q{}, %C{}, _, :newer_state],
      handle_fetch: [%Q{}, %C{}, [{:key, :value} | _], :newest_state],
      handle_deallocate: [%Q{}, %C{}, _, :state2],
      handle_deallocate: [%Q{}, %C{}, [{:key, :value} | _], :new_state2]
      ] = A.record(agent)
  end

  test "declare encodes params" do
    stack = [
      {:ok, :state},
      {:ok, %C{}, :new_state},
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    opts2 = [encode: fn([:param]) -> :encoded end]
    assert P.declare(pool, %Q{}, [:param], opts2) == {:ok, %C{}}

    assert [
      connect: [_],
      handle_declare: [%Q{}, :encoded, _, :state]] = A.record(agent)
  end

  test "declare replaces query" do
    stack = [
      {:ok, :state},
      {:ok, %Q{state: :replaced}, %C{}, :new_state},
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.declare(pool, %Q{state: :old}, [:param], opts) ==
      {:ok, %Q{state: :replaced}, %C{}}

    assert [
      connect: [_],
      handle_declare: [%Q{state: :old}, [:param], _, :state]] = A.record(agent)
  end

  test "fetch decodes result" do
    stack = [
      {:ok, :state},
      {:cont, %R{}, :new_state},
      {:halt, %R{}, :newer_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    opts2 = [decode: fn(%R{}) -> :decoded end]
    assert P.fetch(pool, %Q{}, %C{}, opts2) == {:cont, :decoded}
    assert P.fetch(pool, %Q{}, %C{}, opts2) == {:halt, :decoded}

    assert [
      connect: [_],
      handle_fetch: [%Q{}, %C{}, _, :state],
      handle_fetch: [%Q{}, %C{}, _, :new_state]
      ] = A.record(agent)
  end

  test "declare/fetch/deallocate logs result" do
    stack = [
      {:ok, :state},
      {:ok, %C{}, :new_state},
      {:cont, %R{}, :newer_state},
      {:halt, %R{}, :newest_state},
      {:ok, :deallocated, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    log = &send(parent, &1)
    assert P.declare(pool, %Q{}, [:param], [log: log]) == {:ok, %C{}}

    assert_receive %DBConnection.LogEntry{call: :declare, query: %Q{},
      params: [:param], result: {:ok, %C{}}} = entry
    assert is_integer(entry.pool_time)
    assert entry.pool_time >= 0
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert P.fetch(pool, %Q{}, %C{}, [log: log]) == {:cont, %R{}}

    assert_receive %DBConnection.LogEntry{call: :fetch, query: %Q{},
      params: %C{}, result: {:ok, %R{}}} = entry
    assert is_integer(entry.pool_time)
    assert entry.pool_time >= 0
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_integer(entry.decode_time)
    assert entry.decode_time >= 0

    assert P.fetch(pool, %Q{}, %C{}, [log: log]) == {:halt, %R{}}

    assert_receive %DBConnection.LogEntry{call: :fetch, query: %Q{},
      params: %C{}, result: {:ok, %R{}}} = entry
    assert is_integer(entry.pool_time)
    assert entry.pool_time >= 0
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_integer(entry.decode_time)
    assert entry.decode_time >= 0

    assert P.deallocate(pool, %Q{}, %C{}, [log: log]) == {:ok, :deallocated}

    assert_receive %DBConnection.LogEntry{call: :deallocate, query: %Q{},
      params: %C{}, result: {:ok, :deallocated}} = entry
    assert is_integer(entry.pool_time)
    assert entry.pool_time >= 0
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert [
      connect: [_],
      handle_declare: [%Q{}, [:param], _, :state],
      handle_fetch: [%Q{}, %C{}, _, :new_state],
      handle_fetch: [%Q{}, %C{}, _, :newer_state],
      handle_deallocate: [%Q{}, %C{}, _, :newest_state]
      ] = A.record(agent)
  end

  test "declare/fetch/deallocate error returns error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:error, err, :new_state},
      {:error, err, :newer_state},
      {:error, err, :newesr_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.declare(pool, %Q{}, [:param]) == {:error, err}
    assert P.fetch(pool, %Q{}, %C{}) == {:error, err}
    assert P.deallocate(pool, %Q{}, %C{}) == {:error, err}

    assert [
      connect: [_],
      handle_declare: [%Q{}, [:param], _, :state],
      handle_fetch: [%Q{}, %C{}, _, :new_state],
      handle_deallocate: [%Q{}, %C{}, _, :newer_state]
      ] = A.record(agent)
  end
end
