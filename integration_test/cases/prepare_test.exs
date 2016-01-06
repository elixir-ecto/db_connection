defmodule PrepareTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q

  test "prepare returns query" do
    stack = [
      {:ok, :state},
      {:ok, %Q{state: :prepared}, :new_state},
      {:ok, %Q{state: :prepared}, :new_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.prepare(pool, %Q{}) == {:ok, %Q{state: :prepared}}
    assert P.prepare(pool, %Q{}, [key: :value]) == {:ok, %Q{state: :prepared}}

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state],
      handle_prepare: [%Q{}, [{:key, :value} | _], :new_state]
      ] = A.record(agent)
  end

  test "prepare parses query" do
    stack = [
      {:ok, :state},
      {:ok, %Q{state: :prepared}, :new_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    opts2 = [parse: fn(%Q{}) -> %Q{state: :parsed} end]
    assert P.prepare(pool, %Q{}, opts2) == {:ok, %Q{state: :prepared}}

    assert [
      connect: [_],
      handle_prepare: [%Q{state: :parsed}, _, :state]] = A.record(agent)
  end

  test "prepare describes query" do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    opts2 = [describe: fn(%Q{}) -> %Q{state: :described} end]
    assert P.prepare(pool, %Q{}, opts2) == {:ok, %Q{state: :described}}

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state]] = A.record(agent)
  end

  test "prepare logs result" do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state},
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    log = fn(entry) ->
      assert %DBConnection.LogEntry{call: :prepare, query: %Q{},
                                    params: nil, result: {:ok, %Q{}}} = entry
      assert is_integer(entry.pool_time)
      assert entry.pool_time >= 0
      assert is_integer(entry.connection_time)
      assert entry.connection_time >= 0
      assert is_nil(entry.decode_time)
      send(parent, :logged)
    end
    assert P.prepare(pool, %Q{}, [log: log]) == {:ok, %Q{}}
    assert_received :logged

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state]] = A.record(agent)
  end

  test "prepare error returns error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:error, err, :new_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.prepare(pool, %Q{}) == {:error, err}

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state]] = A.record(agent)
  end

  test "prepare logs error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:error, err, :new_state}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)
    log = fn(entry) ->
      assert %DBConnection.LogEntry{call: :prepare, query: %Q{},
                                    params: nil, result: {:error, ^err}} = entry
      assert is_integer(entry.pool_time)
      assert entry.pool_time >= 0
      assert is_integer(entry.connection_time)
      assert entry.connection_time >= 0
      assert is_nil(entry.decode_time)
      send(parent, :logged)
    end
    assert P.prepare(pool, %Q{}, [log: log]) == {:error, err}
    assert_received :logged

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state]] = A.record(agent)
  end

  test "prepare! error raises error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:error, err, :new_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert_raise RuntimeError, "oops", fn() -> P.prepare!(pool, %Q{}) end

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state]] = A.record(agent)
  end
end
