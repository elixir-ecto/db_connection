defmodule CloseTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q

  test "close returns ok" do
    stack = [
      {:ok, :state},
      {:ok, :new_state},
      {:ok, :new_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.close(pool, %Q{}) == :ok
    assert P.close(pool, %Q{}, [key: :value]) == :ok

    assert [
      connect: [_],
      handle_close: [%Q{}, _, :state],
      handle_close: [%Q{}, [{:key, :value} | _], :new_state]] = A.record(agent)
  end

  test "close logs ok" do
    stack = [
      {:ok, :state},
      {:ok, :new_state},
     ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    log = fn(entry) ->
      assert %DBConnection.LogEntry{call: :close, query: %Q{},
                                    params: nil, result: :ok} = entry
      assert is_integer(entry.pool_time)
      assert entry.pool_time >= 0
      assert is_integer(entry.connection_time)
      assert entry.connection_time >= 0
      assert is_nil(entry.decode_time)
      send(parent, :logged)
    end
    assert P.close(pool, %Q{}, [log: log]) == :ok
    assert_received :logged

    assert [
      connect: [_],
      handle_close: [%Q{}, _, :state]] = A.record(agent)
  end

  test "close error returns error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:error, err, :new_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.close(pool, %Q{}) == {:error, err}

    assert [
      connect: [_],
      handle_close: [%Q{}, _, :state]] = A.record(agent)
  end

  test "close logs error" do
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
      assert %DBConnection.LogEntry{call: :close, query: %Q{},
                                    params: nil, result: {:error, ^err}} = entry
      assert is_integer(entry.pool_time)
      assert entry.pool_time >= 0
      assert is_integer(entry.connection_time)
      assert entry.connection_time >= 0
      assert is_nil(entry.decode_time)
      send(parent, :logged)
    end
    assert P.close(pool, %Q{}, [log: log]) == {:error, err}
    assert_received :logged

    assert [
      connect: [_],
      handle_close: [%Q{}, _, :state]] = A.record(agent)
  end

  test "close! error raises error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:error, err, :new_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert_raise RuntimeError, "oops", fn() -> P.close!(pool, %Q{}) end

    assert [
      connect: [_],
      handle_close: [%Q{}, _, :state]] = A.record(agent)
  end
end
