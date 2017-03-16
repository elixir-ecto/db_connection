defmodule TestIdle do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A

  @tag :idle_timeout
  test "ping after idle timeout" do
    parent = self()
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        {:ok, :state}
      end,
      fn(_) ->
        send(parent, {:pong, self()})
        :timer.sleep(10)
        {:ok, :state}
      end,
      fn(_) ->
        send(parent, {:pong, self()})
        assert_receive {:continue, ^parent}
        {:ok, :state}
      end,
      fn(_) ->
        send(parent, {:pong, self()})
        :timer.sleep(:infinity)
      end]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), idle_timeout: 50]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}
    assert_receive {:pong, ^conn}
    assert_receive {:pong, ^conn}
    send(conn, {:continue, self()})
    P.run(pool, fn(_) -> :ok end)
    assert_receive {:pong, ^conn}

    assert [
      connect: [_],
      ping: [:state],
      ping: [:state],
      ping: [:state]] = A.record(agent)
  end

  @tag :idle_timeout
  test "ping manually" do
    err = RuntimeError.exception(message: "oops")
    stack = [
      {:ok, :state},
      {:disconnect, err, :new_state},
      :ok,
      {:ok, :state2},
      {:ok, :new_state2},
      {:disconnect, err, :newer_state2},
      :ok,
      {:ok, :state3},
      {:ok, :new_state3}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    assert P.ping(pool) == :pang
    assert P.ping(pool) == :pong

    assert P.ping(pool, [log: &send(parent, &1)]) == :pang

    assert_received %DBConnection.LogEntry{call: :ping} = entry
    assert %{query: nil, params: nil, result: {:ok, :pang}} = entry
    assert is_integer(entry.pool_time)
    assert entry.pool_time >= 0
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)


    assert P.run(pool, fn(conn) ->
      P.ping(conn, [log: &send(parent, &1)])
    end) == :pong

    assert_received %DBConnection.LogEntry{call: :ping} = entry
    assert %{query: nil, params: nil, result: {:ok, :pong}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert [
      connect: [_],
      ping: [:state],
      disconnect: [^err, :new_state],
      connect: [_],
      ping: [:state2],
      ping: [:new_state2],
      disconnect: [^err, :newer_state2],
      connect: [_],
      ping: [:state3]] = A.record(agent)
  end
end
