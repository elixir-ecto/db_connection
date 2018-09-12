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
      {:idle, :state},
      {:idle, :state},
      fn(_) ->
        send(parent, {:pong, self()})
        :timer.sleep(:infinity)
      end]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), idle_timeout: 50, idle_interval: 50]
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
      handle_status: _,
      handle_status: _,
      ping: [:state]] = A.record(agent)
  end
end
