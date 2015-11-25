defmodule TestIdle do
  use ExUnit.Case, async: false

  alias TestPool, as: P
  alias TestAgent, as: A

  test "ping after idle timeout" do
    parent = self()
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        {:ok, :state}
      end,
      fn(_) ->
        send(parent, {:pong, self()})
        {:ok, :state}
      end,
      fn(_) ->
        :timer.sleep(:infinity)
      end]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), idle_timeout: 10]
    {:ok, _} = P.start_link(opts)
    assert_receive {:hi, conn}
    assert_receive {:pong, ^conn}

    assert [
      connect: [_],
      ping: [:state]] = A.record(agent)
  end
end
