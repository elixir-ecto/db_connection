defmodule TestShutdown do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A

  test "prepare returns query" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        {:ok, :state}
      end]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), shutdown: :brutal_kill]
    {:ok, pool} = P.start_link(opts)

    assert_receive {:hi, conn}
    monitor = Process.monitor(conn)

    _ = Process.flag(:trap_exit, true)
    Process.exit(pool, :shutdown)
    assert_receive {:DOWN, ^monitor, _, _, :killed}

    assert [connect: [_]] = A.record(agent)
  end
end
