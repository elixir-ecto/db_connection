defmodule TestOverflow do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A

  @tag :pool_overflow
  test "pool_overflow grows and shrinks pool" do
    Process.flag(:trap_exit, true)
    parent = self()
    stack = [
      fn(_) ->
        send(parent, {:hi1, self()})
        {:ok, :state1}
      end,
      fn(_) ->
        send(parent, {:hi2, self()})
        {:ok, :state2}
      end,
      fn(_, :state2) ->
        Process.exit(parent, :shutdown)
        :ok
      end]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, pool_overflow: 1, regulator_update: 10,
            idle_interval: 10, idle_target: 5, idle_timeout: 100, idle_min: 1]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi1, conn1}

    P.run(pool, fn(_) ->
      conn2 = P.run(pool, fn(_) ->
        assert_received {:hi2, conn2} when conn2 != conn1
        Process.link(conn2)
        conn2
      end)
      assert_receive {:EXIT, ^conn2, :shutdown}
    end)

    assert [
      {:connect, [_]},
      {:connect, [_]} | _] = A.record(agent)
  end
end
