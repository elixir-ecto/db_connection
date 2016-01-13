defmodule TestShutdown do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A

  test "pool shutdowns with manager" do
    stack = [{:ok, :state}]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), shutdown: :brutal_kill]
    {:ok, pool} = P.start_link(opts)

    assert {:links, links} = Process.info(pool, :links)
    assert [watcher] = links -- [self()]

    assert {:status, _, _, [_, _, pool_sup, _, _]} = :sys.get_status(watcher)
    monitor = Process.monitor(pool_sup)
    _ = Process.flag(:trap_exit, true)

    Process.exit(pool, :shutdown)
    assert_receive {:DOWN, ^monitor, _, _, :shutdown}

    assert [connect: [_]] = A.record(agent)
  end

  test "manager shutdowns with pool" do
    stack = [{:ok, :state}]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), shutdown: :brutal_kill]
    {:ok, pool} = P.start_link(opts)

    assert {:links, links} = Process.info(pool, :links)
    assert [watcher] = links -- [self()]

    assert {:status, _, _, [_, _, pool_sup, _, _]} = :sys.get_status(watcher)

    _ = Process.flag(:trap_exit, true)
    sup = DBConnection.Ownership.PoolSupervisor
    assert Supervisor.terminate_child(sup, pool_sup) == :ok
    assert_receive {:EXIT, ^pool, :shutdown}

    assert [connect: [_]] = A.record(agent)
  end
end
