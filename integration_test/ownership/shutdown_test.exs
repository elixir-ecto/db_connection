defmodule TestShutdown do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A

  test "pool shutdowns with manager" do
    stack = [{:ok, :state}]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), shutdown: :brutal_kill]
    {:ok, pool} = P.start_link(opts)

    assert {caller_refs, started_refs} = :sys.get_state(DBConnection.Watcher)
    caller_ref = Enum.find_value(started_refs, fn {_, {pid, ref}} -> pid == pool && ref end)
    {_, pool_sup, _} = Map.fetch!(caller_refs, caller_ref)

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

    assert {caller_refs, started_refs} = :sys.get_state(DBConnection.Watcher)
    caller_ref = Enum.find_value(started_refs, fn {_, {pid, ref}} -> pid == pool && ref end)
    {_, pool_sup, _} = Map.fetch!(caller_refs, caller_ref)

    _ = Process.flag(:trap_exit, true)
    sup = DBConnection.Ownership.PoolSupervisor
    assert Supervisor.terminate_child(sup, pool_sup) == :ok
    assert_receive {:EXIT, ^pool, :killed}

    assert [connect: [_]] = A.record(agent)
  end
end
