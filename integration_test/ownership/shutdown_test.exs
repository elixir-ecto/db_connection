defmodule TestShutdown do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A

  test "pool shutdowns with manager" do
    stack = [{:ok, :state}]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), shutdown: :brutal_kill]
    {:ok, pool} = P.start_link(opts)

    {:links, links} = Process.info(pool, :links)
    [inner_pool] = links -- [self()]

    Process.flag(:trap_exit, true)
    ref = Process.monitor(inner_pool)
    Process.exit(pool, :shutdown)
    assert_receive {:DOWN, ^ref, _, _, :shutdown}

    assert [connect: [_]] = A.record(agent)
  end

  test "manager shutdowns with pool" do
    stack = [{:ok, :state}]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), shutdown: :brutal_kill]
    {:ok, pool} = P.start_link(opts)

    {:links, links} = Process.info(pool, :links)
    [inner_pool] = links -- [self()]

    _ = Process.flag(:trap_exit, true)
    Process.exit(inner_pool, :shutdown)
    assert_receive {:EXIT, ^pool, :shutdown}
    assert [connect: [_]] = A.record(agent)
  end
end
