defmodule TestShutdown do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A

  test "connection shutdown with :shutdown strategy" do
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

  test "pool shutdowns with broker" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        {:ok, :state}
      end]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), shutdown: :brutal_kill]
    {:ok, pool} = P.start_link(opts)

    assert_receive {:hi, conn}
    conn_mon = Process.monitor(conn)

    assert {caller_refs, started_refs} = :sys.get_state(DBConnection.Watcher)
    caller_ref = Enum.find_value(started_refs, fn {_, {pid, ref}} -> pid == pool && ref end)
    {_, pool_sup, _} = Map.fetch!(caller_refs, caller_ref)

    pool_mon = Process.monitor(pool_sup)
    _ = Process.flag(:trap_exit, true)

    Process.exit(pool, :shutdown)
    assert_receive {:DOWN, ^pool_mon, _, _, :shutdown}
    assert_received {:DOWN, ^conn_mon, _, _, :killed}

    assert [connect: [_]] = A.record(agent)
  end

  test "broker shutdowns with pool" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        {:ok, :state}
      end]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), shutdown: :brutal_kill]
    {:ok, pool} = P.start_link(opts)

    assert_receive {:hi, conn}
    conn_mon = Process.monitor(conn)

    assert {caller_refs, started_refs} = :sys.get_state(DBConnection.Watcher)
    caller_ref = Enum.find_value(started_refs, fn {_, {pid, ref}} -> pid == pool && ref end)
    {_, pool_sup, _} = Map.fetch!(caller_refs, caller_ref)

    _ = Process.flag(:trap_exit, true)
    sup = DBConnection.Sojourn.Supervisor
    assert Supervisor.terminate_child(sup, pool_sup) == :ok
    assert_receive {:DOWN, ^conn_mon, _, _, :killed}
    assert_receive {:EXIT, ^pool, :killed}

    assert [connect: [_]] = A.record(agent)
  end
end
