defmodule TestIdle do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestResult, as: R

  @tag :idle_time
  test "includes idle time in log entries" do
    parent = self()

    stack = [
      {:ok, :state},
      {:ok, %Q{}, %R{}, :state},
      {:ok, %Q{}, %R{}, :state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.execute(pool, %Q{}, [:param1]) == {:ok, %Q{}, %R{}}
    Process.sleep(10)

    log = fn entry ->
      assert %DBConnection.LogEntry{} = entry
      assert is_integer(entry.idle_time)
      assert entry.idle_time > 0
      send(parent, :logged)
    end

    assert P.execute(pool, %Q{}, [:param2], log: log) == {:ok, %Q{}, %R{}}
    assert_receive :logged

    assert [
             connect: [_],
             handle_execute: _,
             handle_execute: _
           ] = A.record(agent)
  end

  @tag :idle_interval
  test "ping after idle interval" do
    parent = self()

    stack = [
      fn opts ->
        send(opts[:parent], {:hi, self()})
        {:ok, :state}
      end,
      fn _ ->
        send(parent, {:pong, self()})
        :timer.sleep(10)
        {:ok, :state}
      end,
      fn _ ->
        send(parent, {:pong, self()})
        assert_receive {:continue, ^parent}
        {:ok, :state}
      end,
      {:idle, :state},
      {:idle, :state},
      fn _ ->
        send(parent, {:pong, self()})
        :timer.sleep(:infinity)
      end
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), idle_interval: 50]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}
    assert_receive {:pong, ^conn}
    assert_receive {:pong, ^conn}
    send(conn, {:continue, self()})
    P.run(pool, fn _ -> :ok end)
    assert_receive {:pong, ^conn}

    assert [
             connect: [_],
             ping: [:state],
             ping: [:state],
             handle_status: _,
             handle_status: _,
             ping: [:state]
           ] = A.record(agent)
  end

  @tag :idle_interval
  test "ping multiple after idle interval" do
    ping_multiple(2)
  end

  test "ping multiple after idle interval and limit" do
    ping_multiple(1)
  end

  defp ping_multiple(limit) do
    parent = self()

    stack = [
      fn opts ->
        send(opts[:parent], {:hi, self()})
        {:ok, :state}
      end,
      fn _ ->
        send(parent, {:pong, self(), System.system_time(:millisecond)})
        :timer.sleep(10)
        {:ok, :state}
      end,
      fn _ ->
        send(parent, {:pong, self(), System.system_time(:millisecond)})
        assert_receive {:continue, ^parent}
        {:ok, :state}
      end,
      {:idle, :state},
      {:idle, :state},
      fn _ ->
        send(parent, {:pong, self()})
        :timer.sleep(:infinity)
      end
    ]

    {:ok, agent1} = A.start_link(stack)
    {:ok, agent2} = A.start_link(stack)

    opts = [
      agent: [agent1, agent2],
      parent: self(),
      idle_interval: 100,
      idle_limit: limit,
      pool_size: 2
    ]

    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn1}
    assert_receive {:hi, conn2}

    # We want to assert that time1/time2 and time3/time4
    # happen within the same ping *most times* but unfortunately
    # that will lead to many false positives in test runs
    # unless idle_limit is 1.

    assert_receive {:pong, ^conn1, time1}
    assert_receive {:pong, ^conn2, time2} when limit == 2 or time2 > time1
    assert_receive {:pong, ^conn1, time3} when limit == 2 or time3 > time2
    assert_receive {:pong, ^conn2, time4} when limit == 2 or time4 > time3

    send(conn1, {:continue, self()})
    send(conn2, {:continue, self()})

    task1 =
      Task.async(fn ->
        Process.put(:agent, agent1)
        P.run(pool, fn _ -> assert_receive :done end)
      end)

    task2 =
      Task.async(fn ->
        Process.put(:agent, agent2)
        P.run(pool, fn _ -> assert_receive :done end)
      end)

    send(task1.pid, :done)
    send(task2.pid, :done)
    assert_receive {:pong, ^conn1}
    assert_receive {:pong, ^conn2}

    assert [
             connect: [_],
             ping: [:state],
             ping: [:state],
             handle_status: _,
             handle_status: _,
             ping: [:state]
           ] = A.record(agent1)

    assert [
             connect: [_],
             ping: [:state],
             ping: [:state],
             handle_status: _,
             handle_status: _,
             ping: [:state]
           ] = A.record(agent2)
  end
end
