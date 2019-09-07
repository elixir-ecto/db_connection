defmodule TestIdle do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A

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

    {:ok, agent1} = A.start_link(stack)
    {:ok, agent2} = A.start_link(stack)

    opts = [agent: [agent1, agent2], parent: self(), idle_interval: 50, pool_size: 2]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn1}
    assert_receive {:hi, conn2}
    assert_receive {:pong, ^conn1}
    assert_receive {:pong, ^conn2}
    assert_receive {:pong, ^conn1}
    assert_receive {:pong, ^conn2}
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
