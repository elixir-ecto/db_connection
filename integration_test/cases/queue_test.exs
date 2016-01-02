defmodule QueueTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A

  test "queue: false raises on busy" do
    stack = [{:ok, :state}]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)


    P.run(pool, fn(_) ->
      {queue_time, _} = :timer.tc(fn() ->
        opts = [queue: false]
        assert_raise DBConnection.Error, "connection not available",
          fn() -> P.run(pool, fn(_) -> flunk("got connection") end, opts) end
      end)
      assert queue_time <= 1_000_000, "request was queued"
    end)
  end

  test "queue many async" do
    stack = [{:ok, :state}]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    run = fn() -> P.run(pool, fn(_) -> :timer.sleep(20) end) end
    for task <- Enum.map(1..10, fn(_) -> Task.async(run) end) do
      assert :ok = Task.await(task)
    end
  end

  test "queue many async timeouts" do
    stack = [{:ok, :state}]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), queue_timeout: 50]
    {:ok, pool} = P.start_link(opts)

    parent = self()
    runner = spawn_link(fn() ->
      P.run(pool, fn(_) ->
        send(parent, {:go, self()})
        receive do
          {:done, ^parent} -> :ok
        end
      end)
    end)
    assert_receive {:go, ^runner}

    run = fn() ->
      try do
        P.run(pool, fn(_) -> flunk("run ran") end, [pool_timeout: 50])
      rescue
        DBConnection.Error ->
          :error
      catch
        :exit, {:timeout, _} ->
          :error
      end
    end
    for task <- Enum.map(1..10, fn(_) -> Task.async(run) end) do
      assert Task.await(task) == :error
    end

    send(runner, {:done, self()})
    assert P.run(pool, fn(_) -> :result end) == :result
  end

  test "queue many async exits" do
    stack = [{:ok, :state}]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    parent = self()
    runner = spawn_link(fn() ->
      P.run(pool, fn(_) ->
        send(parent, {:go, self()})
        receive do
          {:done, ^parent} -> :ok
        end
      end)
    end)
    assert_receive {:go, ^runner}

    run = fn() ->
      _ = :timer.apply_after(100, Process, :exit, [self(), :shutdown])
      P.run(pool, fn(_) -> flunk("run ran") end)
    end
    Process.flag(:trap_exit, true)
    for task <- Enum.map(1..10, fn(_) -> Task.async(run) end) do
      assert catch_exit(Task.await(task)) ==
        {:shutdown, {Task, :await, [task, 5_000]}}
    end

    send(runner, {:done, self()})
    assert P.run(pool, fn(_) -> :result end) == :result
  end
end
