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
        assert_raise DBConnection.ConnectionError,
          "connection not available and queuing is disabled",
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

    run = fn() ->
      P.run(pool, fn(_) -> :timer.sleep(20) end)
    end

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
        DBConnection.ConnectionError ->
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

  @tag :enqueue_disconnected
  test "queue raises disconnect error when disconnected" do
    stack = [{:error, RuntimeError.exception("oops")}]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), backoff_start: 30_000]
    {:ok, pool} = P.start_link(opts)

    {queue_time, _} = :timer.tc(fn() ->
      opts = [queue: false]
      assert_raise DBConnection.ConnectionError,
      "connection not available because of disconnection",
        fn() -> P.run(pool, fn(_) -> flunk("got connection") end, opts) end
    end)
    assert queue_time <= 1_000_000, "request was queued"
  end

  @tag :dequeue_disconnected
  test "queue raises dropped from queue when disconnected" do
    stack = [{:error, RuntimeError.exception("oops")}]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), backoff_start: 30_000]
    {:ok, pool} = P.start_link(opts)

    {queue_time, _} = :timer.tc(fn() ->
      opts = [queue: false]
      assert_raise DBConnection.ConnectionError,
        "connection not available and queuing is disabled",
        fn() -> P.run(pool, fn(_) -> flunk("got connection") end, opts) end
    end)
    assert queue_time <= 1_000_000, "request was queued"
  end

  @tag :queue_timeout_exit
  test "queue exits on timeout" do
    stack = [{:ok, :state}]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), backoff_start: 30_000,
      queue_timeout: 10, pool_timeout: 10]
    {:ok, pool} = P.start_link(opts)

    P.run(pool, fn(_) ->
      assert {:timeout, {_, _, _}} = catch_exit(P.run(pool, fn() ->
        flunk("got connection")
      end, opts))
    end)

    assert P.run(pool, fn(_) -> :hi end) == :hi
  end

  @tag :queue_timeout_raise
  test "queue raise on timeout" do
    stack = [{:ok, :state}]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), backoff_start: 30_000,
      queue_timeout: 10, pool_timeout: 10]
    {:ok, pool} = P.start_link(opts)

    P.run(pool, fn(_) ->
      assert_raise DBConnection.ConnectionError,
        ~r"^connection not available and request was dropped from queue after \d+ms$",
        fn() -> P.run(pool, fn(_) -> flunk("got connection") end, opts) end
    end)

    assert P.run(pool, fn(_) -> :hi end) == :hi
  end
end
