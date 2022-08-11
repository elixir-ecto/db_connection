defmodule QueueTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A

  test "queue: false raises on busy" do
    stack = [{:ok, :state}, {:idle, :state}, {:idle, :state}]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    P.run(pool, fn(_) ->
      {queue_time, _} = :timer.tc(fn() ->
        opts = [queue: false]
        assert_raise DBConnection.ConnectionError,
          ~r"connection not available and queuing is disabled",
          fn() -> P.run(pool, fn(_) -> flunk("got connection") end, opts) end
      end)
      assert queue_time <= 1_000_000, "request was queued"
    end)
  end

  test "queue many async" do
    stack = [{:ok, :state}] ++ List.duplicate({:idle, :state}, 20)
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    run = fn() ->
      Process.put(:agent, agent)
      P.run(pool, fn(_) -> :timer.sleep(20) end, opts)
    end

    for task <- Enum.map(1..10, fn(_) -> Task.async(run) end) do
      assert :ok = Task.await(task)
    end
  end

  test "queue many async timeouts" do
    stack = [{:ok, :state}] ++ List.duplicate({:idle, :state}, 20)
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), queue_timeout: 50, queue_target: 50,
      queue_interval: 50]
    {:ok, pool} = P.start_link(opts)

    parent = self()
    runner = spawn_link(fn() ->
      Process.put(:agent, agent)
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
        P.run(pool, fn(_) -> flunk("run ran") end, [timeout: 50])
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
    stack = [{:ok, :state}] ++ List.duplicate({:idle, :state}, 20)
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    parent = self()
    runner = spawn_link(fn() ->
      Process.put(:agent, agent)
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

  @tag :dequeue_disconnected
  test "queue raises dropped from queue when disconnected" do
    stack = [{:error, RuntimeError.exception("oops")}]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), backoff_start: 30_000]
    {:ok, pool} = P.start_link(opts)

    {queue_time, _} = :timer.tc(fn() ->
      opts = [queue: false]
      assert_raise DBConnection.ConnectionError,
        ~r"connection not available and queuing is disabled",
        fn() -> P.run(pool, fn(_) -> flunk("got connection") end, opts) end
    end)
    assert queue_time <= 1_000_000, "request was queued"
  end

  @tag :queue_timeout_raise
  test "queue raise on timeout" do
    stack = [{:ok, :state}, {:idle, :state}, {:idle, :state}, {:idle, :state}, {:idle, :state}]
    {:ok, agent} = A.start_link(stack)

    test_pid = self()

    opts = [agent: agent, parent: test_pid, backoff_start: 30_000,
      queue_timeout: 10, queue_target: 10, queue_interval: 10, connection_listeners: [test_pid]
    ]

    event = [:db_connection, :connection_error]

    :telemetry.attach(
      "queue-timeout-handler",
      event,
      fn event, measurements, metadata, _ ->
        send test_pid, {:event, event, measurements, metadata}
      end,
      nil
    )

    {:ok, pool} = P.start_link(opts)

    P.run(pool, fn(_) ->
      exception =
        assert_raise DBConnection.ConnectionError,
          ~r"connection not available and request was dropped from queue after \d+ms",
          fn() -> P.run(pool, fn(_) -> flunk("got connection") end, opts) end

      assert exception.reason == :queue_timeout
    end)

    assert P.run(pool, fn(_) -> :hi end) == :hi

    assert_receive {
      :event,
      ^event,
      %{count: 1},
      %{
        error: %DBConnection.ConnectionError{reason: :queue_timeout},
        opts: event_opts
      }
    }

    assert opts == Enum.take(event_opts, length(opts))
  end

  test "queue handles holder that has been deleted" do
    stack = [{:ok, :state}, {:idle, :state}, {:idle, :state}, :ok, {:ok, :new_state}, {:idle, :new_state}, {:idle, :new_state}]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    P.run(pool, fn(_) ->
      :sys.suspend(pool)
      :timer.sleep(1000)
      :sys.resume(pool)
    end, [timeout: 100])

    P.run(pool, fn(_) -> :ok end)
  end
end
