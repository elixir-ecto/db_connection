defmodule DBConnection.WatcherTest do
  use ExUnit.Case, async: true

  alias TestConnection, as: C
  alias TestAgent, as: A

  test "starting a new pool is not blocked by a slow-to-terminate pool" do
    # Start a pool whose connection takes a long time to disconnect.
    # The disconnect callback sleeps for 10 seconds, simulating a slow shutdown.
    {:ok, agent1} =
      A.start_link([
        {:ok, :state},
        fn _err, _state ->
          Process.sleep(10_000)
          :ok
        end
      ])

    start_supervised!(
      %{
        id: :pool1,
        start: {DBConnection, :start_link, [TestConnection, [agent: agent1, pool_size: 1]]},
        restart: :temporary
      }
    )

    # Trigger termination of pool1 by stopping it (the caller).
    # This sends a :DOWN to the Watcher, which will try to terminate the pool.
    stop_supervised!(:pool1)

    # Give the Watcher a moment to receive the :DOWN and start terminating pool1
    Process.sleep(100)

    # Now try to start a second pool. If the Watcher is blocked by the
    # synchronous DynamicSupervisor.terminate_child call, this will hang.
    {:ok, agent2} = A.start_link([{:ok, :state}])

    task =
      Task.async(fn ->
        C.start_link(agent: agent2, pool_size: 1)
      end)

    assert {:ok, _pool2} = Task.await(task, 3_000)
  end
end
