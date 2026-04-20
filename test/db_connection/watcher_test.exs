defmodule DBConnection.WatcherTest do
  use ExUnit.Case, async: true

  alias TestConnection, as: C
  alias TestAgent, as: A

  test "starting a new pool is not blocked by a slow-to-terminate pool" do
    {:ok, agent1} =
      A.start_link([
        {:ok, :state},
        fn _err, _state ->
          Process.sleep(10_000)
          :ok
        end
      ])

    pool1 =
      start_supervised!(%{
        id: :pool1,
        start:
          {DBConnection, :start_link,
           [
             TestConnection,
             [agent: agent1, pool_size: 1, idle_interval: 100_000, shutdown: 2_500]
           ]},
        restart: :temporary
      })

    # Find the pool sup for pool1 by matching the owner pid in child spec ids.
    pool_sup_ref =
      DBConnection.ConnectionPool.Supervisor
      |> DynamicSupervisor.which_children()
      |> Enum.find_value(fn {_, sup_pid, _, _} ->
        try do
          has_pool1? =
            sup_pid
            |> Supervisor.which_children()
            |> Enum.any?(fn {{_mod, owner, _id}, _, _, _} -> owner == pool1 end)

          if has_pool1?, do: sup_pid
        catch
          :exit, _ -> nil
        end
      end)
      |> Process.monitor()

    # Trigger termination of pool1 by stopping it (the caller).
    # This sends a :DOWN to the Watcher, which will try to terminate the pool.
    stop_supervised!(:pool1)

    # Now try to start a second pool. If the Watcher is blocked by the
    # synchronous DynamicSupervisor.terminate_child call, this will hang.
    {:ok, agent2} = A.start_link([{:ok, :state}])

    task =
      Task.async(fn ->
        C.start_link(agent: agent2, pool_size: 1, idle_interval: 100_000)
      end)

    assert {:ok, _pool2} = Task.await(task, 3_000)

    # Ensure the pool supervisor actually terminates
    assert_receive {:DOWN, ^pool_sup_ref, _, _, _}, 10_000
  end
end
