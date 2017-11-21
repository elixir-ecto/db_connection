defmodule TestIdle do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A

  @ets_table_name_for_test :ets_for_test_hibernate

  @tag :idle_timeout
  test "ping after idle timeout" do
    agent = spawn_agent()
    opts = [agent: agent, parent: self(), idle_timeout: 50]
    execute_test_case(agent, opts)
  end

  @tag :idle_timeout
  test "ping after idle timeout using hibernate" do
    create_ets_for_test()
    agent = spawn_agent()
    opts = [agent: agent, parent: self(), idle_timeout: 50, idle_hibernate: true]
    execute_test_case(agent, opts, true)
  end

  defp spawn_agent() do
    parent = self()
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        {:ok, :state}
      end,
      fn(_) ->
        send(parent, {:pong, self()})
        :timer.sleep(10)
        {:ok, :state}
      end,
      fn(_) ->
        send(parent, {:pong, self()})
        assert_receive {:continue, ^parent}
        {:ok, :state}
      end,
      fn(_) ->
        send(parent, {:pong, self()})
        :timer.sleep(:infinity)
      end]
    {:ok, agent} = A.start_link(stack)
    agent
  end

  defp execute_test_case(agent, opts, test_hibernate? \\ false) do
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}
    assert_receive {:pong, ^conn}
    assert_receive {:pong, ^conn}
    send(conn, {:continue, self()})
    P.run(pool, fn(_) -> :ok end)
    assert_receive {:pong, ^conn}

    assert [
      connect: [_],
      ping: [:state],
      ping: [:state],
      ping: [:state]] = A.record(agent)

    if test_hibernate? and :sojourn != Mix.env() do
      assert [{:hibernate_times, _}] =
        :ets.lookup(@ets_table_name_for_test, :hibernate_times)
    end
  end

  defp create_ets_for_test() do
    case :ets.info(@ets_table_name_for_test) do
      :undefined ->
        :ets.new(@ets_table_name_for_test, [:named_table, :public])
        :ok
      _ ->
        :ets.delete_all_objects(@ets_table_name_for_test)
        :ok
    end
  end

end
