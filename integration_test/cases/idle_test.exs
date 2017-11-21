defmodule TestIdle do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A

  @tag :idle_timeout
  test "ping after idle timeout" do
    agent = spawn_agent()
    opts = [agent: agent, parent: self(), idle_timeout: 50]
    execute_test_case(agent, opts)
  end

  @tag :idle_timeout
  test "ping after idle timeout using hibernate" do
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
    if test_hibernate? do
      assert {:current_function, {:erlang, :hibernate, 3}} ==
        Process.info(conn, :current_function)
    end
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
  end

end
