defmodule ClientTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A

  test "reconnect when client exits" do
    stack = [
      {:ok, :state},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :state}
      end]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    _ = spawn(fn() ->
      P.run(pool, fn(_) ->
        Process.exit(self(), :shutdown)
      end)
    end)

    assert_receive :reconnected
    assert P.run(pool, fn(_) -> :result end) == {:ok, :result}

    assert [
      {:connect, _},
      {:disconnect, _},
      {:connect, _}] = A.record(agent)
  end

  test "reconnect when client timesout" do
    stack = [
      {:ok, :state},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :state}
      end]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    _ = spawn(fn() ->
      P.run(pool, fn(_) -> :timer.sleep(200) end, [timeout: 10])
    end)

    assert_receive :reconnected
    assert P.run(pool, fn(_) -> :result end) == {:ok, :result}

    assert [
      {:connect, _},
      {:disconnect, _},
      {:connect, _}] = A.record(agent)
  end
end

