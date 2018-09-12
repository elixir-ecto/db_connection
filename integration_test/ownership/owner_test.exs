defmodule OwnerTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias DBConnection.Ownership

  test "reconnects when owner exits during a client checkout" do
    stack = [
      {:ok, :state},
      {:idle, :state},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :state}
      end,
      {:idle, :state}]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), ownership_mode: :manual]
    {:ok, pool} = P.start_link(opts)

    parent = self()
    {:ok, owner} = Task.start(fn() ->
      :ok = Ownership.ownership_checkout(pool, [])
      :ok = Ownership.ownership_allow(pool, self(), parent, [])
      send parent, :allowed
      :timer.sleep(:infinity)
    end)

    assert_receive :allowed

    assert P.run(pool, fn(_) ->
      Process.exit(owner, :shutdown)
      assert_receive :reconnected
      :ok
    end) == :ok

    assert [
      {:connect, _},
      {:handle_status, _},
      {:disconnect, _},
      {:connect, _},
      {:handle_status, _}] = A.record(agent)
  end

  test "reconnects when ownership times out" do
    stack = [
      {:ok, :state},
      {:idle, :state},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :state}
      end,
      {:idle, :state},
      {:idle, :state},
      {:idle, :state}]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    pid = spawn_link(fn() ->
      assert_receive {:go, ^parent}
      assert P.run(pool, fn(_) -> :result end, opts) == :result
      send(parent, {:done, self()})
    end)

    :ok = Ownership.ownership_checkout(pool, [ownership_timeout: 100])

    P.run(pool, fn(_) ->
      assert_receive :reconnected
      send(pid, {:go, parent})
      assert_receive {:done, ^pid}
    end)

    assert [
      {:connect, _},
      {:handle_status, _},
      {:disconnect, _},
      {:connect, _},
      {:handle_status, _},
      {:handle_status, _},
      {:handle_status, _}] = A.record(agent)
  end
end
