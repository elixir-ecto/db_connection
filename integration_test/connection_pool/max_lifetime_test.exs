defmodule MaxLifetimeTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A

  test "disconnects and reconnects when idle ping fires after max_lifetime" do
    stack = [
      {:ok, :state},
      :ok,
      {:ok, :state},
      fn _, _ -> Process.sleep(:infinity) end
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [
      agent: agent,
      parent: self(),
      connection_listeners: [self()],
      max_lifetime: 100..100,
      idle_interval: 200,
      backoff_min: 10
    ]

    {:ok, _pool} = P.start_link(opts)

    assert_receive {:connected, conn}
    assert_receive {:disconnected, ^conn}
    assert_receive {:connected, ^conn}
  end

  test "disconnects and reconnects after a long checkout exceeds max_lifetime" do
    stack = [
      {:ok, :state},
      {:idle, :state},
      {:idle, :state},
      :ok,
      {:ok, :state},
      fn _, _ -> Process.sleep(:infinity) end
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [
      agent: agent,
      parent: self(),
      connection_listeners: [self()],
      max_lifetime: 50..50
    ]

    {:ok, pool} = P.start_link(opts)
    assert_receive {:connected, conn}
    assert P.run(pool, fn _conn -> Process.sleep(500) end)
    assert_receive {:disconnected, ^conn}
    assert_receive {:connected, ^conn}
  end
end
