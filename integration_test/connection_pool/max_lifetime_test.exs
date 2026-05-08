defmodule MaxLifetimeTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A

  defmodule ManyPingsConnection do
    def connect(opts) do
      {:ok, %{parent: Keyword.fetch!(opts, :parent), pings: 0}}
    end

    def disconnect(err, state) do
      send(state.parent, {:max_lifetime_disconnect, err, state.pings})
      :ok
    end

    def checkout(state), do: {:ok, state}
    def checkin(state), do: {:ok, state}

    def ping(state) do
      state = %{state | pings: state.pings + 1}
      send(state.parent, {:ping, state.pings})
      {:ok, state}
    end
  end

  test "disconnects and reconnects when idle ping fires before max_lifetime" do
    opts = [
      parent: self(),
      pool: DBConnection.ConnectionPool,
      pool_size: 1,
      connection_listeners: [self()],
      max_lifetime: 200..200,
      idle_interval: 50,
      backoff_min: 10
    ]

    {:ok, _pool} = DBConnection.start_link(ManyPingsConnection, opts)

    assert_receive {:connected, conn}
    assert_receive {:ping, 1}

    assert_receive {:max_lifetime_disconnect,
                    %DBConnection.ConnectionError{message: "max_lifetime exceeded"}, pings},
                   2_000

    assert pings > 0
    assert_receive {:disconnected, ^conn}
    assert_receive {:connected, ^conn}
  end

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
