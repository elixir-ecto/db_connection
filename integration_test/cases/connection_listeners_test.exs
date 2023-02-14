defmodule ConnectionListenersTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q

  test "send `:connected` message after connected" do
    stack = [
      {:ok, :state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), connection_listeners: [self()], backoff_min: 1_000]
    {:ok, _pool} = P.start_link(opts)

    assert_receive {:connected, conn}
    assert is_pid(conn)

    refute_receive {:connected, _conn}
    refute_receive {:disconnected, _conn}
  end

  test "send `:connected` message after connected with `after_connect` function" do
    stack = [
      {:ok, :state},
      {:idle, :state},
      :ok,
      {:ok, :state2},
      {:idle, :state},
      :ok
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()

    after_connect = fn _ ->
      send(parent, :after_connect)
      :ok
    end

    opts = [
      after_connect: after_connect,
      agent: agent,
      parent: self(),
      connection_listeners: [self()],
      backoff_min: 1_000
    ]

    {:ok, _pool} = P.start_link(opts)

    assert_receive {:connected, _conn}
    assert_receive :after_connect
  end

  test "send `:disconnected` message after disconnect" do
    err = RuntimeError.exception("oops")

    stack = [
      fn opts ->
        send(opts[:parent], {:hi1, self()})
        {:ok, :state}
      end,
      {:disconnect, err, :discon},
      :ok,
      {:error, err}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), connection_listeners: [self()], backoff_min: 1_000]
    {:ok, pool} = P.start_link(opts)

    assert P.close(pool, %Q{})
    assert_receive {:hi1, conn}
    assert_receive {:connected, ^conn}
    assert_receive {:disconnected, ^conn}
    refute_receive {:connected, _conn}
    refute_receive {:disconnected, _conn}
  end

  test "send `:connected` message after disconnect then reconnected" do
    err = RuntimeError.exception("oops")

    stack = [
      fn opts ->
        send(opts[:parent], {:hi1, self()})
        {:ok, :state}
      end,
      {:disconnect, err, :discon},
      :ok,
      fn opts ->
        send(opts[:parent], {:hi2, self()})
        {:ok, :reconnected}
      end
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), connection_listeners: [self()], backoff_min: 1_000]
    {:ok, pool} = P.start_link(opts)

    assert P.close(pool, %Q{})
    assert_receive {:hi1, conn}
    assert_receive {:connected, ^conn}
    assert_receive {:disconnected, ^conn}
    assert_receive {:hi2, ^conn}
    assert_receive {:connected, ^conn}
    refute_receive {:disconnected, _conn}
    refute_receive {:connected, _conn}
  end

  test "send `:connected` message after each connection in pool has connected" do
    stack = [
      {:ok, :state},
      {:ok, :state},
      {:ok, :state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [
      agent: agent,
      parent: self(),
      connection_listeners: [self()],
      pool_size: 3,
      backoff_min: 1_000
    ]

    {:ok, _pool} = P.start_link(opts)

    assert_receive {:connected, conn1}
    assert_receive {:connected, conn2}
    assert_receive {:connected, conn3}
    refute_receive {:connected, _conn}
    assert is_pid(conn1)
    assert is_pid(conn2)
    assert is_pid(conn3)
    refute conn1 == conn2 == conn3
  end
end
