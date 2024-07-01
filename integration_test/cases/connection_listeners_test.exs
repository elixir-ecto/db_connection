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

  test "send `:connected` message with tag after connected" do
    stack = [
      {:ok, :state}
    ]

    {:ok, agent} = A.start_link(stack)

    tag_ref = make_ref()

    opts = [
      agent: agent,
      parent: self(),
      connection_listeners: {[self()], tag_ref},
      backoff_min: 1_000
    ]

    {:ok, _pool} = P.start_link(opts)

    assert_receive {:connected, conn, ^tag_ref}
    assert is_pid(conn)

    refute_receive {:connected, _conn}
    refute_receive {:connected, _conn, _tag}
    refute_receive {:disconnected, _conn}
    refute_receive {:disconnected, _conn, _tag}
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

  test "send `:disconnected` message with tag after disconnect" do
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

    tag_ref = make_ref()

    opts = [
      agent: agent,
      parent: self(),
      connection_listeners: {[self()], tag_ref},
      backoff_min: 1_000
    ]

    {:ok, pool} = P.start_link(opts)

    assert P.close(pool, %Q{})
    assert_receive {:hi1, conn}
    assert_receive {:connected, ^conn, ^tag_ref}
    assert_receive {:disconnected, ^conn, ^tag_ref}
    refute_receive {:connected, _conn}
    refute_receive {:connected, _conn, _tag}
    refute_receive {:disconnected, _conn}
    refute_receive {:disconnected, _conn, _tag}
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

  describe "telemetry listener" do
    test "emits events with no tag" do
      attach_telemetry_forwarding_handler()
      err = RuntimeError.exception("oops")

      stack = [
        {:ok, :state},
        {:disconnect, err, :discon},
        :ok,
        {:error, err}
      ]

      {:ok, agent} = A.start_link(stack)
      {:ok, telemetry_listener} = DBConnection.TelemetryListener.start_link()

      {:ok, pool} =
        P.start_link(
          agent: agent,
          parent: self(),
          connection_listeners: [telemetry_listener],
          backoff_min: 1_000
        )

      assert_receive {:telemetry, :connected, %{tag: nil}}
      assert P.close(pool, %Q{})
      assert_receive {:telemetry, :disconnected, %{tag: nil}}
    after
      detach_telemetry_forwarding_handler()
    end

    test "emits events with tag" do
      attach_telemetry_forwarding_handler()
      err = RuntimeError.exception("oops")

      stack = [
        {:ok, :state},
        {:disconnect, err, :discon},
        :ok,
        {:error, err}
      ]

      {:ok, agent} = A.start_link(stack)
      {:ok, telemetry_listener} = DBConnection.TelemetryListener.start_link()

      tag = make_ref()

      {:ok, pool} =
        P.start_link(
          agent: agent,
          parent: self(),
          connection_listeners: {[telemetry_listener], tag},
          backoff_min: 1_000
        )

      assert_receive {:telemetry, :connected, %{tag: ^tag}}
      assert P.close(pool, %Q{})
      assert_receive {:telemetry, :disconnected, %{tag: ^tag}}
    after
      detach_telemetry_forwarding_handler()
    end

    test "handles non-graceful disconnects" do
      attach_telemetry_forwarding_handler()

      stack = [
        fn opts ->
          send(opts[:parent], {:hi, self()})
          {:ok, :state}
        end,
        {:ok, :state}
      ]

      {:ok, agent} = A.start_link(stack)
      {:ok, telemetry_listener} = DBConnection.TelemetryListener.start_link()

      {:ok, _pool} =
        P.start_link(
          agent: agent,
          parent: self(),
          connection_listeners: [telemetry_listener],
          backoff_min: 1_000
        )

      assert_receive {:hi, pid}
      Process.exit(pid, :kill)

      assert_receive {:telemetry, :disconnected, %{pid: ^pid}}
    after
      detach_telemetry_forwarding_handler()
    end
  end

  defp attach_telemetry_forwarding_handler() do
    test_pid = self()

    :telemetry.attach_many(
      "TestHandler",
      [
        [:db_connection, :connected],
        [:db_connection, :disconnected]
      ],
      fn [:db_connection, action], _, metadata, _ ->
        send(test_pid, {:telemetry, action, metadata})
      end,
      %{}
    )
  end

  defp detach_telemetry_forwarding_handler() do
    :telemetry.detach("TestHandler")
  end
end
