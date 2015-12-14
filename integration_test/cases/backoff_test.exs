defmodule BackoffTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A

  test "backoff after failed initial connection attempt" do
    err = RuntimeError.exception("oops")
    stack = [
      fn(opts) ->
        send(opts[:parent], {:error, self()})
        {:error, err}
      end,
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        {:ok, :state}
      end]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), backoff_start: 10,
            backoff_type: :normal]
    {:ok, _} = P.start_link(opts)
    assert_receive {:error, conn}
    assert_receive {:hi, ^conn}

    assert [
      connect: [[_, _ | ^opts]],
      connect: [[_, _ | ^opts]]] = A.record(agent)
  end

  test "backoff after disconnect and failed connection attempt" do
    err = RuntimeError.exception("oops")
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi1, self()})
        send(self, :hello)
        {:ok, :state}
      end,
      {:disconnect, err, :discon},
      :ok,
      {:error, err},
      fn(opts) ->
        send(opts[:parent], {:hi2, self()})
        {:ok, :reconnected}
      end]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), backoff_start: 10,
            backoff_type: :jitter]
    {:ok, _} = P.start_link(opts)
    assert_receive {:hi1, conn}
    assert_receive {:hi2, ^conn}

    assert [
      connect: [[_, _ | ^opts]],
      handle_info: [:hello, :state],
      disconnect: [^err, :discon],
      connect: [[_, _ | ^opts]],
      connect: [[_, _ | ^opts]]] = A.record(agent)
  end

  test "backoff :stop exits on failed initial connection attempt" do
    err = RuntimeError.exception("oops")
    stack = [
      fn(opts) ->
        send(opts[:parent], {:error, self()})
        Process.link(opts[:parent])
        {:error, err}
      end,
      {:ok, :state}]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), backoff_type: :stop]
    Process.flag(:trap_exit, true)
    {:ok, _} = P.start_link(opts)
    assert_receive {:error, conn}
    assert_receive {:EXIT, ^conn, {^err, _}}

    assert [{:connect, [[_, _ | ^opts]]} | _] = A.record(agent)
  end

  test "backoff :stop exits after disconnect without attempting to connect" do
    err = RuntimeError.exception("oops")
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        send(self(), :hello)
        {:ok, :state}
      end,
      {:disconnect, err, :discon},
      {:ok, :state}]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), backoff_type: :stop]
    Process.flag(:trap_exit, true)
    {:ok, _} = P.start_link(opts)
    assert_receive {:hi, conn}
    assert_receive {:EXIT, ^conn, {^err, _}}

    assert [
      {:connect, [[_, _ | ^opts]]},
      {:handle_info, [:hello, :state]} | _] = A.record(agent)
  end

  test "backoff after failed after_connect" do
    stack = [
      {:ok, :state},
      :ok,
      {:ok, :state2},
      :ok
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    after_connect = fn(_) ->
      send(parent, :after_connect)
      Process.exit(self(), :shutdown)
    end
    opts = [after_connect: after_connect, agent: agent, parent: self(),
            backoff_type: :normal]
    {:ok, _} = P.start_link(opts)

    assert_receive :after_connect
    refute_receive :after_connect, 50
    assert_receive :after_connect, 500

    assert [
      {:connect, [_]},
      {:disconnect, [%DBConnection.Error{}, :state]},
      {:connect, [_]} | _] = A.record(agent)
  end
end
