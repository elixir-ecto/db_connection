defmodule BackoffTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A

  test "backoff after failed initial connection attempt" do
    agent = spawn_agent_backoff_after_failed()
    opts = [agent: agent, parent: self(), backoff_min: 10]
    execute_test_backoff_after_failed(agent, opts)
  end

  @tag :idle_hibernate_backoff
  test "backoff after failed initial connection attempt with idle_hibernate" do
    agent = spawn_agent_backoff_after_failed()
    opts = [agent: agent, parent: self(), backoff_min: 10, idle_hibernate: true]
    execute_test_backoff_after_failed(agent, opts, true)
  end

  defp spawn_agent_backoff_after_failed() do
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
    agent
  end

  defp execute_test_backoff_after_failed(agent, opts, test_hibernate? \\ false) do
    {:ok, _} = P.start_link(opts)
    assert_receive {:error, conn}
    assert_receive {:hi, ^conn}

    if test_hibernate? do
      assert {:current_function, {:erlang, :hibernate, 3}} ==
        Process.info(conn, :current_function)
    end

    assert [
      connect: [opts2],
      connect: [opts2]] = A.record(agent)
  end

  test "backoff after disconnect and failed connection attempt" do
    err = RuntimeError.exception("oops")
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi1, self()})
        send(self(), :hello)
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

    opts = [agent: agent, parent: self(), backoff_min: 10]
    {:ok, _} = P.start_link(opts)
    assert_receive {:hi1, conn}
    assert_receive {:hi2, ^conn}

    assert [
      connect: [opts2],
      handle_info: [:hello, :state],
      disconnect: [^err, :discon],
      connect: [opts2],
      connect: [opts2]] = A.record(agent)
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

    assert [{:connect, _} | _] = A.record(agent)
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
      :ok,
      {:ok, :state}]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), backoff_type: :stop]
    Process.flag(:trap_exit, true)
    {:ok, _} = P.start_link(opts)
    assert_receive {:hi, conn}
    assert_receive {:EXIT, ^conn, {:shutdown, ^err}}

    assert [
      {:connect, [_]},
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
    opts = [after_connect: after_connect, agent: agent, parent: self()]
    {:ok, _} = P.start_link(opts)

    assert_receive :after_connect
    refute_receive :after_connect, 50
    assert_receive :after_connect, 500

    assert [
      {:connect, [_]},
      {:disconnect, [%DBConnection.ConnectionError{}, :state]},
      {:connect, [_]} | _] = A.record(agent)
  end
end
