defmodule OwnerTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias DBConnection.Ownership

  import ExUnit.CaptureLog

  test "reconnects when owner exits during a client checkout" do
    stack = [
      {:ok, :state},
      {:idle, :state},
      :ok,
      fn opts ->
        send(opts[:parent], :reconnected)
        {:ok, :state}
      end
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), ownership_mode: :manual]
    {:ok, pool} = P.start_link(opts)

    parent = self()

    {:ok, owner} =
      Task.start(fn ->
        :ok = Ownership.ownership_checkout(pool, [])
        :ok = Ownership.ownership_allow(pool, self(), parent, [])
        send(parent, :allowed)
        Process.sleep(:infinity)
      end)

    assert_receive :allowed

    log =
      capture_log(fn ->
        assert P.run(pool, fn _ ->
                 Process.exit(owner, :shutdown)
                 assert_receive :reconnected
                 :ok
               end) == :ok
      end)

    assert log =~ ~r"owner #PID<\d+\.\d+\.\d+> exited"

    assert log =~
             ~r"Client #PID<\d+\.\d+\.\d+> is still using a connection from owner at location"

    assert log =~ ~r"The connection itself was checked out by #PID<\d+\.\d+\.\d+> at location"

    assert [
             {:connect, _},
             {:handle_status, _},
             {:disconnect, _},
             {:connect, _}
           ] = A.record(agent)
  end

  test "reconnects when ownership times out" do
    stack = [
      {:ok, :state},
      {:idle, :state},
      :ok,
      fn opts ->
        send(opts[:parent], :reconnected)
        {:ok, :state}
      end,
      {:idle, :state},
      {:idle, :state}
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    pid =
      spawn_link(fn ->
        assert_receive {:go, ^parent}
        assert P.run(pool, fn _ -> :result end, opts) == :result
        send(parent, {:done, self()})
      end)

    :ok = Ownership.ownership_checkout(pool, ownership_timeout: 100)

    assert capture_log(fn ->
             P.run(pool, fn _ ->
               assert_receive :reconnected
               send(pid, {:go, parent})
               assert_receive {:done, ^pid}
             end)
           end) =~
             ~r"owner #PID<\d+\.\d+\.\d+> timed out because it owned the connection for longer than 100ms"

    assert [
             {:connect, _},
             {:handle_status, _},
             {:disconnect, _},
             {:connect, _},
             {:handle_status, _},
             {:handle_status, _}
           ] = A.record(agent)
  end

  test "reconnects when client times out through owner" do
    stack = [
      {:ok, :state},
      {:idle, :state},
      :ok,
      fn opts ->
        send(opts[:parent], :reconnected)
        {:ok, :state}
      end
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    :ok = Ownership.ownership_checkout(pool, [])

    assert capture_log(fn ->
             P.run(
               pool,
               fn _ ->
                 assert_receive :reconnected
               end,
               timeout: 0
             )
           end) =~ ~r"client #PID<\d+\.\d+\.\d+> timed out"

    assert [
             {:connect, _},
             {:handle_status, _},
             {:disconnect, _},
             {:connect, _}
           ] = A.record(agent)
  end
end
