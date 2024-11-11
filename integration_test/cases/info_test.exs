defmodule InfoTest do
  use ExUnit.Case, async: true
  alias TestPool, as: P
  alias TestAgent, as: A

  test "handle_info handles message and moves on" do
    stack = [
      fn opts ->
        send(opts[:parent], {:connected, self()})
        {:ok, :state}
      end,
      :ok,
      {:idle, :state},
      {:idle, :state}
    ]

    {:ok, agent} = A.start_link(stack)
    {:ok, pool} = P.start_link(agent: agent, parent: self())

    assert_receive {:connected, conn}
    send(conn, "some harmless message")
    assert P.run(pool, fn _ -> :result end) == :result

    assert [
             connect: _,
             handle_info: _,
             handle_status: _,
             handle_status: _
           ] = A.record(agent)
  end

  test "handle_info can force disconnect" do
    stack = [
      fn opts ->
        send(opts[:parent], {:connected, self()})
        {:ok, :state}
      end,
      {:disconnect, :reason},
      :ok,
      fn opts ->
        send(opts[:parent], :reconnected)
        {:ok, :state}
      end
    ]

    {:ok, agent} = A.start_link(stack)
    P.start_link(agent: agent, parent: self())

    assert_receive {:connected, conn}
    send(conn, "some harmful message")
    assert_receive :reconnected

    assert [
             connect: _,
             handle_info: _,
             disconnect: _,
             connect: _
           ] = A.record(agent)
  end

  test "handle_info's disconnect with connection checked out"
end
