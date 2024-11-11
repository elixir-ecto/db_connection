defmodule InfoTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q

  test "handle_info handles harmless message and moves on" do
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
      {:disconnect, RuntimeError.exception("TCP connection just closed")},
      :ok,
      fn opts ->
        send(opts[:parent], :reconnected)
        {:ok, :state}
      end
    ]

    {:ok, agent} = A.start_link(stack)
    P.start_link(agent: agent, parent: self())

    assert_receive {:connected, conn}
    send(conn, "monitor says TCP connection just closed")
    assert_receive :reconnected

    assert [
             connect: _,
             handle_info: _,
             disconnect: _,
             connect: _
           ] = A.record(agent)
  end

  test "handle_info's disconnect while checked out client crashes is no-op" do
    stack = [
      fn _opts ->
        {:ok, %{conn_pid: self()}}
      end,
      fn _query, _params, _opts, %{conn_pid: conn_pid} ->
        send(conn_pid, "monitor says TCP connection just closed")

        # This waits for the info message to be processed.
        :sys.get_state(conn_pid)

        {:disconnect, RuntimeError.exception("TCP connection is closed"), :new_state}
      end,
      {:disconnect, RuntimeError.exception("TCP connection just closed")},
      :ok,
      fn opts ->
        send(opts[:parent], :reconnected)
        {:ok, :state}
      end
    ]

    {:ok, agent} = A.start_link(stack)
    {:ok, pool} = P.start_link(agent: agent, parent: self())

    assert {:error, %RuntimeError{message: "TCP connection is closed"}} =
             P.execute(pool, %Q{}, [:first])

    assert_receive :reconnected

    assert [
             connect: _,
             handle_execute: _,
             handle_info: _,
             disconnect: _,
             connect: _
           ] = A.record(agent)
  end
end
