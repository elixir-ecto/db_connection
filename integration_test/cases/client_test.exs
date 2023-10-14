defmodule ClientTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestResult, as: R

  test "reconnect when client exits" do
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

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    _ =
      spawn(fn ->
        _ = Process.put(:agent, agent)

        P.run(pool, fn _ ->
          Process.exit(self(), :shutdown)
        end)
      end)

    assert_receive :reconnected
    assert P.run(pool, fn _ -> :result end) == :result

    assert [
             {:connect, _},
             {:handle_status, _},
             {:disconnect, _},
             {:connect, _},
             {:handle_status, _},
             {:handle_status, _}
           ] = A.record(agent)
  end

  test "reconnect when client timeout" do
    stack = [
      {:ok, :state},
      {:idle, :state},
      :ok,
      fn opts ->
        send(opts[:parent], :reconnected)
        {:ok, :state}
      end,
      {:idle, :state},
      {:idle, :state},
      {:idle, :state}
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    pid =
      spawn_link(fn ->
        _ = Process.put(:agent, agent)
        assert_receive {:go, ^parent}
        assert P.run(pool, fn _ -> :result end) == :result
        send(parent, {:done, self()})
      end)

    P.run(
      pool,
      fn conn ->
        assert_receive :reconnected

        assert {:error, %DBConnection.ConnectionError{}} = P.execute(conn, %Q{}, [:first])

        send(pid, {:go, parent})
        assert_receive {:done, ^pid}
      end,
      timeout: 100
    )

    assert [
             {:connect, _},
             {:handle_status, _},
             {:disconnect, _},
             {:connect, _},
             {:handle_status, _},
             {:handle_status, _}
           ] = A.record(agent)
  end

  test "reconnect when client timeout and then returns ok even when disconnected" do
    stack = [
      {:ok, :state},
      {:idle, :state},
      fn _, _, _, _ ->
        assert_receive :reconnected
        {:ok, %Q{}, %R{}, :new_state}
      end,
      :ok,
      fn opts ->
        send(opts[:parent], :reconnected)
        {:ok, :new_state}
      end,
      {:idle, :new_state},
      {:idle, :new_state},
      {:ok, %Q{}, %R{}, :newer_state}
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()

    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    assert P.run(
             pool,
             fn conn ->
               assert {:ok, %Q{}, %R{}} = P.execute(conn, %Q{}, [:first])

               spawn_link(fn ->
                 _ = Process.put(:agent, agent)
                 assert P.run(pool, fn _ -> :result end) == :result
                 send(parent, :done)
               end)

               :result
             end,
             timeout: 100
           ) == :result

    assert_receive :done
    assert P.execute(pool, %Q{}, [:second]) == {:ok, %Q{}, %R{}}

    assert [
             {:connect, _},
             {:handle_status, _},
             {:handle_execute, [%Q{}, [:first], _, :state]},
             {:disconnect, _},
             {:connect, _},
             {:handle_status, _},
             {:handle_status, _},
             {:handle_execute, [%Q{}, [:second], _, :new_state]}
           ] = A.record(agent)
  end

  test "reconnect when client timeout and then returns error when disconnected" do
    stack = [
      {:ok, :state},
      {:idle, :state},
      fn _, _, _, _ ->
        assert_receive :reconnected
        {:disconnect, DBConnection.ConnectionError.exception("oops"), :new_state}
      end,
      :ok,
      fn opts ->
        send(opts[:parent], :reconnected)
        {:ok, :new_state}
      end,
      {:idle, :new_state},
      {:idle, :new_state},
      {:ok, %Q{}, %R{}, :newer_state}
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()

    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    assert P.run(
             pool,
             fn conn ->
               message =
                 "oops (the connection was closed by the pool, possibly due to a timeout or because the pool has been terminated)"

               assert {:error, %DBConnection.ConnectionError{message: ^message}} =
                        P.execute(conn, %Q{}, [:first])

               spawn_link(fn ->
                 _ = Process.put(:agent, agent)
                 assert P.run(pool, fn _ -> :result end) == :result
                 send(parent, :done)
               end)

               :result
             end,
             timeout: 100
           ) == :result

    assert_receive :done
    assert P.execute(pool, %Q{}, [:second]) == {:ok, %Q{}, %R{}}

    assert [
             {:connect, _},
             {:handle_status, _},
             {:handle_execute, [%Q{}, [:first], _, :state]},
             {:disconnect, _},
             {:connect, _},
             {:handle_status, _},
             {:handle_status, _},
             {:handle_execute, [%Q{}, [:second], _, :new_state]}
           ] = A.record(agent)
  end

  test "reconnect when client timeout and then crashes" do
    stack = [
      {:ok, :state},
      {:idle, :state},
      fn _, _, _, _ ->
        throw(:oops)
      end,
      :ok,
      fn opts ->
        send(opts[:parent], :reconnected)
        {:ok, :new_state}
      end,
      {:idle, :new_state},
      {:idle, :new_state},
      {:ok, %Q{}, %R{}, :newer_state}
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()

    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    try do
      P.run(
        pool,
        fn conn ->
          spawn_link(fn ->
            _ = Process.put(:agent, agent)
            assert P.run(pool, fn _ -> :result end) == :result
            send(parent, :done)
          end)

          P.execute(conn, %Q{}, [:first])
        end,
        timeout: 100
      )
    catch
      :throw, :oops ->
        :ok
    end

    assert_receive :done
    assert P.execute(pool, %Q{}, [:second]) == {:ok, %Q{}, %R{}}

    assert [
             {:connect, _},
             {:handle_status, _},
             {:handle_execute, [%Q{}, [:first], _, :state]},
             {:disconnect, _},
             {:connect, _},
             {:handle_status, _},
             {:handle_status, _},
             {:handle_execute, [%Q{}, [:second], _, :new_state]}
           ] = A.record(agent)
  end

  test "fails when using an outdated connection reference" do
    stack = [
      {:ok, :state},
      {:idle, :state},
      {:idle, :state},
      {:idle, :new_state},
      {:idle, :new_state}
    ]

    {:ok, agent} = A.start_link(stack)
    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    outdated_conn = P.run(pool, fn conn -> conn end)

    assert_raise RuntimeError, ~r"an outdated connection has been given to DBConnection", fn ->
      P.execute(outdated_conn, %Q{}, [:first])
    end

    P.run(pool, fn _ ->
      assert_raise RuntimeError, ~r"an outdated connection has been given to DBConnection", fn ->
        P.execute(outdated_conn, %Q{}, [:first])
      end
    end)

    assert [
             {:connect, _},
             {:handle_status, _},
             {:handle_status, _},
             {:handle_status, _},
             {:handle_status, _}
           ] = A.record(agent)
  end
end
