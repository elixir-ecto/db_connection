defmodule AfterConnectTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestResult, as: R

  test "after_connect execute" do
    stack = [
      {:ok, :state},
      {:idle, :state},
      {:ok, %Q{}, %R{}, :new_state},
      {:ok, %Q{}, %R{}, :newer_state},
      {:idle, :newer_state},
      {:ok, %Q{}, %R{}, :newest_state}
    ]

    {:ok, agent} = A.start_link(stack)

    after_connect = fn conn ->
      _ = Process.put(:agent, agent)
      assert P.execute(conn, %Q{}, [:after_connect]) == {:ok, %Q{}, %R{}}
      assert P.execute(conn, %Q{}, [:after_connect], key: :value) == {:ok, %Q{}, %R{}}
      :ok
    end

    opts = [after_connect: after_connect, agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.execute(pool, %Q{}, [:client])

    assert [
             connect: [_],
             handle_status: _,
             handle_execute: [%Q{}, [:after_connect], _, :state],
             handle_execute: [%Q{}, [:after_connect], [{:key, :value} | _], :new_state],
             handle_status: _,
             handle_execute: [%Q{}, [:client], _, :newer_state]
           ] = A.record(agent)
  end

  test "after_connect execute with mfargs" do
    stack = [
      {:ok, :state},
      {:idle, :state},
      {:ok, %Q{}, %R{}, :new_state},
      {:ok, %Q{}, %R{}, :newer_state},
      {:idle, :newer_state},
      {:ok, %Q{}, %R{}, :newest_state}
    ]

    {:ok, agent} = A.start_link(stack)

    defmodule AfterCon do
      def after_connect(conn, :arg, agent) do
        _ = Process.put(:agent, agent)
        assert P.execute(conn, %Q{}, [:after_connect]) == {:ok, %Q{}, %R{}}
        assert P.execute(conn, %Q{}, [:after_connect], key: :value) == {:ok, %Q{}, %R{}}
        :ok
      end
    end

    after_connect = {AfterCon, :after_connect, [:arg, agent]}
    opts = [after_connect: after_connect, agent: agent, parent: self()]

    try do
      {:ok, pool} = P.start_link(opts)
      assert P.execute(pool, %Q{}, [:client])

      assert [
               connect: [_],
               handle_status: _,
               handle_execute: [%Q{}, [:after_connect], _, :state],
               handle_execute: [%Q{}, [:after_connect], [{:key, :value} | _], :new_state],
               handle_status: _,
               handle_execute: [%Q{}, [:client], _, :newer_state]
             ] = A.record(agent)
    after
      :code.purge(AfterConn)
      :code.delete(AfterConn)
    end
  end

  test "after_connect execute error returns error" do
    err = RuntimeError.exception("oops")

    stack = [
      {:ok, :state},
      {:idle, :state},
      {:error, err, :new_state},
      {:ok, %Q{}, %R{}, :newer_state},
      {:idle, :newer_state},
      {:ok, %Q{}, %R{}, :newest_state}
    ]

    {:ok, agent} = A.start_link(stack)

    after_connect = fn conn ->
      _ = Process.put(:agent, agent)
      assert P.execute(conn, %Q{}, [:after_connect]) == {:error, err}
      assert P.execute(conn, %Q{}, [:after_connect]) == {:ok, %Q{}, %R{}}
      :ok
    end

    opts = [after_connect: after_connect, agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.execute(pool, %Q{}, [:client])

    assert [
             connect: [_],
             handle_status: _,
             handle_execute: [%Q{}, [:after_connect], _, :state],
             handle_execute: [%Q{}, [:after_connect], _, :new_state],
             handle_status: _,
             handle_execute: [%Q{}, [:client], _, :newer_state]
           ] = A.record(agent)
  end

  test "after_connect execute disconnect returns error" do
    err = RuntimeError.exception("oops")

    stack = [
      {:ok, :state},
      {:idle, :state},
      {:disconnect, err, :new_state},
      :ok,
      {:ok, :state2},
      {:idle, :state2},
      {:ok, %Q{}, %R{}, :new_state2},
      {:idle, :new_state2},
      {:ok, %Q{}, %R{}, :newer_state2}
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()

    after_connect = fn conn ->
      send(parent, :after_connect)
      _ = Process.put(:agent, agent)

      case P.execute(conn, %Q{}, [:after_connect]) do
        {:error, ^err} ->
          assert_raise DBConnection.Connection.Error, "connection is closed", fn ->
            P.execute(conn, %Q{}, [:after_connect])
          end

          :ok

        {:ok, %Q{}, %R{}} ->
          :ok
      end
    end

    opts = [after_connect: after_connect, agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_receive :after_connect
    assert_receive :after_connect, 500
    assert P.execute(pool, %Q{}, [:client])

    assert [
             connect: [_],
             handle_status: _,
             handle_execute: [%Q{}, [:after_connect], _, :state],
             disconnect: [^err, :new_state],
             connect: [_],
             handle_status: _,
             handle_execute: [%Q{}, [:after_connect], _, :state2],
             handle_status: _,
             handle_execute: [%Q{}, [:client], _, :new_state2]
           ] = A.record(agent)
  end

  test "after_connect execute bad return raises DBConnection.ConnectionError" do
    stack = [
      fn opts ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:idle, :state},
      :oops,
      fn _ ->
        :timer.sleep(:infinity)
      end
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()

    after_connect = fn conn ->
      send(parent, {:after_connect, self()})
      _ = Process.put(:agent, agent)

      assert_raise DBConnection.ConnectionError, "bad return value: :oops", fn ->
        P.execute(conn, %Q{}, [:after_connect])
      end

      :ok
    end

    opts = [after_connect: after_connect, agent: agent, parent: parent]
    Process.flag(:trap_exit, true)
    {:ok, _} = P.start_link(opts)

    assert_receive {:hi, conn}

    assert_receive {:after_connect, after_pid}

    prefix =
      "client #{inspect(after_pid)} stopped: " <>
        "** (DBConnection.ConnectionError) bad return value: :oops"

    len = byte_size(prefix)

    assert_receive {:EXIT, ^conn,
                    {%DBConnection.ConnectionError{
                       message: <<^prefix::binary-size(len), _::binary>>
                     }, [_ | _]}}

    assert [
             {:connect, _},
             {:handle_status, _},
             {:handle_execute, [%Q{}, [:after_connect], _, :state]} | _
           ] = A.record(agent)
  end

  test "after_connect execute raise raises and stops connection" do
    stack = [
      fn opts ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:idle, :state},
      fn _, _, _, _ ->
        raise "oops"
      end,
      fn _ ->
        :timer.sleep(:infinity)
      end
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()

    after_connect = fn conn ->
      send(parent, {:after_connect, self()})
      _ = Process.put(:agent, agent)
      assert_raise RuntimeError, "oops", fn -> P.execute(conn, %Q{}, [:after_connect]) end
      :ok
    end

    opts = [after_connect: after_connect, agent: agent, parent: parent]
    Process.flag(:trap_exit, true)
    {:ok, _} = P.start_link(opts)

    assert_receive {:hi, conn}

    assert_receive {:after_connect, after_pid}
    prefix = "client #{inspect(after_pid)} stopped: ** (RuntimeError) oops"
    len = byte_size(prefix)

    assert_receive {:EXIT, ^conn,
                    {%DBConnection.ConnectionError{
                       message: <<^prefix::binary-size(len), _::binary>>
                     }, [_ | _]}}

    assert [
             {:connect, _},
             {:handle_status, _},
             {:handle_execute, [%Q{}, [:after_connect], _, :state]} | _
           ] = A.record(agent)
  end

  test "after_connect exit and reconnect" do
    stack = [
      {:ok, :state},
      {:idle, :state},
      fn _, _, _, _ ->
        Process.exit(self(), :shutdown)
      end,
      :ok,
      fn opts ->
        send(opts[:parent], :reconnected)
        {:ok, :state2}
      end,
      {:idle, :state2},
      {:ok, %Q{}, %R{}, :new_state2},
      {:idle, :new_state2},
      {:ok, %Q{}, %R{}, :newer_state2}
    ]

    {:ok, agent} = A.start_link(stack)

    after_connect = fn conn ->
      _ = Process.put(:agent, agent)
      assert P.execute(conn, %Q{}, [:after_connect]) == {:ok, %Q{}, %R{}}
      :ok
    end

    opts = [after_connect: after_connect, agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_receive :reconnected, 500
    assert P.execute(pool, %Q{}, [:client]) == {:ok, %Q{}, %R{}}

    assert [
             {:connect, _},
             {:handle_status, _},
             {:handle_execute, [%Q{}, [:after_connect], _, :state]},
             {:disconnect, [%DBConnection.ConnectionError{}, :state]},
             {:connect, _},
             {:handle_status, _},
             {:handle_execute, [%Q{}, [:after_connect], _, :state2]},
             {:handle_status, _},
             {:handle_execute, [%Q{}, [:client], _, :new_state2]}
             | _
           ] = A.record(agent)
  end

  test "after_connect prepare" do
    stack = [
      {:ok, :state},
      {:idle, :state},
      {:ok, %Q{}, :new_state},
      {:ok, %Q{}, :newer_state},
      {:idle, :newer_state},
      {:ok, %Q{}, :newest_state}
    ]

    {:ok, agent} = A.start_link(stack)

    after_connect = fn conn ->
      _ = Process.put(:agent, agent)
      assert P.prepare(conn, %Q{}) == {:ok, %Q{}}
      assert P.prepare(conn, %Q{}, key: :value) == {:ok, %Q{}}
      :ok
    end

    opts = [after_connect: after_connect, agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.prepare(pool, %Q{})

    assert [
             connect: [_],
             handle_status: _,
             handle_prepare: [%Q{}, _, :state],
             handle_prepare: [%Q{}, [{:key, :value} | _], :new_state],
             handle_status: _,
             handle_prepare: [%Q{}, _, :newer_state]
           ] = A.record(agent)
  end

  test "after_connect cancels timeout" do
    stack = [
      {:ok, :state},
      {:idle, :state},
      {:idle, :newer_state}
    ]

    {:ok, agent} = A.start_link(stack)

    after_connect = fn _conn -> :ok end

    opts = [
      after_connect: after_connect,
      after_connect_timeout: 500,
      agent: agent,
      parent: self(),
      idle_interval: 10_000
    ]

    {:ok, _} = P.start_link(opts)

    # Wait until the after_connect could trigger by mistake
    Process.sleep(1000)

    assert [connect: [_], handle_status: _, handle_status: _] = A.record(agent)
  end
end
