defmodule ClientTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestResult, as: R

  test "reconnect when client exits" do
    stack = [
      {:ok, :state},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :state}
      end]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    _ = spawn(fn() ->
      P.run(pool, fn(_) ->
        Process.exit(self(), :shutdown)
      end)
    end)

    assert_receive :reconnected
    assert P.run(pool, fn(_) -> :result end) == :result

    assert [
      {:connect, _},
      {:disconnect, _},
      {:connect, _}] = A.record(agent)
  end

  test "reconnect when client timeout" do
    stack = [
      {:ok, :state},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :state}
      end]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    pid = spawn_link(fn() ->
      assert_receive {:go, ^parent}
      assert P.run(pool, fn(_) -> :result end) == :result
      send(parent, {:done, self()})
    end)

    P.run(pool, fn(_) ->
      assert_receive :reconnected
      send(pid, {:go, parent})
      assert_receive {:done, ^pid}
    end, [timeout: 0])

    assert [
      {:connect, _},
      {:disconnect, _},
      {:connect, _}] = A.record(agent)
  end

  test "reconnect when client timeout and then disconnects" do
    stack = [
      {:ok, :state},
      :ok,
      fn(opts) ->
        send(opts[:runner], :reconnected)
        {:ok, :new_state}
      end,
      {:disconnect, RuntimeError.exception("oops"), :bad_state},
      {:ok, %R{}, :newer_state}]
    {:ok, agent} = A.start_link(stack)


    runner = spawn_link(fn() ->
      assert_receive {:pool, pool}
      assert_receive :reconnected
      assert P.run(pool, fn(_) -> :result end) == :result
    end)

    opts = [agent: agent, parent: self(), runner: runner]
    {:ok, pool} = P.start_link(opts)

    send(runner, {:pool, pool})

    assert P.run(pool, fn(conn) ->
        :timer.sleep(50)
        assert {:error, %RuntimeError{}} = P.execute(conn, %Q{}, [:param])
        :result
    end, [timeout: 0])  == :result

    assert P.execute(pool, %Q{}, [:param]) == {:ok, %R{}}

    assert [
      {:connect, _},
      {:disconnect, _},
      {:connect, _},
      {:handle_execute, [%Q{}, [:param], _, :state]},
      {:handle_execute, [%Q{}, [:param], _, :new_state]}] = A.record(agent)
  end

  test "reconnect when client timeout and then crashes" do
    stack = [
      {:ok, :state},
      :ok,
      fn(opts) ->
        send(opts[:runner], :reconnected)
        {:ok, :new_state}
      end,
      fn(_, _, _, _) ->
        throw(:oops)
      end,
      {:ok, %R{}, :newer_state}]
    {:ok, agent} = A.start_link(stack)

    parent = self()

    runner = spawn_link(fn() ->
      assert_receive {:pool, pool}
      assert_receive :reconnected
      assert P.run(pool, fn(_) -> :result end) == :result
      send(parent, :continue)
    end)

    opts = [agent: agent, parent: parent, runner: runner]
    {:ok, pool} = P.start_link(opts)

    send(runner, {:pool, pool})

    try do
      P.run(pool, fn(conn) ->
        assert_receive :continue
        P.execute(conn, %Q{}, [:param])
      end, [timeout: 0])
    catch
      :throw, :oops ->
        :ok
    end

    assert P.execute(pool, %Q{}, [:param]) == {:ok, %R{}}

    assert [
      {:connect, _},
      {:disconnect, _},
      {:connect, _},
      {:handle_execute, [%Q{}, [:param], _, :state]},
      {:handle_execute, [%Q{}, [:param], _, :new_state]}] = A.record(agent)
  end
end
