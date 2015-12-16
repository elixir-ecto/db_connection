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

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    {_, ref} = spawn_monitor(fn() ->
      P.run(pool, fn(_) -> :timer.sleep(50) end, [timeout: 0])
    end)

    assert_receive :reconnected
    assert P.run(pool, fn(_) -> :result end) == :result
    assert_receive {:DOWN, ^ref, _, _, _}

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


    runner = spawn_link(fn() ->
      assert_receive {:pool, pool}
      assert_receive :reconnected
      assert P.run(pool, fn(_) -> :result end) == :result
    end)

    opts = [agent: agent, parent: self(), runner: runner]
    {:ok, pool} = P.start_link(opts)

    send(runner, {:pool, pool})

    try do
      P.run(pool, fn(conn) ->
        :timer.sleep(50)
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
