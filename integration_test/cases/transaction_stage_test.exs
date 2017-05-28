defmodule TransactionStageTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestCursor, as: C
  alias TestResult, as: R

  test "start_link produces result" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %C{}, :newer_state},
      {:ok, %R{}, :newest_state},
      {:deallocate, %R{}, :state2},
      {:ok, :deallocated, :new_state2},
      {:ok, :commited, :newer_state2}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    {:ok, stage} = P.stream_stage(pool, %Q{}, [:param], opts)
    mon = Process.monitor(stage)
    assert [{stage, [cancel: :transient]}] |> GenStage.stream() |> Enum.to_list() == [%R{}, %R{}]

    assert_receive {:DOWN, ^mon, :process, ^stage, :normal}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_declare: [%Q{}, [:param], _, :new_state],
      handle_first: [%Q{}, %C{}, _, :newer_state],
      handle_next: [%Q{}, %C{}, _, :newest_state],
      handle_deallocate: [%Q{}, %C{}, _, :state2],
      handle_commit: [_, :new_state2]
      ] = A.record(agent)
  end

  test "start_link with prepare: true produces result" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %Q{}, :newer_state},
      {:ok, %C{}, :newest_state},
      {:ok, %R{}, :state2},
      {:deallocate, %R{}, :new_state2},
      {:ok, :deallocated, :newer_state2},
      {:ok, :commited, :newest_state2}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    {:ok, stage} = P.stream_stage(pool, %Q{}, [:param], [stage_prepare: true] ++ opts)
    mon = Process.monitor(stage)
    assert [{stage, [cancel: :transient]}] |> GenStage.stream() |> Enum.to_list() == [%R{}, %R{}]

    assert_receive {:DOWN, ^mon, :process, ^stage, :normal}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_prepare: [%Q{}, _, :new_state],
      handle_declare: [%Q{}, [:param], _, :newer_state],
      handle_first: [%Q{}, %C{}, _, :newest_state],
      handle_next: [%Q{}, %C{}, _, :state2],
      handle_deallocate: [%Q{}, %C{}, _, :new_state2],
      handle_commit: [_, :newer_state2]
      ] = A.record(agent)
  end

  test "stage stops normally after it's done" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %C{}, :newer_state},
      {:deallocate, %R{}, :state2},
      {:ok, :deallocated, :new_state2},
      {:ok, :commited, :newer_state2}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)
    {:ok, stage} = P.stream_stage(pool, %Q{}, [:param], opts)

    ref = Process.monitor(stage)
    send(stage, {:"$gen_producer", {parent, ref}, {:subscribe, nil, [cancel: :transient]}})
    sub = {stage, ref}
    GenStage.ask(sub, 1000)

    assert_receive {:"$gen_consumer", ^sub, [%R{}]}

    assert_receive {:DOWN, ^ref, :process, ^stage, :normal}
    refute_received {:"$gen_producer", ^sub, _}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_declare: [%Q{}, [:param], _, :new_state],
      handle_first: [%Q{}, %C{}, _, :newer_state],
      handle_deallocate: [%Q{}, %C{}, _, :state2],
      handle_commit: [_, :new_state2]
      ] = A.record(agent)
  end

  test "stage rolls back on abnormal exit" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %C{}, :newer_state},
      {:ok, :deallocated, :newest_state},
      {:ok, :rolledback, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    Process.flag(:trap_exit, true)
    {:ok, pool} = P.start_link(opts)
    {:ok, stage} = P.stream_stage(pool, %Q{}, [:param], opts)

    send(stage, {:"$gen_producer", {parent, make_ref()}, {:subscribe, nil, []}})

    GenStage.stop(stage, :oops)

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_declare: [%Q{}, [:param], _, :new_state],
      handle_deallocate: [%Q{}, %C{}, _, :newer_state],
      handle_rollback: [_, :newest_state]
      ] = A.record(agent)
  end

  test "stage declare disconnects" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:disconnect, err, :newer_state},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :state2}
      end
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    Process.flag(:trap_exit, true)
    {:ok, pool} = P.start_link(opts)
    assert {:error, {^err, _}} = P.stream_stage(pool, %Q{}, [:param], opts)

    assert_receive :reconnected

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_declare: [%Q{}, [:param], _, :new_state],
      disconnect: [^err, :newer_state],
      connect: [_]
      ] = A.record(agent)
  end

  test "stage declare bad return raises and stops" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, :began, :new_state},
      :oops,
      {:ok, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}

    Process.flag(:trap_exit, true)
    {:error, {%DBConnection.ConnectionError{}, _} = reason} = P.stream_stage(pool, %Q{}, [:param], opts)

    assert_receive {:EXIT, stage, ^reason}

    prefix = "client #{inspect stage} stopped: " <>
      "** (DBConnection.ConnectionError) bad return value: :oops"
    len = byte_size(prefix)
    assert_receive {:EXIT, ^conn,
      {%DBConnection.ConnectionError{message: <<^prefix::binary-size(len), _::binary>>},
        [_|_]}}

    assert [
      {:connect, _},
      {:handle_begin, [_, :state]},
      {:handle_declare, [%Q{}, [:param], _, :new_state]} | _] = A.record(agent)
  end

  test "stage rolls back if first errors" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %C{}, :newer_state},
      {:error, err, :newest_state},
      {:ok, :deallocated, :state2},
      {:ok, :rolledback, :new_state2}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)
    Process.flag(:trap_exit, true)
    {:ok, stage} = P.stream_stage(pool, %Q{}, [:param], opts)
    catch_exit([{stage, [cancel: :transient]}] |> GenStage.stream() |> Enum.to_list())
    assert_receive {:EXIT, ^stage, {^err, _}}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_declare: [%Q{}, [:param], _, :new_state],
      handle_first: [%Q{}, %C{}, _, :newer_state],
      handle_deallocate: [%Q{}, %C{}, _, :newest_state],
      handle_rollback: [_, :state2]
      ] = A.record(agent)
  end

  test "stage first disconnects" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %C{}, :newer_state},
      {:disconnect, err, :newest_state},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :state2}
      end
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)
    Process.flag(:trap_exit, true)
    {:ok, stage} = P.stream_stage(pool, %Q{}, [:param], opts)
    catch_exit([{stage, [cancel: :transient]}] |> GenStage.stream() |> Enum.to_list())
    assert_receive {:EXIT, ^stage, {^err, _}}

    assert_receive :reconnected

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_declare: [%Q{}, [:param], _, :new_state],
      handle_first: [%Q{}, %C{}, _, :newer_state],
      disconnect: [^err, :newest_state],
      connect: [_]
      ] = A.record(agent)
  end

  test "stage rolls back if deallocate errors" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %C{}, :newer_state},
      {:deallocate, %R{}, :newest_state},
      {:error, err, :state2},
      {:ok, :rolledback, :new_state2}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)
    Process.flag(:trap_exit, true)
    {:ok, stage} = P.stream_stage(pool, %Q{}, [:param], opts)
    catch_exit([{stage, [cancel: :transient]}] |> GenStage.stream() |> Enum.to_list())
    assert_receive {:EXIT, ^stage, {^err, _}}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_declare: [%Q{}, [:param], _, :new_state],
      handle_first: [%Q{}, %C{}, _, :newer_state],
      handle_deallocate: [%Q{}, %C{}, _, :newest_state],
      handle_rollback: [_, :state2]
      ] = A.record(agent)
  end

  test "stage deallocate disconnects" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %C{}, :newer_state},
      {:deallocate, %R{}, :newest_state},
      {:disconnect, err, :state2},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :new_state2}
      end
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)
    Process.flag(:trap_exit, true)
    {:ok, stage} = P.stream_stage(pool, %Q{}, [:param], opts)
    catch_exit([{stage, [cancel: :transient]}] |> GenStage.stream() |> Enum.to_list())
    assert_receive {:EXIT, ^stage, {^err, _}}

    assert_receive :reconnected

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_declare: [%Q{}, [:param], _, :new_state],
      handle_first: [%Q{}, %C{}, _, :newer_state],
      handle_deallocate: [%Q{}, %C{}, _, :newest_state],
      disconnect: [^err, :state2],
      connect: [_]
      ] = A.record(agent)
  end
end
