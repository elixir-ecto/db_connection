defmodule StageTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestCursor, as: C
  alias TestResult, as: R

  test "start_link produces result" do
    stack = [
      {:ok, :state},
      {:ok, %C{}, :new_state},
      {:ok, %R{}, :newer_state},
      {:deallocate, %R{}, :newest_state},
      {:ok, :deallocated, :state2},
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), stage_transaction: false]
    {:ok, pool} = P.start_link(opts)
    {:ok, stage} = P.stream_stage(pool, %Q{}, [:param], opts)
    mon = Process.monitor(stage)
    assert [{stage, [cancel: :transient]}] |> GenStage.stream() |> Enum.to_list() == [%R{}, %R{}]

    assert_receive {:DOWN, ^mon, :process, ^stage, :normal}

    assert [
      connect: [_],
      handle_declare: [%Q{}, [:param], _, :state],
      handle_first: [%Q{}, %C{}, _, :new_state],
      handle_next: [%Q{}, %C{}, _, :newer_state],
      handle_deallocate: [%Q{}, %C{}, _, :newest_state]
      ] = A.record(agent)
  end

  test "start_link with prepare: true produces result" do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state},
      {:ok, %C{}, :newer_state},
      {:ok, %R{}, :newest_state},
      {:deallocate, %R{}, :state2},
      {:ok, :deallocated, :new_state2},
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), stage_transaction: false]
    {:ok, pool} = P.start_link(opts)
    {:ok, stage} = P.stream_stage(pool, %Q{}, [:param], [stage_prepare: true] ++ opts)
    mon = Process.monitor(stage)
    assert [{stage, [cancel: :transient]}] |> GenStage.stream() |> Enum.to_list() == [%R{}, %R{}]

    assert_receive {:DOWN, ^mon, :process, ^stage, :normal}

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state],
      handle_declare: [%Q{}, [:param], _, :new_state],
      handle_first: [%Q{}, %C{}, _, :newer_state],
      handle_next: [%Q{}, %C{}, _, :newest_state],
      handle_deallocate: [%Q{}, %C{}, _, :state2]
      ] = A.record(agent)
  end

  test "stage stops normally after it's done" do
    stack = [
      {:ok, :state},
      {:ok, %C{}, :new_state},
      {:deallocate, %R{}, :newer_state},
      {:ok, :deallocated, :newest_state},
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent, stage_transaction: false]
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
      handle_declare: [%Q{}, [:param], _, :state],
      handle_first: [%Q{}, %C{}, _, :new_state],
      handle_deallocate: [%Q{}, %C{}, _, :newer_state],
      ] = A.record(agent)
  end

  test "stage with execute in stream_mapper" do
    stack = [
      {:ok, :state},
      {:ok, %C{}, :new_state},
      {:ok, %R{}, :newer_state},
      {:ok, %R{}, :newest_state},
      {:deallocate, %R{}, :state2},
      {:ok, %R{}, :new_state2},
      {:ok, :deallocated, :newer_state2},
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), stage_transaction: false]
    {:ok, pool} = P.start_link(opts)

    mapper = fn(conn, res) -> [P.execute!(conn, %Q{}, res, opts), :mapped] end
    opts = [stream_mapper: mapper] ++ opts
    {:ok, stage} = P.stream_stage(pool, %Q{}, [:param], opts)
    mon = Process.monitor(stage)
    assert [{stage, [cancel: :transient]}] |> GenStage.stream() |> Enum.to_list() == [%R{}, :mapped, %R{}, :mapped]

    assert_receive {:DOWN, ^mon, :process, ^stage, :normal}

    assert [
      connect: [_],
      handle_declare: [%Q{}, [:param], _, :state],
      handle_first: [%Q{}, %C{}, _, :new_state],
      handle_execute: [%Q{}, %R{}, _, :newer_state],
      handle_next: [%Q{}, %C{}, _, :newest_state],
      handle_execute: [%Q{}, %R{}, _, :state2],
      handle_deallocate: [%Q{}, %C{}, _, :new_state2],
      ] = A.record(agent)
  end

  test "stage checks in on abnormal exit" do
    stack = [
      {:ok, :state},
      {:ok, %C{}, :new_state},
      {:ok, :deallocated, :newer_state},
      {:ok, %R{}, :new_state2}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent, stage_transaction: false]
    Process.flag(:trap_exit, true)
    {:ok, pool} = P.start_link(opts)
    {:ok, stage} = P.stream_stage(pool, %Q{}, [:param], opts)

    send(stage, {:"$gen_producer", {parent, make_ref()}, {:subscribe, nil, []}})

    GenStage.stop(stage, :oops)

    assert P.execute!(pool, %Q{}, [:param], opts) == %R{}

    assert [
      connect: [_],
      handle_declare: [%Q{}, [:param], _, :state],
      handle_deallocate: [%Q{}, %C{}, _, :new_state],
      handle_execute: [%Q{}, [:param], _, :newer_state]
      ] = A.record(agent)
  end

  test "stage declare disconnects" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:disconnect, err, :new_state},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :state2}
      end
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent, stage_transaction: false]
    Process.flag(:trap_exit, true)
    {:ok, pool} = P.start_link(opts)
    assert {:error, {^err, _}} = P.stream_stage(pool, %Q{}, [:param], opts)

    assert_receive :reconnected

    assert [
      connect: [_],
      handle_declare: [%Q{}, [:param], _, :state],
      disconnect: [^err, :new_state],
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
      :oops,
      {:ok, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent, stage_transaction: false]
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
      {:handle_declare, [%Q{}, [:param], _, :state]} | _] = A.record(agent)
  end

  test "stage checks in if first errors" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, %C{}, :new_state},
      {:error, err, :newer_state},
      {:ok, :deallocated, :newest_state},
      {:ok, %R{}, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent, stage_transaction: false]
    {:ok, pool} = P.start_link(opts)
    Process.flag(:trap_exit, true)
    {:ok, stage} = P.stream_stage(pool, %Q{}, [:param], opts)
    catch_exit([{stage, [cancel: :transient]}] |> GenStage.stream() |> Enum.to_list())
    assert_receive {:EXIT, ^stage, {^err, _}}

    assert P.execute!(pool, %Q{}, [:param], opts) == %R{}

    assert [
      connect: [_],
      handle_declare: [%Q{}, [:param], _, :state],
      handle_first: [%Q{}, %C{}, _, :new_state],
      handle_deallocate: [%Q{}, %C{}, _, :newer_state],
      handle_execute: [%Q{}, [:param], _, :newest_state]
      ] = A.record(agent)
  end

  test "stage first disconnects" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, %C{}, :new_state},
      {:disconnect, err, :newer_state},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :state2}
      end
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent, stage_transaction: false]
    {:ok, pool} = P.start_link(opts)
    Process.flag(:trap_exit, true)
    {:ok, stage} = P.stream_stage(pool, %Q{}, [:param], opts)
    catch_exit([{stage, [cancel: :transient]}] |> GenStage.stream() |> Enum.to_list())
    assert_receive {:EXIT, ^stage, {^err, _}}

    assert_receive :reconnected

    assert [
      connect: [_],
      handle_declare: [%Q{}, [:param], _, :state],
      handle_first: [%Q{}, %C{}, _, :new_state],
      disconnect: [^err, :newer_state],
      connect: [_]
      ] = A.record(agent)
  end

  test "stage checks in if deallocate errors" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, %C{}, :new_state},
      {:deallocate, %R{}, :newer_state},
      {:error, err, :newest_state},
      {:ok, %R{}, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent, stage_transaction: false]
    {:ok, pool} = P.start_link(opts)
    Process.flag(:trap_exit, true)
    {:ok, stage} = P.stream_stage(pool, %Q{}, [:param], opts)
    catch_exit([{stage, [cancel: :transient]}] |> GenStage.stream() |> Enum.to_list())
    assert_receive {:EXIT, ^stage, {^err, _}}

    assert P.execute!(pool, %Q{}, [:param], opts) == %R{}

    assert [
      connect: [_],
      handle_declare: [%Q{}, [:param], _, :state],
      handle_first: [%Q{}, %C{}, _, :new_state],
      handle_deallocate: [%Q{}, %C{}, _, :newer_state],
      handle_execute: [%Q{}, [:param], _, :newest_state]
      ] = A.record(agent)
  end

  test "stage deallocate disconnects" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, %C{}, :new_state},
      {:deallocate, %R{}, :newer_state},
      {:disconnect, err, :newest_state},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :new_state2}
      end
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent, stage_transaction: false]
    {:ok, pool} = P.start_link(opts)
    Process.flag(:trap_exit, true)
    {:ok, stage} = P.stream_stage(pool, %Q{}, [:param], opts)
    catch_exit([{stage, [cancel: :transient]}] |> GenStage.stream() |> Enum.to_list())
    assert_receive {:EXIT, ^stage, {^err, _}}

    assert_receive :reconnected

    assert [
      connect: [_],
      handle_declare: [%Q{}, [:param], _, :state],
      handle_first: [%Q{}, %C{}, _, :new_state],
      handle_deallocate: [%Q{}, %C{}, _, :newer_state],
      disconnect: [^err, :newest_state],
      connect: [_]
      ] = A.record(agent)
  end
end
