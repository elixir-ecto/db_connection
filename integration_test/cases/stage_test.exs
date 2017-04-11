defmodule StageTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestCursor, as: C
  alias TestResult, as: R

  test "producer executes on demand" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %Q{}, :newer_state},
      {:ok, %R{}, :newest_state},
      {:ok, :closed, :state2},
      {:ok, :commited, :new_state2}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    start = &DBConnection.prepare!(&1, %Q{})
    handle = fn(conn, demand, query) ->
      GenStage.async_notify(self(), {:producer, :done})
      {[DBConnection.execute!(conn, query, [demand])], query}
    end
    stop = fn(conn, _, query) -> DBConnection.close!(conn, query) end
    {:ok, stage} = P.producer(pool, start, handle, stop, opts)
    mon = Process.monitor(stage)
    assert stage |> Flow.from_stage() |> Enum.to_list() == [%R{}]

    assert_receive {:DOWN, ^mon, :process, ^stage, :normal}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_prepare: [%Q{}, _, :new_state],
      handle_execute: [%Q{}, _, _, :newer_state],
      handle_close: [%Q{}, _, :newest_state],
      handle_commit: [_, :state2]
      ] = A.record(agent)
  end

  test "producer exits when last consumer cancels" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %Q{}, :newer_state},
      {:ok, %R{}, :newest_state},
      {:ok, :closed, :state2},
      {:ok, :commited, :new_state2}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)
    start = &DBConnection.prepare!(&1, %Q{})
    handle = fn(conn, demand, query) ->
      GenStage.async_notify(self(), {:producer, :done})
      {[DBConnection.execute!(conn, query, [demand])], query}
    end
    stop = fn(conn, _, query) -> DBConnection.close!(conn, query) end
    {:ok, stage} = P.producer(pool, start, handle, stop, opts)

    ref = make_ref()
    sub_opts = [cancel: :temporary]
    send(stage, {:"$gen_producer", {parent, ref}, {:subscribe, nil, sub_opts}})
    sub = {stage, ref}
    GenStage.ask(sub, 1)
    assert_receive {:"$gen_consumer", ^sub, [%R{}]}
    mon = Process.monitor(stage)
    GenStage.cancel(sub, :normal)
    assert_receive {:DOWN, ^mon, :process, ^stage, :normal}
    assert_received {:"$gen_consumer", ^sub, {:cancel, :normal}}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_prepare: [%Q{}, _, :new_state],
      handle_execute: [%Q{}, [1], _, :newer_state],
      handle_close: [%Q{}, _, :newest_state],
      handle_commit: [_, :state2]
      ] = A.record(agent)
  end

  test "producer does not stop or commit on rollback" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %Q{}, :newer_state},
      {:ok, :rolledback, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    start = &DBConnection.prepare!(&1, %Q{})
    handle = fn(conn, _, _) ->
      DBConnection.rollback(conn, :normal)
    end
    stop = fn(conn, _, query) -> DBConnection.close!(conn, query) end
    {:ok, stage} = P.producer(pool, start, handle, stop, opts)
    mon = Process.monitor(stage)
    catch_exit(stage |> Flow.from_stage() |> Enum.to_list())

    assert_receive {:DOWN, ^mon, :process, ^stage, :normal}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_prepare: [%Q{}, _, :new_state],
      handle_rollback: [_, :newer_state]
      ] = A.record(agent)
  end

  test "producer does not stop or commit on handle nested rollback" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %Q{}, :newer_state},
      {:ok, :rolledback, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    start = &DBConnection.prepare!(&1, %Q{})
    handle = fn(conn, _, query) ->
      assert DBConnection.transaction(conn, fn(conn2) ->
        DBConnection.rollback(conn2, :oops)
      end) == {:error, :oops}
      {[:oops], query}
    end
    stop = fn(conn, _, query) -> DBConnection.close!(conn, query) end
    {:ok, stage} = P.producer(pool, start, handle, stop, opts)
    mon = Process.monitor(stage)
    assert stage |> Flow.from_stage() |> Enum.take(1) == [:oops]

    assert_receive {:DOWN, ^mon, :process, ^stage, :normal}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_prepare: [%Q{}, _, :new_state],
      handle_rollback: [_, :newer_state]
      ] = A.record(agent)
  end

  test "producer exits but does not commit on start nested rollback" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, :rolledback, :newer_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    start = fn(conn) ->
      assert DBConnection.transaction(conn, fn(conn2) ->
        DBConnection.rollback(conn2, :oops)
      end) == {:error, :oops}
      :oops
    end
    fail = fn(_, _, _) -> flunk "should not run" end
    {:ok, stage} = P.producer(pool, start, fail, fail, opts)

    Process.flag(:trap_exit, true)
    catch_exit(stage |> Flow.from_stage() |> Enum.to_list())
    assert_receive {:EXIT, ^stage, :rollback}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_rollback: [_, :new_state]
      ] = A.record(agent)
  end

  test "consumer executes on events" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %Q{}, :newer_state},
      {:ok, %R{}, :newest_state},
      {:ok, :closed, :state2},
      {:ok, :commited, :new_state2}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    start = &DBConnection.prepare!(&1, %Q{})
    handle = fn(conn, events, query) ->
      DBConnection.execute!(conn, query, events)
      {[], query}
    end
    stop = fn(conn, _, query) -> DBConnection.close!(conn, query) end
    {:ok, stage} = P.consumer(pool, start, handle, stop, opts)
    mon = Process.monitor(stage)
    flow = Flow.from_enumerable([:param])
    {:ok, _} = Flow.into_stages(flow, [stage])

    assert_receive {:DOWN, ^mon, :process, ^stage, :normal}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_prepare: [%Q{}, _, :new_state],
      handle_execute: [%Q{}, [:param], _, :newer_state],
      handle_close: [%Q{}, _, :newest_state],
      handle_commit: [_, :state2]
      ] = A.record(agent)
  end

  test "consumer cancels new producers after it's done" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %Q{}, :newer_state},
      {:ok, %R{}, :newest_state},
      {:ok, :closed, :state2},
      {:ok, :commited, :new_state2}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)
    start = &DBConnection.prepare!(&1, %Q{})
    handle = fn(conn, events, query) ->
      DBConnection.execute!(conn, query, events)
      {[], query}
    end
    stop = fn(conn, _, query) -> DBConnection.close!(conn, query) end
    {:ok, stage} = P.consumer(pool, start, handle, stop, opts)

    :ok = GenStage.async_subscribe(stage, [to: self(), cancel: :temporary, max_demand: 1])

    assert_receive {:"$gen_producer", {^stage, ref1} = sub1, {:subscribe, _, _}}
    assert_receive {:"$gen_producer", ^sub1, {:ask, 1}}

    send(stage, {:"$gen_consumer", {self(), ref1}, [:param]})
    send(stage, {{self(), ref1}, {:producer, :done}})
    :ok = GenStage.async_subscribe(stage, [to: self(), cancel: :temporary, max_demand: 1])

    assert_receive {:"$gen_producer", {^stage, _} = sub2, {:subscribe, _, _}}
    assert_receive {:"$gen_producer", ^sub1, {:ask, 1}}
    assert_receive {:"$gen_producer", ^sub1, {:cancel, :normal}}
    assert_receive {:"$gen_producer", ^sub2, {:cancel, :normal}}

    GenStage.stop(stage)
    refute_received {:"$gen_producer", _, _}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_prepare: [%Q{}, _, :new_state],
      handle_execute: [%Q{}, [:param], _, :newer_state],
      handle_close: [%Q{}, _, :newest_state],
      handle_commit: [_, :state2]
      ] = A.record(agent)
  end

  test "producer_consumer executes on events" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %Q{}, :newer_state},
      {:ok, %R{}, :newest_state},
      {:ok, :closed, :state2},
      {:ok, :commited, :new_state2}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    start = &DBConnection.prepare!(&1, %Q{})
    handle = fn(conn, events, query) ->
      {[DBConnection.execute!(conn, query, events)], query}
    end
    stop = fn(conn, _, query) -> DBConnection.close!(conn, query) end
    {:ok, stage} = P.producer_consumer(pool, start, handle, stop, opts)
    mon = Process.monitor(stage)
    {:ok, _} = Flow.from_enumerable([:param]) |> Flow.into_stages([stage])
    assert Flow.from_stage(stage) |> Enum.to_list() == [%R{}]

    assert_receive {:DOWN, ^mon, :process, ^stage, :normal}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_prepare: [%Q{}, _, :new_state],
      handle_execute: [%Q{}, [:param], _, :newer_state],
      handle_close: [%Q{}, _, :newest_state],
      handle_commit: [_, :state2]
      ] = A.record(agent)
  end

  test "producer_consumer does not send demand to new producers after done" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %Q{}, :newer_state},
      {:ok, %R{}, :newest_state},
      {:ok, :closed, :state2},
      {:ok, :commited, :new_state2}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)
    start = &DBConnection.prepare!(&1, %Q{})
    handle = fn(conn, events, query) ->
      {[DBConnection.execute!(conn, query, events)], query}
    end
    stop = fn(conn, _, query) -> DBConnection.close!(conn, query) end
    {:ok, stage} = P.producer_consumer(pool, start, handle, stop, opts)

    :ok = GenStage.async_subscribe(stage, [to: self(), cancel: :temporary, max_demand: 1])

    assert_receive {:"$gen_producer", {^stage, ref1} = sub1, {:subscribe, _, _}}
    assert_receive {:"$gen_producer", ^sub1, {:ask, 1}}

    task = Task.async(fn() ->
      [stage] |> Flow.from_stages() |> Enum.map(&send(parent, &1))
    end)

    send(stage, {:"$gen_consumer", {self(), ref1}, [:param]})
    assert_receive %R{}

    :sys.suspend(stage)
    send(stage, {{self(), ref1}, {:producer, :done}})
    :ok = GenStage.async_subscribe(stage, [to: self(), cancel: :temporary, max_demand: 1])
    mon = Process.monitor(stage)
    :sys.resume(stage)

    assert Task.await(task) == [%R{}]
    assert_receive {:"$gen_producer", {^stage, _}, {:subscribe, _, _}}
    assert_receive {:DOWN, ^mon, :process, ^stage, :normal}
    assert_received {:"$gen_producer", ^sub1, {:ask, 1}}
    refute_received {:"$gen_producer", _, _}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_prepare: [%Q{}, _, :new_state],
      handle_execute: [%Q{}, [:param], _, :newer_state],
      handle_close: [%Q{}, _, :newest_state],
      handle_commit: [_, :state2]
      ] = A.record(agent)
  end

  test "producer_consumer does notifies new consumers after done" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %Q{}, :newer_state},
      {:ok, %R{}, :newest_state},
      {:ok, :closed, :state2},
      {:ok, :commited, :new_state2}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)
    start = &DBConnection.prepare!(&1, %Q{})
    handle = fn(conn, events, query) ->
      {[DBConnection.execute!(conn, query, events)], query}
    end
    stop = fn(conn, _, query) -> DBConnection.close!(conn, query) end
    {:ok, stage} = P.producer_consumer(pool, start, handle, stop, opts)

    mon = Process.monitor(stage)

    {:ok, _} = [:param] |> Flow.from_enumerable() |> Flow.into_stages([stage])

    send(stage, {:"$gen_producer", {self(), mon}, {:subscribe, nil, []}})
    send(stage, {:"$gen_producer", {self(), mon}, {:ask, 1000}})

    sub1 = {stage, mon}

    assert_receive {:"$gen_consumer", ^sub1, [%R{}]}
    assert_receive {:"$gen_consumer", ^sub1, {:notification, {:producer, :done}}}

    task = Task.async(fn() ->
      [stage] |> Flow.from_stages() |> Enum.to_list()
    end)

    :timer.sleep(100)

    GenStage.cancel(sub1, :normal)

    assert Task.await(task) == []

    assert_receive {:DOWN, ^mon, _, _, :normal}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_prepare: [%Q{}, _, :new_state],
      handle_execute: [%Q{}, [:param], _, :newer_state],
      handle_close: [%Q{}, _, :newest_state],
      handle_commit: [_, :state2]
      ] = A.record(agent)
  end

  test "stream returns result" do
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
    assert stage |> Flow.from_stage() |> Enum.to_list() == [%R{}, %R{}]

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

  test "prepare_stream returns result" do
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
    {:ok, stage} = P.prepare_stream_stage(pool, %Q{}, [:param], opts)
    mon = Process.monitor(stage)
    assert stage |> Flow.from_stage() |> Enum.to_list() == [%R{}, %R{}]

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

  test "stream notifies new consumers that it's done" do
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

    ref = make_ref()
    send(stage, {:"$gen_producer", {parent, ref}, {:subscribe, nil, []}})
    sub = {stage, ref}
    GenStage.ask(sub, 1000)

    assert_receive {:"$gen_consumer", ^sub, [%R{}]}
    assert_receive {:"$gen_consumer", ^sub, {:notification, {:producer, :halted}}}

    assert stage |> Flow.from_stage() |> Enum.to_list() == []

    mon = Process.monitor(stage)
    GenStage.cancel(sub, :normal)
    assert_receive {:DOWN, ^mon, :process, ^stage, :normal}
    assert_received {:"$gen_consumer", ^sub, {:cancel, :normal}}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_declare: [%Q{}, [:param], _, :new_state],
      handle_first: [%Q{}, %C{}, _, :newer_state],
      handle_deallocate: [%Q{}, %C{}, _, :state2],
      handle_commit: [_, :new_state2]
      ] = A.record(agent)
  end

  test "stream finishes when consumers do" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %C{}, :newer_state},
      {:ok, :deallocated, :newest_state},
      {:ok, :commited, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)
    {:ok, stage} = P.stream_stage(pool, %Q{}, [:param], opts)

    ref = make_ref()
    send(stage, {:"$gen_producer", {parent, ref}, {:subscribe, nil, []}})
    sub = {stage, ref}

    mon = Process.monitor(stage)
    GenStage.cancel(sub, :normal)
    assert_receive {:DOWN, ^mon, :process, ^stage, :normal}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_declare: [%Q{}, [:param], _, :new_state],
      handle_deallocate: [%Q{}, %C{}, _, :newer_state],
      handle_commit: [_, :newest_state]
      ] = A.record(agent)
  end

  test "stream rolls back on abnormal exit" do
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

  test "stream declare disconnects" do
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

  test "stream declare bad return raises and stops" do
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

  test "stream rolls back if first errors" do
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
    catch_exit(stage |> Flow.from_stage() |> Enum.to_list())
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

  test "stream first disconnects" do
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
    catch_exit(stage |> Flow.from_stage() |> Enum.to_list())
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

  test "stream rolls back if deallocate errors" do
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
    catch_exit(stage |> Flow.from_stage() |> Enum.to_list())
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

  test "stream deallocate disconnects" do
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
    catch_exit(stage |> Flow.from_stage() |> Enum.to_list())
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
