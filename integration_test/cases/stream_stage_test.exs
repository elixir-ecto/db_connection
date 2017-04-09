defmodule StreamStageTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias DBConnection.StreamStage, as: S
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestCursor, as: C
  alias TestResult, as: R

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
    {:ok, stage} = S.stream_link(pool, %Q{}, [:param], opts)
    assert stage |> Flow.from_stage() |> Enum.to_list() == [%R{}, %R{}]

    GenStage.stop(stage)

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
    {:ok, stage} = S.prepare_stream_link(pool, %Q{}, [:param], opts)
    assert stage |> Flow.from_stage() |> Enum.to_list() == [%R{}, %R{}]

    GenStage.stop(stage)

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

  test "stream can be rolled back in flat map" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %C{}, :newer_state},
      {:ok, %R{}, :newest_state},
      {:ok, :deallocated, :state2},
      {:ok, :rolledback, :new_state2}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    map = fn(conn, _) -> P.rollback(conn, :normal) end
    {:ok, stage} = S.stream_link(pool, %Q{}, [:param], [flat_map: map] ++ opts)
    mon = Process.monitor(stage)
    catch_exit(stage |> Flow.from_stage() |> Enum.to_list())

    assert_receive {:DOWN, ^ref, :process, ^stage, :normal}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_declare: [%Q{}, [:param], _, :new_state],
      handle_first: [%Q{}, %C{}, _, :newer_state],
      handle_deallocate: [%Q{}, %C{}, _, :newest_state],
      handle_rollback: [_, :state2]
      ] = A.record(agent)
  end
end
