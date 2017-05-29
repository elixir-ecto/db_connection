defmodule ConsumerTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestResult, as: R

  test "start_link consumes events" do
    stack = [
      {:ok, :state},
      {:ok, %R{}, :new_state},
      {:ok, %R{}, :newer_state},
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), stage_transaction: false]
    {:ok, pool} = P.start_link(opts)
    {:ok, stage} = P.start_consumer(pool, &P.execute!(&1, %Q{}, &2, opts), opts)
    mon = Process.monitor(stage)

    {:ok, _} =
      [1, 2]
      |> Flow.from_enumerable()
      |> Flow.into_stages([{stage, [cancel: :transient, max_demand: 1]}])

    assert_receive {:DOWN, ^mon, :process, ^stage, :normal}

    assert [
      connect: [_],
      handle_execute: [%Q{}, [1], _, :state],
      handle_execute: [%Q{}, [2], _, :new_state]
      ] = A.record(agent)
  end
end
