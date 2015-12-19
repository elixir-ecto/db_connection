defmodule PrepareExecuteTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestResult, as: R

  test "prepare_execute returns query and result" do
    stack = [
      {:ok, :state},
      {:ok, %Q{state: :prepared}, :new_state},
      {:ok, %R{}, :newer_state},
      {:ok, %Q{state: :prepared}, :newest_state},
      {:ok, %R{}, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.prepare_execute(pool, %Q{}, [:param]) ==
      {:ok, %Q{state: :prepared}, %R{}}
    assert P.prepare_execute(pool, %Q{}, [:param],
      [key: :value]) == {:ok, %Q{state: :prepared}, %R{}}

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state],
      handle_execute: [%Q{state: :prepared}, [:param], _, :new_state],
      handle_prepare: [%Q{}, [{:key, :value} | _], :newer_state],
      handle_execute: [%Q{state: :prepared},
        [:param], _, :newest_state]] = A.record(agent)
  end

  test "prepare_execute parses query" do
    stack = [
      {:ok, :state},
      {:ok, %Q{state: :prepared}, :new_state},
      {:ok, %R{}, :newer_stater}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    opts2 = [parse: fn(%Q{}) -> %Q{state: :parsed} end]
    assert P.prepare_execute(pool, %Q{}, [:param],
      opts2) == {:ok, %Q{state: :prepared}, %R{}}

    assert [
      connect: [_],
      handle_prepare: [%Q{state: :parsed}, _, :state],
      handle_execute: [%Q{state: :prepared},
        [:param], _, :new_state]] = A.record(agent)
  end

  test "prepare_execute describes query" do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state},
      {:ok, %R{}, :newer_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    opts2 = [describe: fn(%Q{}) -> %Q{state: :described} end]
    assert P.prepare_execute(pool, %Q{}, [:param],
      opts2) == {:ok, %Q{state: :described}, %R{}}

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state],
      handle_execute: [%Q{state: :described},
        [:param], _, :new_state]] = A.record(agent)
  end

  test "prepare_execute encodes params and decodes result" do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state},
      {:ok, %R{}, :newer_state},
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    opts2 = [encode: fn([:param]) -> :encoded end,
             decode: fn(%R{}) -> :decoded end]
    assert P.prepare_execute(pool, %Q{}, [:param],
      opts2) == {:ok, %Q{}, :decoded}

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state],
      handle_execute: [_, :encoded, _, :new_state]] = A.record(agent)
  end

  test "prepare_execute prepare error returns error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:error, err, :new_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.prepare_execute(pool, %Q{}, [:param]) == {:error, err}

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state]] = A.record(agent)
  end

  test "prepare_execute! error raises error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:error, err, :new_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_raise RuntimeError, "oops",
      fn() -> P.prepare_execute!(pool, %Q{}, [:param]) end

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state]] = A.record(agent)
  end

  test "prepare_execute execute disconnect returns error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state},
      {:disconnect, err, :newer_state},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :state}
      end
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.prepare_execute(pool, %Q{}, [:param]) == {:error, err}

    assert_receive :reconnected

    assert [
      connect: [opts2],
      handle_prepare: [%Q{}, _, :state],
      handle_execute: [%Q{}, [:param], _, :new_state],
      disconnect: [^err, :newer_state],
      connect: [opts2]] = A.record(agent)
  end
end
