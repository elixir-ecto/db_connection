defmodule PrepareExecuteTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestResult, as: R

  test "prepare_execute returns query and result" do
    stack = [
      {:ok, :state},
      {:ok, :prepared, :new_state},
      {:ok, %R{}, :newer_state},
      {:ok, :prepared, :newest_state},
      {:ok, %R{}, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.prepare_execute(pool, %Q{}, [:param]) == {:ok, :prepared, %R{}}
    assert P.prepare_execute(pool, %Q{}, [:param],
      [key: :value]) == {:ok, :prepared, %R{}}

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state],
      handle_execute: [:prepared, [:param], _, :new_state],
      handle_prepare: [%Q{}, [{:key, :value} | _], :newer_state],
      handle_execute: [:prepared, [:param], _, :newest_state]] = A.record(agent)
  end

  test "prepare_execute parses query" do
    stack = [
      {:ok, :state},
      {:ok, :prepared, :new_state},
      {:ok, %R{}, :newer_stater}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    opts2 = [parse: fn(%Q{}) -> :parsed end]
    assert P.prepare_execute(pool, %Q{}, [:param],
      opts2) == {:ok, :prepared, %R{}}

    assert [
      connect: [_],
      handle_prepare: [:parsed, _, :state],
      handle_execute: [:prepared, [:param], _, :new_state]] = A.record(agent)
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

    opts2 = [describe: fn(%Q{}) -> :described end]
    assert P.prepare_execute(pool, %Q{}, [:param],
      opts2) == {:ok, :described, %R{}}

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state],
      handle_execute: [:described, [:param], _, :new_state]] = A.record(agent)
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

  test "prepare_execute execute error returns error and closes" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state},
      {:error, err, :newer_state},
      {:ok, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.prepare_execute(pool, %Q{}, [:param]) == {:error, err}

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state],
      handle_execute: [%Q{}, [:param], _, :new_state],
      handle_close: [%Q{}, _, :newer_state]] = A.record(agent)
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

  test "prepare_execute execute and close error returns close error" do
    err1 = RuntimeError.exception("execute oops")
    err2 = RuntimeError.exception("close oops")
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state},
      {:error, err1, :newer_state},
      {:error, err2, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.prepare_execute(pool, %Q{}, [:param]) == {:error, err2}

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state],
      handle_execute: [%Q{}, [:param], _, :new_state],
      handle_close: [%Q{}, _, :newer_state]] = A.record(agent)
  end
end
