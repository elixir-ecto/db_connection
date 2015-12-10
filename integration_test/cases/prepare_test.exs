defmodule PrepareTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q

  test "prepare returns query" do
    stack = [
      {:ok, :state},
      {:ok, :prepared, :new_state},
      {:ok, :prepared, :new_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.prepare(pool, %Q{}) == {:ok, :prepared}
    assert P.prepare(pool, %Q{}, [key: :value]) == {:ok, :prepared}

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state],
      handle_prepare: [%Q{}, [{:key, :value} | _], :new_state]
      ] = A.record(agent)
  end

  test "prepare parses query" do
    stack = [
      {:ok, :state},
      {:ok, :prepared, :new_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    opts2 = [parse: fn(%Q{}) -> :parsed end]
    assert P.prepare(pool, %Q{}, opts2) == {:ok, :prepared}

    assert [
      connect: [_],
      handle_prepare: [:parsed, _, :state]] = A.record(agent)
  end

  test "prepare describes query" do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    opts2 = [describe: fn(%Q{}) -> :described end]
    assert P.prepare(pool, %Q{}, opts2) == {:ok, :described}

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state]] = A.record(agent)
  end

  test "prepare error returns error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:error, err, :new_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.prepare(pool, %Q{}) == {:error, err}

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state]] = A.record(agent)
  end

  test "prepare! error raises error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:error, err, :new_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert_raise RuntimeError, "oops", fn() -> P.prepare!(pool, %Q{}) end

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state]] = A.record(agent)
  end
end
