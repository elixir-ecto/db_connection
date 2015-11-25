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

  test "prepare prepares query" do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state},
      {:ok, %Q{}, :newer_state},
      {:ok, %Q{}, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    opts2 = [prepare_fun: fn(%Q{}) -> :prepared end]
    assert P.prepare(pool, %Q{}, opts2) == {:ok, %Q{}}

    assert P.prepare(pool, %Q{}, [prepare: :auto] ++ opts2) == {:ok, %Q{}}
    assert P.prepare(pool, %Q{}, [prepare: :manual] ++ opts2) == {:ok, %Q{}}

    assert [
      connect: [_],
      handle_prepare: [:prepared, _, :state],
      handle_prepare: [:prepared, _, :new_state],
      handle_prepare: [%Q{}, _, :newer_state]] = A.record(agent)
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
