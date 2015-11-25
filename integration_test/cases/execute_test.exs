defmodule ExecuteTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestResult, as: R

  test "execute returns result" do
    stack = [
      {:ok, :state},
      {:ok, %R{}, :new_state},
      {:ok, %R{}, :new_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.execute(pool, %Q{}) == {:ok, %R{}}
    assert P.execute(pool, %Q{}, [key: :value]) == {:ok, %R{}}

    assert [
      connect: [_],
      handle_execute: [%Q{}, _, :state],
      handle_execute: [%Q{}, [{:key, :value} | _], :new_state]
      ] = A.record(agent)
  end

  test "execute encodes query" do
    stack = [
      {:ok, :state},
      {:ok, %R{}, :new_state},
      {:ok, %R{}, :newer_state},
      {:ok, %R{}, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    opts2 = [encode_fun: fn(%Q{}) -> :encoded end]
    assert P.execute(pool, %Q{}, opts2) == {:ok, %R{}}

    assert P.execute(pool, %Q{}, [encode: :auto] ++ opts2) == {:ok, %R{}}
    assert P.execute(pool, %Q{}, [encode: :manual] ++ opts2) == {:ok, %R{}}

    assert [
      connect: [_],
      handle_execute: [:encoded, _, :state],
      handle_execute: [:encoded, _, :new_state],
      handle_execute: [%Q{}, _, :newer_state]] = A.record(agent)
  end

  test "execute decodes result" do
    stack = [
      {:ok, :state},
      {:ok, %R{}, :new_state},
      {:ok, %R{}, :newer_state},
      {:ok, %R{}, :newest_state},
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    opts2 = [decode_fun: fn(%R{}) -> :decoded end]
    assert P.execute(pool, %Q{}, opts2) == {:ok, :decoded}

    assert P.execute(pool, %Q{}, [decode: :auto] ++ opts) == {:ok, %R{}}

    assert P.execute(pool, %Q{}, [decode: :manual] ++ opts) == {:ok, %R{}}

    assert [
      connect: [_],
      handle_execute: [%Q{}, _, :state],
      handle_execute: [%Q{}, _, :new_state],
      handle_execute: [%Q{}, _, :newer_state]] = A.record(agent)
  end

  test "execute error returns error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:error, err, :new_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.execute(pool, %Q{}) == {:error, err}

    assert [
      connect: [_],
      handle_execute: [%Q{}, _, :state]] = A.record(agent)
  end

  test "execute! error raises error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:error, err, :new_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert_raise RuntimeError, "oops", fn() -> P.execute!(pool, %Q{}) end

    assert [
      connect: [_],
      handle_execute: [%Q{}, _, :state]] = A.record(agent)
  end

  test "execute disconnect returns error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:disconnect, err, :new_state},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :state}
      end
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.execute(pool, %Q{}) == {:error, err}

    assert_receive :reconnected

    assert [
      connect: [opts2],
      handle_execute: [%Q{}, _, :state],
      disconnect: [^err, :new_state],
      connect: [opts2]] = A.record(agent)
  end
end
