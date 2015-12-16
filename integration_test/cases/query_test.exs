defmodule QueryTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestResult, as: R

  test "query returns result" do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state},
      {:ok, %R{}, :newer_state},
      {:ok, %Q{}, :newest_state},
      {:ok, %R{}, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.query(pool, %Q{}, [:param]) == {:ok, %R{}}
    assert P.query(pool, %Q{}, [:param], [key: :value]) == {:ok, %R{}}

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state],
      handle_execute_close: [%Q{}, [:param], _, :new_state],
      handle_prepare: [%Q{}, [{:key, :value} | _], :newer_state],
      handle_execute_close: [%Q{}, [:param],
        [{:key, :value} | _], :newest_state]] = A.record(agent)
  end

  test "query parses query" do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state},
      {:ok, %R{}, :newer_state}
     ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    opts2 = [parse: fn(%Q{}) -> %Q{state: :parsed} end]
    assert P.query(pool, %Q{}, [:param], opts2) == {:ok, %R{}}

    assert [
      connect: [_],
      handle_prepare: [%Q{state: :parsed}, _, :state],
      handle_execute_close: [%Q{}, [:param], _, :new_state]] = A.record(agent)
  end

  test "query describe query" do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state},
      {:ok, %R{}, :newer_state}
     ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    opts2 = [describe: fn(%Q{}) -> %Q{state: :described} end]
    assert P.query(pool, %Q{}, [:param], opts2) == {:ok, %R{}}

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state],
      handle_execute_close: [%Q{state: :described},
        [:param], _, :new_state]] = A.record(agent)
  end

  test "query encodes parameters" do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state},
      {:ok, %R{}, :newer_state}
     ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    opts2 = [encode: fn([:param]) -> :encoded end]
    assert P.query(pool, %Q{}, [:param], opts2) == {:ok, %R{}}

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state],
      handle_execute_close: [%Q{}, :encoded, _, :new_state]] = A.record(agent)
  end

  test "query decodes result" do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state},
      {:ok, %R{}, :newer_state}
     ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    opts2 = [decode: fn(%R{}) -> :decoded end]
    assert P.query(pool, %Q{}, [:param], opts2) == {:ok, :decoded}

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state],
      handle_execute_close: [%Q{}, [:param], _, :new_state]] = A.record(agent)
  end

  test "query handle_prepare error returns error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:error, err, :new_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.query(pool, %Q{}, [:param]) == {:error, err}

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state]] = A.record(agent)
  end

  test "query handle_execute_close error returns error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state},
      {:error, err, :newer_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.query(pool, %Q{}, [:param]) == {:error, err}

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state],
      handle_execute_close: [%Q{}, [:param], _, :new_state]] = A.record(agent)
  end

  test "query! error raises error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:error, err, :new_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert_raise RuntimeError, "oops",
      fn() -> P.query!(pool, %Q{}, [:param]) end

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state]] = A.record(agent)
  end

  test "query handle_prepare disconnect returns error" do
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
    assert P.query(pool, %Q{}, [:param]) == {:error, err}

    assert_receive :reconnected

    assert [
      connect: [opts2],
      handle_prepare: [%Q{}, _, :state],
      disconnect: [^err, :new_state],
      connect: [opts2]] = A.record(agent)
  end

  test "query handle_execute_close disconnect returns error" do
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
    assert P.query(pool, %Q{}, [:param]) == {:error, err}

    assert_receive :reconnected

    assert [
      connect: [opts2],
      handle_prepare: [%Q{}, _, :state],
      handle_execute_close: [%Q{}, [:param], _, :new_state],
      disconnect: [^err, :newer_state],
      connect: [opts2]] = A.record(agent)
  end
end
