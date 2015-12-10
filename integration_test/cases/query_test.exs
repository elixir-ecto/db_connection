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
    assert P.query(pool, %Q{}) == {:ok, %R{}}
    assert P.query(pool, %Q{}, [key: :value]) == {:ok, %R{}}

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state],
      handle_execute_close: [%Q{}, _, :new_state],
      handle_prepare: [%Q{},
        [{:decode, :manual}, {:key, :value} | _], :newer_state],
      handle_execute_close: [%Q{},
        [{:encode, :manual}, {:decode, :manual}, {:key, :value} | _], :newest_state]] = A.record(agent)
  end

  test "query prepares query" do
    stack = [
      {:ok, :state},
      {:ok, :prepared2, :new_state},
      {:ok, %R{}, :newer_state},
      {:ok, :prepared2, :newest_state},
      {:ok, %R{}, :new_state2},
      {:ok, :handle_prepared, :newer_state2},
      {:ok, %R{}, :newest_state2}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    opts2 = [prepare_fun: fn(%Q{}) -> :prepared end]
    assert P.query(pool, %Q{}, opts2) == {:ok, %R{}}

    assert P.query(pool, %Q{}, [prepare: :auto] ++ opts2) == {:ok, %R{}}
    assert P.query(pool, %Q{}, [prepare: :manual] ++ opts2) == {:ok, %R{}}

    assert [
      connect: [_],
      handle_prepare: [:prepared, _, :state],
      handle_execute_close: [:prepared2, _, :new_state],
      handle_prepare: [:prepared, _, :newer_state],
      handle_execute_close: [:prepared2, _, :newest_state],
      handle_prepare: [%Q{}, _, :new_state2],
      handle_execute_close: [:handle_prepared, _, :newer_state2]] = A.record(agent)
  end

  test "query decodes result" do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, :new_state},
      {:ok, %R{}, :newer_state},
      {:ok, %Q{}, :newest_state},
      {:ok, %R{}, :new_state2},
      {:ok, %Q{}, :newer_state2},
      {:ok, %R{}, :newest_state2} 
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    opts2 = [decode_fun: fn(%R{}) -> :decoded end]
    assert P.query(pool, %Q{}, opts2) == {:ok, :decoded}

    assert P.query(pool, %Q{}, [decode: :auto] ++ opts) == {:ok, %R{}}

    assert P.query(pool, %Q{}, [decode: :manual] ++ opts) == {:ok, %R{}}

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state],
      handle_execute_close: [%Q{}, _, :new_state],
      handle_prepare: [%Q{}, _, :newer_state],
      handle_execute_close: [%Q{}, _, :newest_state],
      handle_prepare: [%Q{}, _, :new_state2],
      handle_execute_close: [%Q{}, _, :newer_state2]] = A.record(agent)
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
    assert P.query(pool, %Q{}) == {:error, err}

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
    assert P.query(pool, %Q{}) == {:error, err}

    assert [
      connect: [_],
      handle_prepare: [%Q{}, _, :state],
      handle_execute_close: [%Q{}, _, :new_state]] = A.record(agent)
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
    assert_raise RuntimeError, "oops", fn() -> P.query!(pool, %Q{}) end

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
    assert P.query(pool, %Q{}) == {:error, err}

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
    assert P.query(pool, %Q{}) == {:error, err}

    assert_receive :reconnected

    assert [
      connect: [opts2],
      handle_prepare: [%Q{}, _, :state],
      handle_execute_close: [%Q{}, _, :new_state],
      disconnect: [^err, :newer_state],
      connect: [opts2]] = A.record(agent)
  end
end
