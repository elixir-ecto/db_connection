defmodule CloseTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q

  test "close returns ok" do
    stack = [
      {:ok, :state},
      {:ok, :result, :new_state},
      {:ok, :result, :new_state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.close(pool, %Q{}) == {:ok, :result}
    assert P.close(pool, %Q{}, key: :value) == {:ok, :result}

    assert [
             connect: [_],
             handle_close: [%Q{}, _, :state],
             handle_close: [%Q{}, [{:key, :value} | _], :new_state]
           ] = A.record(agent)
  end

  test "close logs ok" do
    stack = [
      {:ok, :state},
      {:ok, :result, :new_state}
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    log = fn entry ->
      assert %DBConnection.LogEntry{
               call: :close,
               query: %Q{},
               params: nil,
               result: {:ok, :result}
             } = entry

      assert is_integer(entry.pool_time)
      assert entry.pool_time >= 0
      assert is_integer(entry.connection_time)
      assert entry.connection_time >= 0
      assert is_nil(entry.decode_time)
      send(parent, :logged)
    end

    assert P.close(pool, %Q{}, log: log) == {:ok, :result}
    assert_received :logged

    assert [
             connect: [_],
             handle_close: [%Q{}, _, :state]
           ] = A.record(agent)
  end

  test "close error returns error" do
    err = RuntimeError.exception("oops")

    stack = [
      {:ok, :state},
      {:error, err, :new_state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.close(pool, %Q{}) == {:error, err}

    assert [
             connect: [_],
             handle_close: [%Q{}, _, :state]
           ] = A.record(agent)
  end

  test "close logs error" do
    err = RuntimeError.exception("oops")

    stack = [
      {:ok, :state},
      {:error, err, :new_state}
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    log = fn entry ->
      assert %DBConnection.LogEntry{
               call: :close,
               query: %Q{},
               params: nil,
               result: {:error, ^err}
             } = entry

      assert is_integer(entry.pool_time)
      assert entry.pool_time >= 0
      assert is_integer(entry.connection_time)
      assert entry.connection_time >= 0
      assert is_nil(entry.decode_time)
      send(parent, :logged)
    end

    assert P.close(pool, %Q{}, log: log) == {:error, err}
    assert_received :logged

    assert [
             connect: [_],
             handle_close: [%Q{}, _, :state]
           ] = A.record(agent)
  end

  test "close logs raise" do
    stack = [
      fn opts ->
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      fn _, _, _ ->
        raise "oops"
      end,
      :ok,
      {:ok, :state2}
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    _ = Process.flag(:trap_exit, true)
    {:ok, pool} = P.start_link(opts)

    log = fn entry ->
      assert %DBConnection.LogEntry{call: :close, query: %Q{}, params: nil, result: {:error, err}} =
               entry

      assert %DBConnection.ConnectionError{
               message: "an exception was raised: ** (RuntimeError) oops" <> _
             } = err

      assert is_integer(entry.pool_time)
      assert entry.pool_time >= 0
      assert is_integer(entry.connection_time)
      assert entry.connection_time >= 0
      assert is_nil(entry.decode_time)
      send(parent, :logged)
    end

    assert_raise RuntimeError, "oops", fn -> P.close(pool, %Q{}, log: log) end
    assert_received :logged
    assert_receive {:EXIT, _, {%DBConnection.ConnectionError{}, [_ | _]}}

    assert [
             {:connect, [_]},
             {:handle_close, [%Q{}, _, :state]},
             {:disconnect, _} | _
           ] = A.record(agent)
  end

  test "close! error raises error" do
    err = RuntimeError.exception("oops")

    stack = [
      {:ok, :state},
      {:error, err, :new_state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert_raise RuntimeError, "oops", fn -> P.close!(pool, %Q{}) end

    assert [
             connect: [_],
             handle_close: [%Q{}, _, :state]
           ] = A.record(agent)
  end

  test "close in failed transaction" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, :result, :newer_state},
      {:ok, :rolledback, :newest_state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn conn ->
             assert P.transaction(conn, fn conn2 ->
                      P.rollback(conn2, :oops)
                    end) == {:error, :oops}

             assert P.close(conn, %Q{}, opts) == {:ok, :result}
           end) == {:error, :rollback}

    assert [
             connect: [_],
             handle_begin: [_, :state],
             handle_close: [%Q{}, _, :new_state],
             handle_rollback: [_, :newer_state]
           ] = A.record(agent)
  end
end
