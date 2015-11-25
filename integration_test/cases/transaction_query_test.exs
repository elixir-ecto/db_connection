defmodule TransactionQueryTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestResult, as: R

  test "query returns result" do
    stack = [
      {:ok, :state},
      {:ok, :new_state},
      {:ok, %R{}, :newer_state},
      {:ok, %R{}, :newest_state},
      {:ok, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.transaction(pool, fn(conn) ->
      assert P.query(conn, %Q{}) == {:ok, %R{}}
      assert P.query(conn, %Q{}, [key: :value]) == {:ok, %R{}}
      :hi
    end) == {:ok, :hi}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_query: [%Q{}, _, :new_state],
      handle_query: [%Q{}, [{:key, :value} | _], :newer_state],
      handle_commit: [_, :newest_state]] = A.record(agent)
  end

  test "query error returns error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, :new_state},
      {:error, err, :newer_state},
      {:ok, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn(conn) ->
      assert P.query(conn, %Q{}) == {:error, err}
      :hi
    end) == {:ok, :hi}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_query: [%Q{}, _, :new_state],
      handle_commit: [_, :newer_state]] = A.record(agent)
  end

  test "query! error raises error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, :new_state},
      {:error, err, :newer_state},
      {:ok,  %R{}, :newest_state},
      {:ok, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn(conn) ->
      assert_raise RuntimeError, "oops", fn() -> P.query!(conn, %Q{}) end

      assert P.query(conn, %Q{}) == {:ok, %R{}}
      :hi
    end) == {:ok, :hi}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_query: [%Q{}, _, :new_state],
      handle_query: [%Q{}, _, :newer_state],
      handle_commit: [_, :newest_state]] = A.record(agent)
  end

  test "query disconnect returns error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, :new_state},
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

    assert P.transaction(pool, fn(conn) ->
      assert P.query(conn, %Q{}) == {:error, err}

      assert_raise DBConnection.Error, "connection is closed",
        fn() -> P.query(conn, %Q{}) end

      :hi
    end) == {:ok, :hi}

    assert_receive :reconnected

    assert [
      connect: [opts2],
      handle_begin: [_, :state],
      handle_query: [%Q{}, _, :new_state],
      disconnect: [^err, :newer_state],
      connect: [opts2]] = A.record(agent)
  end

  test "query bad return raises DBConnection.Error and stops connection" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, :new_state},
      :oops,
      {:ok, :state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}

    Process.flag(:trap_exit, true)

    assert P.transaction(pool, fn(conn) ->
      assert_raise DBConnection.Error, "bad return value: :oops",
        fn() -> P.query(conn, %Q{}) end

      assert_raise DBConnection.Error, "connection is closed",
        fn() -> P.query(conn, %Q{}) end

      :hi
    end) == {:ok, :hi}

    assert_receive {:EXIT, ^conn,
      {%DBConnection.Error{message: "client stopped: " <> _}, [_|_]}}

    assert [
      {:connect, _},
      {:handle_begin, [_, :state]},
      {:handle_query, [%Q{}, _, :new_state]} | _] = A.record(agent)
  end

  test "query raise raises and stops connection" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, :new_state},
      fn(_, _, _) ->
        raise "oops"
      end,
      {:ok, :state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}

    Process.flag(:trap_exit, true)

    assert P.transaction(pool, fn(conn) ->
      assert_raise RuntimeError, "oops", fn() -> P.query(conn, %Q{}) end

      assert_raise DBConnection.Error, "connection is closed",
        fn() -> P.query(conn, %Q{}) end

      :hi
    end) == {:ok, :hi}

    assert_receive {:EXIT, ^conn,
      {%DBConnection.Error{message: "client stopped: " <> _}, [_|_]}}

    assert [
      {:connect, _},
      {:handle_begin, [_, :state]},
      {:handle_query, [%Q{}, _, :new_state]}| _] = A.record(agent)
  end
end
