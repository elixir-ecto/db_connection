defmodule RunQueryTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestResult, as: R

  test "query returns result" do
    stack = [
      {:ok, :state},
      {:ok, %R{}, :new_state},
      {:ok, %R{}, :new_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.run(pool, fn(conn) ->
      assert P.query(conn, %Q{}) == {:ok, %R{}}
      assert P.query(conn, %Q{}, [key: :value]) == {:ok, %R{}}
      :hi
    end) == {:ok, :hi}

    assert [
      connect: [_],
      handle_query: [%Q{}, _, :state],
      handle_query: [%Q{}, [{:key, :value} | _], :new_state]] = A.record(agent)
  end

  test "query error returns error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:error, err, :new_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.run(pool, fn(conn) ->
      assert P.query(conn, %Q{}) == {:error, err}
      :hi
    end) == {:ok, :hi}

    assert [
      connect: [_],
      handle_query: [%Q{}, _, :state]] = A.record(agent)
  end

  test "query! error raises error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:error, err, :new_state},
      {:ok,  %R{}, :newer_state},
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.run(pool, fn(conn) ->
      assert_raise RuntimeError, "oops", fn() -> P.query!(conn, %Q{}) end

      assert P.query(conn, %Q{}) == {:ok, %R{}}
      :hi
    end) == {:ok, :hi}

    assert [
      connect: [_],
      handle_query: [%Q{}, _, :state],
      handle_query: [%Q{}, _, :new_state]] = A.record(agent)
  end

  test "query disconnect returns error" do
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

    assert P.run(pool, fn(conn) ->
      assert P.query(conn, %Q{}) == {:error, err}

      assert_raise DBConnection.Error, "connection is closed",
        fn() -> P.query(conn, %Q{}) end

      :hi
    end) == {:ok, :hi}

    assert_receive :reconnected

    assert [
      connect: [opts2],
      handle_query: [%Q{}, _, :state],
      disconnect: [^err, :new_state],
      connect: [opts2]] = A.record(agent)
  end

  test "query bad return raises DBConnection.Error and stops connection" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      :oops,
      {:ok, :state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}

    Process.flag(:trap_exit, true)

    assert P.run(pool, fn(conn) ->
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
      {:handle_query, [%Q{}, _, :state]}| _] = A.record(agent)
  end

  test "query raise raises and stops connection" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
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

    assert P.run(pool, fn(conn) ->
      assert_raise RuntimeError, "oops", fn() -> P.query(conn, %Q{}) end

      assert_raise DBConnection.Error, "connection is closed",
        fn() -> P.query(conn, %Q{}) end

      :hi
    end) == {:ok, :hi}

    assert_receive {:EXIT, ^conn,
      {%DBConnection.Error{message: "client stopped: " <> _}, [_|_]}}

    assert [
      {:connect, _},
      {:handle_query, [%Q{}, _, :state]}| _] = A.record(agent)
  end
end
