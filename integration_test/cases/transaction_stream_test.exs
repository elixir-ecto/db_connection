defmodule TransactionStreamTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestCursor, as: C
  alias TestResult, as: R

  test "stream returns result" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %C{}, :newer_state},
      {:cont, %R{}, :newest_state},
      {:done, %R{}, :state2},
      {:ok, :committed, :new_state2}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.transaction(pool, fn(conn) ->
      stream = P.stream(conn, %Q{}, [:param])
      assert %DBConnection.Stream{} = stream
      assert Enum.to_list(stream) == [%R{}, %R{}]
      :hi
    end) == {:ok, :hi}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_open: [%Q{}, [:param], _, :new_state],
      handle_fetch: [%Q{}, %C{}, _, :newer_state],
      handle_fetch: [%Q{}, %C{}, _, :newest_state],
      handle_commit: [_, :state2]
      ] = A.record(agent)
  end

  test "halted stream closes and returns result" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %C{}, :newer_state},
      {:cont, %R{}, :newest_state},
      {:ok, :result, :state2},
      {:ok, :committed, :new_state2}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert P.transaction(pool, fn(conn) ->
      stream = P.stream(conn, %Q{}, [:param])
      assert Enum.take(stream, 1) == [%R{}]
      :hi
    end) == {:ok, :hi}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_open: [%Q{}, [:param], _, :new_state],
      handle_fetch: [%Q{}, %C{}, _, :newer_state],
      handle_close: [%Q{}, %C{}, _, :newest_state],
      handle_commit: [_, :state2]
      ] = A.record(agent)
  end

  test "stream logs result" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:error, err, :newer_state},
      {:ok, :comitted, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn(conn) ->
      stream = P.stream(conn, %Q{}, [:param])
      assert_raise RuntimeError, "oops",  fn() -> Enum.take(stream, 1) end
      :hi
    end) == {:ok, :hi}

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_open: [%Q{}, [:param], _, :new_state],
      handle_commit: [_, :newer_state],
      ] = A.record(agent)
  end

  test "stream open disconnects" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:disconnect, err, :newer_state},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :state2}
      end
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn(conn) ->
      stream = P.stream(conn, %Q{}, [:param])
      assert_raise RuntimeError, "oops",  fn() -> Enum.take(stream, 1) end
      :hi
    end) == {:error, :rollback}

    assert_receive :reconnected

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_open: [%Q{}, [:param], _, :new_state],
      disconnect: [^err, :newer_state],
      connect: [_]
      ] = A.record(agent)
  end

  test "stream fetch disconnects" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %C{}, :newer_state},
      {:disconnect, err, :newest_state},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :state2}
      end
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn(conn) ->
      stream = P.stream(conn, %Q{}, [:param])
      assert_raise RuntimeError, "oops",  fn() -> Enum.take(stream, 1) end
      :hi
    end) == {:error, :rollback}

    assert_receive :reconnected

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_open: [%Q{}, [:param], _, :new_state],
      handle_fetch: [%Q{}, %C{}, _, :newer_state],
      disconnect: [^err, :newest_state],
      connect: [_]
      ] = A.record(agent)
  end

  test "stream close disconnect" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, %C{}, :newer_state},
      {:cont, %R{}, :newest_state},
      {:disconnect, err, :state2},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :state2}
      end
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    assert P.transaction(pool, fn(conn) ->
      stream = P.stream(conn, %Q{}, [:param])
      assert_raise RuntimeError, "oops",  fn() -> Enum.take(stream, 1) end
      :hi
    end) == {:error, :rollback}

    assert_receive :reconnected

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_open: [%Q{}, [:param], _, :new_state],
      handle_fetch: [%Q{}, %C{}, _, :newer_state],
      handle_close: [%Q{}, %C{}, _, :newest_state],
      disconnect: [^err, :state2],
      connect: [_]
      ] = A.record(agent)
  end

  test "stream open bad return raises and stops" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, :began, :new_state},
      :oops,
      {:ok, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}

    Process.flag(:trap_exit, true)
    assert P.transaction(pool, fn(conn) ->
      stream = P.stream(conn, %Q{}, [:param])
      assert_raise DBConnection.ConnectionError, "bad return value: :oops",
        fn() -> Enum.to_list(stream) end
      :hi
    end) == {:error, :rollback}

    prefix = "client #{inspect self()} stopped: " <>
      "** (DBConnection.ConnectionError) bad return value: :oops"
    len = byte_size(prefix)
    assert_receive {:EXIT, ^conn,
      {%DBConnection.ConnectionError{message: <<^prefix::binary-size(len), _::binary>>},
        [_|_]}}

    assert [
      {:connect, _},
      {:handle_begin, [_, :state]},
      {:handle_open, [%Q{}, [:param], _, :new_state]} | _] = A.record(agent)
  end

  test "stream open raise raises and stops connection" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, :began, :new_state},
      fn(_, _, _, _) ->
        raise "oops"
      end,
      {:ok, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}

    Process.flag(:trap_exit, true)
    assert P.transaction(pool, fn(conn) ->
      stream = P.stream(conn, %Q{}, [:param])
      assert_raise RuntimeError, "oops", fn() -> Enum.to_list(stream) end
      :hi
    end) == {:error, :rollback}

    prefix = "client #{inspect self()} stopped: ** (RuntimeError) oops"
    len = byte_size(prefix)
    assert_receive {:EXIT, ^conn,
      {%DBConnection.ConnectionError{message: <<^prefix::binary-size(len), _::binary>>},
        [_|_]}}

    assert [
      {:connect, _},
      {:handle_begin, [_, :state]},
      {:handle_open, [%Q{}, [:param], _, :new_state]} | _] = A.record(agent)
  end

  test "stream fetch bad return raises and stops" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, :began, :new_state},
      {:ok, %C{}, :newer_state},
      :oops,
      {:ok, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}

    Process.flag(:trap_exit, true)
    assert P.transaction(pool, fn(conn) ->
      stream = P.stream(conn, %Q{}, [:param])
      assert_raise DBConnection.ConnectionError, "bad return value: :oops",
        fn() -> Enum.to_list(stream) end
      :hi
    end) == {:error, :rollback}

    prefix = "client #{inspect self()} stopped: " <>
      "** (DBConnection.ConnectionError) bad return value: :oops"
    len = byte_size(prefix)
    assert_receive {:EXIT, ^conn,
      {%DBConnection.ConnectionError{message: <<^prefix::binary-size(len), _::binary>>},
        [_|_]}}

    assert [
      {:connect, _},
      {:handle_begin, [_, :state]},
      {:handle_open, [%Q{}, [:param], _, :new_state]},
      {:handle_fetch, [%Q{}, %C{}, _, :newer_state]} | _] = A.record(agent)
  end

  test "stream close raise raises and stops connection" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:ok, :began, :new_state},
      {:ok, %C{}, :newer_state},
      {:cont, %R{}, :newest_state},
      fn(_, _, _, _) ->
        raise "oops"
      end,
      {:ok, :state2}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, conn}

    Process.flag(:trap_exit, true)
    assert P.transaction(pool, fn(conn) ->
      stream = P.stream(conn, %Q{}, [:param])
      assert_raise RuntimeError, "oops", fn() -> Enum.take(stream, 1) end
      :hi
    end) == {:error, :rollback}

    prefix = "client #{inspect self()} stopped: ** (RuntimeError) oops"
    len = byte_size(prefix)
    assert_receive {:EXIT, ^conn,
      {%DBConnection.ConnectionError{message: <<^prefix::binary-size(len), _::binary>>},
        [_|_]}}

    assert [
      {:connect, _},
      {:handle_begin, [_, :state]},
      {:handle_open, [%Q{}, [:param], _, :new_state]},
      {:handle_fetch, [%Q{}, %C{}, _, :newer_state]},
      {:handle_close, [%Q{}, %C{}, _, :newest_state]} | _] = A.record(agent)
  end
end
