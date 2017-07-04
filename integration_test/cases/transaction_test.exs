defmodule TransactionTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A

  test "begin/commit/rollback return result" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, :committed, :newer_state},
      {:ok, :rolledback, :newest_state},
      {:ok, :began, :state2},
      {:ok, :committed, :new_state2},
      {:ok, :rolledback, :newer_state2}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.begin(pool, opts) == {:ok, :began}
    assert P.commit(pool, opts) == {:ok, :committed}
    assert P.rollback(pool, opts) == {:ok, :rolledback}

    opts2 = [key: :value] ++ opts
    assert P.begin(pool, opts2) == {:ok, :began}
    assert P.commit(pool, opts2) == {:ok, :committed}
    assert P.rollback(pool, opts2) == {:ok, :rolledback}

    assert [
      connect: [_],
      handle_begin: [ _, :state],
      handle_commit: [_, :new_state],
      handle_rollback: [_, :newer_state],
      handle_begin: [[{:key, :value} | _], :newest_state],
      handle_commit: [[{:key, :value} | _], :state2],
      handle_rollback: [[{:key, :value} | _], :new_state2]
    ] = A.record(agent)
  end

  test "begin/commit/rollback log results" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, :committed, :newer_state},
      {:ok, :rolledback, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    log = &send(parent, &1)

    assert P.begin(pool, [log: log] ++ opts) == {:ok, :began}

    assert_received %DBConnection.LogEntry{call: :begin} = entry
    assert %{query: :begin, params: nil, result: {:ok, :began}} = entry
    assert is_integer(entry.pool_time)
    assert entry.pool_time >= 0
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert P.commit(pool, [log: log] ++ opts) == {:ok, :committed}

    assert_received %DBConnection.LogEntry{call: :commit} = entry
    assert %{query: :commit, params: nil, result: {:ok, :committed}} = entry
    assert is_integer(entry.pool_time)
    assert entry.pool_time >= 0
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert P.rollback(pool, [log: log] ++ opts) == {:ok, :rolledback}

    assert_received %DBConnection.LogEntry{call: :rollback} = entry
    assert %{query: :rollback, params: nil, result: {:ok, :rolledback}} = entry
    assert is_integer(entry.pool_time)
    assert entry.pool_time >= 0
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert [
      connect: [_],
      handle_begin: [ _, :state],
      handle_commit: [_, :new_state],
      handle_rollback: [_, :newer_state]] = A.record(agent)
  end

  test "begin!/commit!/rollback! error raises error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:error, err, :new_state},
      {:error, err, :newer_state},
      {:error, err, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_raise RuntimeError, "oops", fn() -> P.begin!(pool, opts) end
    assert_raise RuntimeError, "oops", fn() -> P.commit!(pool, opts) end
    assert_raise RuntimeError, "oops", fn() -> P.rollback!(pool, opts) end

    assert [
      connect: [_],
      handle_begin: [ _, :state],
      handle_commit: [_, :new_state],
      handle_rollback: [_, :newer_state]
      ] = A.record(agent)
  end

  test "begin/commit/rollback logs error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:error, err, :new_state},
      {:error, err, :newer_state},
      {:error, err, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    log = &send(parent, &1)

    assert P.begin(pool, [log: log] ++ opts) == {:error, err}

    assert_received %DBConnection.LogEntry{call: :begin} = entry
    assert %{query: :begin, params: nil, result: {:error, ^err}} = entry
    assert is_integer(entry.pool_time)
    assert entry.pool_time >= 0
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert P.commit(pool, [log: log] ++ opts) == {:error, err}

    assert_received %DBConnection.LogEntry{call: :commit} = entry
    assert %{query: :commit, params: nil, result: {:error, ^err}} = entry
    assert is_integer(entry.pool_time)
    assert entry.pool_time >= 0
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert P.rollback(pool, [log: log] ++ opts) == {:error, err}

    assert_received %DBConnection.LogEntry{call: :rollback} = entry
    assert %{query: :rollback, params: nil, result: {:error, ^err}} = entry
    assert is_integer(entry.pool_time)
    assert entry.pool_time >= 0
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_commit: [_, :new_state],
      handle_rollback: [_, :newer_state]
      ] = A.record(agent)
  end

  test "begin! disconnect raises error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:disconnect, err, :new_state},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :newest_state}
      end
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_raise RuntimeError, "oops", fn() -> P.begin!(pool, opts) end

    assert_receive :reconnected

    assert [
      connect: [_],
      handle_begin: [_, :state],
      disconnect: [_, :new_state],
      connect: [_]] = A.record(agent)
  end

  test "begin bad return raises and stops connection" do
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
    assert_receive {:hi, pid}

    Process.flag(:trap_exit, true)
    assert_raise DBConnection.ConnectionError, "bad return value: :oops",
      fn() -> P.begin(pool, opts) end

    prefix = "client #{inspect self()} stopped: " <>
      "** (DBConnection.ConnectionError) bad return value: :oops"
    len = byte_size(prefix)
    assert_receive {:EXIT, ^pid,
      {%DBConnection.ConnectionError{message: <<^prefix::binary-size(len), _::binary>>},
        [_|_]}}

    assert [
      {:connect, _},
      {:handle_begin, [_, :state]}| _] = A.record(agent)
  end

  test "begin raise raises and stops connection" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      fn(_, _) ->
        raise "oops"
      end,
      {:ok, :state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, pid}

    Process.flag(:trap_exit, true)
    assert_raise RuntimeError, "oops",
      fn() -> P.begin(pool, opts) end

    prefix = "client #{inspect self()} stopped: ** (RuntimeError) oops"
    len = byte_size(prefix)
    assert_receive {:EXIT, ^pid,
      {%DBConnection.ConnectionError{message: <<^prefix::binary-size(len), _::binary>>},
       [_|_]}}

    assert [
      {:connect, _},
      {:handle_begin, [_, :state]} | _] = A.record(agent)
  end

  test "run begin!/commit!/rollback! error raises error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:error, err, :new_state},
      {:error, err, :newer_state},
      {:error, err, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.run(pool, fn(conn) ->
      assert_raise RuntimeError, "oops", fn() -> P.begin!(conn, opts) end
      assert_raise RuntimeError, "oops", fn() -> P.commit!(conn, opts) end
      assert_raise RuntimeError, "oops", fn() -> P.rollback!(conn, opts) end

      :hi
    end) == :hi

    assert [
      connect: [_],
      handle_begin: [ _, :state],
      handle_commit: [_, :new_state],
      handle_rollback: [_, :newer_state]
      ] = A.record(agent)
  end

  test "run begin/commit/rollback logs error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:error, err, :new_state},
      {:error, err, :newer_state},
      {:error, err, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    log = &send(parent, &1)

    assert P.run(pool, fn(conn) ->
      assert P.begin(conn, [log: log] ++ opts) == {:error, err}

      :hi
    end) == :hi

    assert_received %DBConnection.LogEntry{call: :begin} = entry
    assert %{query: :begin, params: nil, result: {:error, ^err}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert P.run(pool, fn(conn) ->
      assert P.commit(conn, [log: log] ++ opts) == {:error, err}

      :hi
    end) == :hi

    assert_received %DBConnection.LogEntry{call: :commit} = entry
    assert %{query: :commit, params: nil, result: {:error, ^err}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert P.run(pool, fn(conn) ->
      assert P.rollback(conn, [log: log] ++ opts) == {:error, err}

      :hi
    end) == :hi

    assert_received %DBConnection.LogEntry{call: :rollback} = entry
    assert %{query: :rollback, params: nil, result: {:error, ^err}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_commit: [_, :new_state],
      handle_rollback: [_, :newer_state]
      ] = A.record(agent)
  end

  test "run begin! disconnect raises error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:disconnect, err, :new_state},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :newest_state}
      end
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert P.run(pool, fn(conn) ->
      assert_raise RuntimeError, "oops", fn() -> P.begin!(conn, opts) end

      :hi
    end) == :hi

    assert_receive :reconnected

    assert [
      connect: [_],
      handle_begin: [_, :state],
      disconnect: [_, :new_state],
      connect: [_]] = A.record(agent)
  end

  test "run begin bad return raises and stops connection" do
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
    assert_receive {:hi, pid}

    Process.flag(:trap_exit, true)

    assert P.run(pool, fn(conn) ->
      assert_raise DBConnection.ConnectionError, "bad return value: :oops",
        fn() -> P.begin(conn, opts) end

      :hi
    end) == :hi

    prefix = "client #{inspect self()} stopped: " <>
      "** (DBConnection.ConnectionError) bad return value: :oops"
    len = byte_size(prefix)
    assert_receive {:EXIT, ^pid,
      {%DBConnection.ConnectionError{message: <<^prefix::binary-size(len), _::binary>>},
        [_|_]}}

    assert [
      {:connect, _},
      {:handle_begin, [_, :state]}| _] = A.record(agent)
  end

  test "run begin raise raises and stops connection" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      fn(_, _) ->
        raise "oops"
      end,
      {:ok, :state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, pid}

    Process.flag(:trap_exit, true)

    assert P.run(pool, fn(conn) ->
      assert_raise RuntimeError, "oops",
        fn() -> P.begin(conn, opts) end

      :hi
    end) == :hi

    prefix = "client #{inspect self()} stopped: ** (RuntimeError) oops"
    len = byte_size(prefix)
    assert_receive {:EXIT, ^pid,
      {%DBConnection.ConnectionError{message: <<^prefix::binary-size(len), _::binary>>},
       [_|_]}}

    assert [
      {:connect, _},
      {:handle_begin, [_, :state]} | _] = A.record(agent)
  end

  test "checkout_begin returns result and conn" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, :began, :newer_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert {:ok, conn, :began} = P.checkout_begin(pool, opts)
    assert P.checkin(conn, opts) == :ok


    opts2 = [key: :value] ++ opts
    assert {:ok, conn, :began} = P.checkout_begin(pool, opts2)
    assert P.checkin(conn, opts) == :ok

    assert [
      connect: [_],
      handle_begin: [ _, :state],
      handle_begin: [[{:key, :value} | _], :new_state],
    ] = A.record(agent)
  end

  test "commit_checkin returns result" do
    stack = [
      {:ok, :state},
      {:ok, :committed, :new_state},
      {:ok, :committed, :newer_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert {:ok, conn} = P.checkout(pool, opts)
    assert P.commit_checkin(conn, opts) == {:ok, :committed}


    opts2 = [key: :value] ++ opts
    assert {:ok, conn} = P.checkout(pool, opts2)
    assert P.commit_checkin(conn, opts2) == {:ok, :committed}

    assert [
      connect: [_],
      handle_commit: [ _, :state],
      handle_commit: [[{:key, :value} | _], :new_state],
    ] = A.record(agent)
  end

  test "rollback_checkin returns result" do
    stack = [
      {:ok, :state},
      {:ok, :rolledback, :new_state},
      {:ok, :rolledback, :newer_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert {:ok, conn} = P.checkout(pool, opts)
    assert P.rollback_checkin(conn, opts) == {:ok, :rolledback}


    opts2 = [key: :value] ++ opts
    assert {:ok, conn} = P.checkout(pool, opts2)
    assert P.rollback_checkin(conn, opts2) == {:ok, :rolledback}

    assert [
      connect: [_],
      handle_rollback: [ _, :state],
      handle_rollback: [[{:key, :value} | _], :new_state],
    ] = A.record(agent)
  end

  test "checkout_begin/commit_checkin/rollback_checkin log results" do
    stack = [
      {:ok, :state},
      {:ok, :began, :new_state},
      {:ok, :committed, :newer_state},
      {:ok, :rolledback, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    log = &send(parent, &1)

    assert {:ok, conn, :began} = P.checkout_begin(pool, [log: log] ++ opts)

    assert_received %DBConnection.LogEntry{call: :checkout_begin} = entry
    assert %{query: :begin, params: nil, result: {:ok, ^conn, :began}} = entry
    assert is_integer(entry.pool_time)
    assert entry.pool_time >= 0
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert P.commit_checkin(conn, [log: log] ++ opts) == {:ok, :committed}

    assert_received %DBConnection.LogEntry{call: :commit_checkin} = entry
    assert %{query: :commit, params: nil, result: {:ok, :committed}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert {:ok, conn} = P.checkout(pool, opts)

    assert P.rollback_checkin(conn, [log: log] ++ opts) == {:ok, :rolledback}

    assert_received %DBConnection.LogEntry{call: :rollback_checkin} = entry
    assert %{query: :rollback, params: nil, result: {:ok, :rolledback}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert [
      connect: [_],
      handle_begin: [ _, :state],
      handle_commit: [_, :new_state],
      handle_rollback: [_, :newer_state]] = A.record(agent)
  end

  test "checkout_begin!/commit_checkin!/rollback_checkin! error raises error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:error, err, :new_state},
      {:error, err, :newer_state},
      {:error, err, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_raise RuntimeError, "oops", fn() -> P.checkout_begin!(pool, opts) end

    assert {:ok, conn} = P.checkout(pool, opts)
    assert_raise RuntimeError, "oops", fn() -> P.commit!(conn, opts) end
    assert_raise RuntimeError, "oops", fn() -> P.rollback!(conn, opts) end

    assert :ok = P.checkin(conn, opts)

    assert [
      connect: [_],
      handle_begin: [ _, :state],
      handle_commit: [_, :new_state],
      handle_rollback: [_, :newer_state]
      ] = A.record(agent)
  end

  test "checkout_begin/checkout_commit/checkout_rollback logs error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:error, err, :new_state},
      {:error, err, :newer_state},
      {:error, err, :newest_state}
      ]
    {:ok, agent} = A.start_link(stack)

    parent = self()
    opts = [agent: agent, parent: parent]
    {:ok, pool} = P.start_link(opts)

    log = &send(parent, &1)

    assert P.checkout_begin(pool, [log: log] ++ opts) == {:error, err}

    assert_received %DBConnection.LogEntry{call: :checkout_begin} = entry
    assert %{query: :begin, params: nil, result: {:error, ^err}} = entry
    assert is_integer(entry.pool_time)
    assert entry.pool_time >= 0
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert {:ok, conn} = P.checkout(pool, opts)
    assert P.commit_checkin(conn, [log: log] ++ opts) == {:error, err}

    assert_received %DBConnection.LogEntry{call: :commit_checkin} = entry
    assert %{query: :commit, params: nil, result: {:error, ^err}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert P.rollback_checkin(conn, [log: log] ++ opts) == {:error, err}

    assert_received %DBConnection.LogEntry{call: :rollback_checkin} = entry
    assert %{query: :rollback, params: nil, result: {:error, ^err}} = entry
    assert is_nil(entry.pool_time)
    assert is_integer(entry.connection_time)
    assert entry.connection_time >= 0
    assert is_nil(entry.decode_time)

    assert :ok = P.checkin(conn, opts)

    assert [
      connect: [_],
      handle_begin: [_, :state],
      handle_commit: [_, :new_state],
      handle_rollback: [_, :newer_state]
      ] = A.record(agent)
  end

  test "checkout_begin! disconnect raises error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:disconnect, err, :new_state},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :newest_state}
      end
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    assert_raise RuntimeError, "oops", fn() -> P.checkout_begin!(pool, opts) end

    assert_receive :reconnected

    assert [
      connect: [_],
      handle_begin: [_, :state],
      disconnect: [_, :new_state],
      connect: [_]] = A.record(agent)
  end

  test "commit_checkin! disconnect raises error" do
    err = RuntimeError.exception("oops")
    stack = [
      {:ok, :state},
      {:disconnect, err, :new_state},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :newest_state}
      end
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)

    {:ok, conn} = P.checkout(pool, opts)
    assert_raise RuntimeError, "oops", fn() -> P.commit_checkin!(conn, opts) end

    assert_receive :reconnected

    assert [
      connect: [_],
      handle_commit: [_, :state],
      disconnect: [_, :new_state],
      connect: [_]] = A.record(agent)
  end

  test "checkout_begin bad return raises and stops connection" do
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
    assert_receive {:hi, pid}

    Process.flag(:trap_exit, true)
    assert_raise DBConnection.ConnectionError, "bad return value: :oops",
      fn() -> P.checkout_begin(pool, opts) end

    prefix = "client #{inspect self()} stopped: " <>
      "** (DBConnection.ConnectionError) bad return value: :oops"
    len = byte_size(prefix)
    assert_receive {:EXIT, ^pid,
      {%DBConnection.ConnectionError{message: <<^prefix::binary-size(len), _::binary>>},
        [_|_]}}

    assert [
      {:connect, _},
      {:handle_begin, [_, :state]}| _] = A.record(agent)
  end

  test "rollback_checkin bad return raises and stops connection" do
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
    assert_receive {:hi, pid}

    assert {:ok, conn} = P.checkout(pool, opts)
    Process.flag(:trap_exit, true)
    assert_raise DBConnection.ConnectionError, "bad return value: :oops",
      fn() -> P.rollback_checkin(conn, opts) end

    prefix = "client #{inspect self()} stopped: " <>
      "** (DBConnection.ConnectionError) bad return value: :oops"
    len = byte_size(prefix)
    assert_receive {:EXIT, ^pid,
      {%DBConnection.ConnectionError{message: <<^prefix::binary-size(len), _::binary>>},
        [_|_]}}

    assert [
      {:connect, _},
      {:handle_rollback, [_, :state]}| _] = A.record(agent)
  end

  test "checkout_begin raise raises and stops connection" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      fn(_, _) ->
        raise "oops"
      end,
      {:ok, :state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, pid}

    Process.flag(:trap_exit, true)
    assert_raise RuntimeError, "oops",
      fn() -> P.checkout_begin(pool, opts) end

    prefix = "client #{inspect self()} stopped: ** (RuntimeError) oops"
    len = byte_size(prefix)
    assert_receive {:EXIT, ^pid,
      {%DBConnection.ConnectionError{message: <<^prefix::binary-size(len), _::binary>>},
       [_|_]}}

    assert [
      {:connect, _},
      {:handle_begin, [_, :state]} | _] = A.record(agent)
  end

  test "commit_checkin raise raises and stops connection" do
    stack = [
      fn(opts) ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      fn(_, _) ->
        raise "oops"
      end,
      {:ok, :state}
      ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    {:ok, pool} = P.start_link(opts)
    assert_receive {:hi, pid}

    assert {:ok, conn} = P.checkout(pool, opts)
    Process.flag(:trap_exit, true)
    assert_raise RuntimeError, "oops",
      fn() -> P.commit_checkin(conn, opts) end

    prefix = "client #{inspect self()} stopped: ** (RuntimeError) oops"
    len = byte_size(prefix)
    assert_receive {:EXIT, ^pid,
      {%DBConnection.ConnectionError{message: <<^prefix::binary-size(len), _::binary>>},
       [_|_]}}

    assert [
      {:connect, _},
      {:handle_commit, [_, :state]} | _] = A.record(agent)
  end
end
