defmodule ConnectTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q

  test "backoff after failed initial connection attempt" do
    err = RuntimeError.exception("oops")

    stack = [
      fn opts ->
        send(opts[:parent], {:error, self()})
        {:error, err}
      end,
      fn opts ->
        send(opts[:parent], {:hi, self()})
        {:ok, :state}
      end
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), backoff_min: 10]
    {:ok, _} = P.start_link(opts)
    assert_receive {:error, conn}
    assert_receive {:hi, ^conn}

    assert [
             connect: [opts2],
             connect: [opts2]
           ] = A.record(agent)
  end

  test "MatchError on connect doesn't leak sensitive data" do
    stack = [
      fn opts ->
        Process.link(Keyword.get(opts, :parent))
        raise MatchError, term: :password
      end,
      {:ok, :state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), backoff_min: 10]
    Process.flag(:trap_exit, true)
    {:ok, _} = P.start_link(opts)
    assert_receive {:EXIT, _, {%RuntimeError{}, [{_, _, 1, _} | _rest]}}
  end

  test "MatchError on connect leaks sensitive data when configured" do
    stack = [
      fn opts ->
        Process.link(Keyword.get(opts, :parent))
        raise MatchError, term: :password
      end,
      {:ok, :state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [
      agent: agent,
      parent: self(),
      backoff_min: 10,
      show_sensitive_data_on_connection_error: true
    ]

    Process.flag(:trap_exit, true)
    {:ok, _} = P.start_link(opts)
    assert_receive {:EXIT, _, {%MatchError{}, [{_, _, 1, _} | _rest]}}
  end

  test "ErlangError on connect doesn't leak sensitive data" do
    stack = [
      fn opts ->
        Process.link(Keyword.get(opts, :parent))
        :erlang.error(:badarg, [:password])
      end,
      {:ok, :state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), backoff_min: 10]
    Process.flag(:trap_exit, true)
    {:ok, _} = P.start_link(opts)
    assert_receive {:EXIT, _, {%RuntimeError{}, [{_, _, 1, _} | _rest]}}
  end

  test "ErlangError on connect leaks sensitive data when configured" do
    stack = [
      fn opts ->
        Process.link(Keyword.get(opts, :parent))
        :erlang.error(:badarg, [:password])
      end,
      {:ok, :state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [
      agent: agent,
      parent: self(),
      backoff_min: 10,
      show_sensitive_data_on_connection_error: true
    ]

    Process.flag(:trap_exit, true)
    {:ok, _} = P.start_link(opts)
    assert_receive {:EXIT, _, {%ArgumentError{}, [{_, _, [:password], _} | _rest]}}
  end

  test "lazy configure connection with module function and args" do
    stack = [
      fn opts ->
        send(opts[:parent], {:hello, opts[:ref]})
        {:ok, :state}
      end
    ]

    {:ok, agent} = A.start_link(stack)
    ref = make_ref()
    extra_opts = [parent: self(), ref: ref]
    opts = [agent: agent, configure: {Keyword, :merge, [extra_opts]}]
    {:ok, _} = P.start_link(opts)

    assert_receive {:hello, ^ref}
  end

  test "lazy configure connection with fun" do
    stack = [
      fn opts ->
        send(opts[:parent], {:hello, opts[:ref]})
        {:ok, :state}
      end
    ]

    {:ok, agent} = A.start_link(stack)
    ref = make_ref()
    extra_opts = [parent: self(), ref: ref]
    opts = [agent: agent, configure: &Keyword.merge(&1, extra_opts)]
    {:ok, _} = P.start_link(opts)

    assert_receive {:hello, ^ref}
  end

  test "backoff after disconnect and failed connection attempt" do
    err = RuntimeError.exception("oops")

    stack = [
      fn opts ->
        send(opts[:parent], {:hi1, self()})
        {:ok, :state}
      end,
      {:disconnect, err, :discon},
      :ok,
      {:error, err},
      fn opts ->
        send(opts[:parent], {:hi2, self()})
        {:ok, :reconnected}
      end
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), backoff_min: 10]
    {:ok, pool} = P.start_link(opts)
    assert P.close(pool, %Q{})
    assert_receive {:hi1, conn}
    assert_receive {:hi2, ^conn}

    assert [
             connect: [opts2],
             handle_close: _,
             disconnect: [^err, :discon],
             connect: [opts2],
             connect: [opts2]
           ] = A.record(agent)
  end

  test "backoff :stop exits on failed initial connection attempt" do
    err = RuntimeError.exception("oops")

    stack = [
      fn opts ->
        send(opts[:parent], {:error, self()})
        Process.link(opts[:parent])
        {:error, err}
      end,
      {:ok, :state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), backoff_type: :stop]
    Process.flag(:trap_exit, true)
    {:ok, _} = P.start_link(opts)
    assert_receive {:error, conn}
    assert_receive {:EXIT, ^conn, {^err, _}}

    assert [{:connect, _} | _] = A.record(agent)
  end

  test "backoff :stop exits after disconnect without attempting to connect" do
    err = RuntimeError.exception("oops")

    stack = [
      fn opts ->
        send(opts[:parent], {:hi, self()})
        Process.link(opts[:parent])
        {:ok, :state}
      end,
      {:disconnect, err, :discon},
      :ok,
      {:ok, :state}
    ]

    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), backoff_type: :stop]
    Process.flag(:trap_exit, true)
    {:ok, pool} = P.start_link(opts)
    assert P.close(pool, %Q{})

    assert_receive {:hi, conn}
    assert_receive {:EXIT, ^conn, {:shutdown, ^err}}

    assert [
             {:connect, [_]},
             {:handle_close, _} | _
           ] = A.record(agent)
  end

  test "backoff after failed after_connect" do
    stack = [
      {:ok, :state},
      {:idle, :state},
      :ok,
      {:ok, :state2},
      {:idle, :state},
      :ok
    ]

    {:ok, agent} = A.start_link(stack)

    parent = self()

    after_connect = fn _ ->
      send(parent, :after_connect)
      Process.exit(self(), :shutdown)
    end

    opts = [after_connect: after_connect, agent: agent, parent: self()]
    {:ok, _} = P.start_link(opts)

    assert_receive :after_connect
    refute_receive :after_connect, 50
    assert_receive :after_connect, 500

    assert [
             {:connect, [_]},
             {:handle_status, _},
             {:disconnect, [%DBConnection.ConnectionError{}, :state]},
             {:connect, [_]},
             {:handle_status, _} | _
           ] = A.record(agent)
  end
end
