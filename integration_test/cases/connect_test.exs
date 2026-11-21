defmodule ConnectTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

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

  for configure <- [:function, :mfa] do
    @configure configure

    for show_sensitive_data <- [false, true] do
      @show_sensitive_data show_sensitive_data

      test "backoff after #{@configure} configure refuses a connection with sensitive data #{@show_sensitive_data}" do
        err = RuntimeError.exception("temporarily unavailable")
        {:ok, agent} = A.start_link([{:ok, :state}])
        parent = self()

        configure = fn opts ->
          Process.put(:agent, opts[:agent])
          send(parent, {:configure, self(), System.monotonic_time(:millisecond)})

          if Process.put(:configured, true) do
            receive do
              :connect -> opts
            end
          else
            {:error, err}
          end
        end

        log =
          capture_log(fn ->
            {:ok, _pool} =
              P.start_link(
                agent: agent,
                configure: configure_callback(@configure, configure),
                backoff_type: :exp,
                backoff_min: 100,
                idle_interval: 60_000,
                connection_listeners: [self()],
                show_sensitive_data_on_connection_error: @show_sensitive_data
              )

            assert_receive {:configure, conn, first_attempt}
            assert_receive {:configure, ^conn, next_attempt}
            assert next_attempt - first_attempt >= 100
            assert A.record(agent) == []
            refute_received {:connected, ^conn}
            refute_received {:disconnected, ^conn}

            send(conn, :connect)
            assert_receive {:connected, ^conn}
            refute_received {:disconnected, ^conn}
            assert [connect: [opts]] = A.record(agent)
            assert Keyword.fetch!(opts, :agent) == agent
          end)

        assert log =~ "TestConnection ("
        assert log =~ ") failed to connect: ** (RuntimeError) temporarily unavailable"
      end

      test "backoff :stop exits when #{@configure} configure refuses with sensitive data #{@show_sensitive_data}" do
        err = RuntimeError.exception("temporarily unavailable")
        {:ok, agent} = A.start_link([{:ok, :state}])
        parent = self()
        Process.flag(:trap_exit, true)

        configure = fn opts ->
          Process.put(:agent, opts[:agent])
          Process.link(parent)
          send(parent, {:configure, self()})
          {:error, err}
        end

        log =
          capture_log(fn ->
            {:ok, _pool} =
              P.start_link(
                agent: agent,
                configure: configure_callback(@configure, configure),
                backoff_type: :stop,
                idle_interval: 60_000,
                connection_listeners: [self()],
                show_sensitive_data_on_connection_error: @show_sensitive_data
              )

            assert_receive {:configure, conn}
            assert_receive {:EXIT, ^conn, {^err, [_ | _]}}
            assert A.record(agent) == []
            refute_received {:connected, ^conn}
            refute_received {:disconnected, ^conn}
          end)

        assert log =~ ") failed to connect: ** (RuntimeError) temporarily unavailable"
      end

      test "#{@configure} configure still raises with sensitive data #{@show_sensitive_data}" do
        {:ok, agent} = A.start_link([])
        parent = self()
        Process.flag(:trap_exit, true)

        configure = fn _opts ->
          Process.link(parent)
          send(parent, {:configure, self()})
          raise MatchError, term: :password
        end

        {:ok, _pool} =
          P.start_link(
            agent: agent,
            configure: configure_callback(@configure, configure),
            show_sensitive_data_on_connection_error: @show_sensitive_data
          )

        assert_receive {:configure, conn}

        if @show_sensitive_data do
          assert_receive {:EXIT, ^conn, {%MatchError{term: :password}, _}}
        else
          assert_receive {:EXIT, ^conn, {%RuntimeError{message: message}, _}}
          assert message =~ "exception details are hidden"
          refute message =~ "password"
        end

        assert A.record(agent) == []
      end
    end

    test "#{@configure} configure still passes non-exception errors to connect" do
      parent = self()

      {:ok, agent} =
        A.start_link([
          fn opts ->
            send(parent, {:connect_opts, opts})
            {:ok, :state}
          end
        ])

      configure = fn opts ->
        Process.put(:agent, opts[:agent])
        {:error, :unavailable}
      end

      {:ok, _pool} =
        P.start_link(agent: agent, configure: configure_callback(@configure, configure))

      assert_receive {:connect_opts, {:error, :unavailable}}
      assert [connect: [{:error, :unavailable}]] = A.record(agent)
    end
  end

  def configure(opts, callback), do: callback.(opts)

  defp configure_callback(:function, callback), do: callback
  defp configure_callback(:mfa, callback), do: {__MODULE__, :configure, [callback]}

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
