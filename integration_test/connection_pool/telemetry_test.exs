defmodule TestPoolTelemetry do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestResult, as: R

  @events [
    [:db_connection, :enqueue],
    [:db_connection, :dequeue],
    [:db_connection, :checkout],
    [:db_connection, :checkin],
    [:db_connection, :drop_idle]
  ]

  setup do
    telemetry_ref = make_ref()

    :telemetry.attach_many(
      telemetry_ref,
      @events,
      fn event, measurements, metadata, {ref, dest_pid} ->
        send(dest_pid, {event, ref, measurements, metadata})
      end,
      {telemetry_ref, self()}
    )

    on_exit(fn ->
      :telemetry.detach(telemetry_ref)
    end)

    %{telemetry_ref: telemetry_ref}
  end

  test "checkin/checkout telemetry when ready", %{telemetry_ref: telemetry_ref} do
    stack = [
      {:ok, :state},
      {:ok, %Q{}, %R{}, :state}
    ]

    {:ok, agent} = A.start_link(stack)
    {:ok, pool} = P.start_link(agent: agent, parent: self())

    assert_receive {[:db_connection, :checkin], ^telemetry_ref, %{count: 1},
                    %{checkout_queue_length: 0, ready_conn_count: 1}}

    assert P.execute(pool, %Q{}, [:client])

    assert_receive {[:db_connection, :checkout], ^telemetry_ref, %{count: 1},
                    %{checkout_queue_length: 0, ready_conn_count: 0}}

    assert_receive {[:db_connection, :checkin], ^telemetry_ref, %{count: 1},
                    %{checkout_queue_length: 0, ready_conn_count: 1}}

    assert [
             connect: [_],
             handle_execute: [%Q{}, [:client], _, :state]
           ] = A.record(agent)
  end

  test "idle telemetry on idle timeout", %{telemetry_ref: telemetry_ref} do
    stack = [
      {:ok, :state},
      {:ok, :state}
    ]

    idle_interval = 100

    {:ok, agent} = A.start_link(stack)
    {:ok, _} = P.start_link(agent: agent, parent: self(), idle_interval: idle_interval)

    assert_receive {[:db_connection, :checkin], ^telemetry_ref, %{count: 1},
                    %{checkout_queue_length: 0, ready_conn_count: 1}}

    assert_receive {[:db_connection, :drop_idle], ^telemetry_ref, %{count: 1},
                    %{checkout_queue_length: 0, ready_conn_count: 0}},
                   idle_interval

    assert [{:connect, [_]} | _] = A.record(agent)
  end

  test "enqueue/dequeue telemetry when busy", %{telemetry_ref: telemetry_ref} do
    stack = [
      {:ok, :state},
      {:ok, :state},
      fn _, _, _, _ ->
        receive do
          :continue -> {:ok, %Q{}, %R{}, :state}
        end
      end,
      fn _, _, _, _ ->
        receive do
          :continue -> {:ok, %Q{}, %R{}, :state}
        end
      end,
      fn _, _, _, _ ->
        receive do
          :continue -> {:ok, %Q{}, %R{}, :state}
        end
      end
    ]

    {:ok, agent} = A.start_link(stack)
    {:ok, pool} = P.start_link(agent: agent, parent: self(), pool_size: 2)

    assert_receive {[:db_connection, :checkin], ^telemetry_ref, %{count: 1},
                    %{checkout_queue_length: 0, ready_conn_count: 1}}

    assert_receive {[:db_connection, :checkin], ^telemetry_ref, %{count: 1},
                    %{checkout_queue_length: 0, ready_conn_count: 2}}

    fn ->
      spawn_link(fn ->
        Process.put(:agent, agent)
        assert P.execute(pool, %Q{}, [:client])
      end)
    end
    |> Stream.repeatedly()
    |> Enum.take(3)
    |> Enum.each(fn pid ->
      send(pid, :continue)
    end)

    assert_receive {[:db_connection, :checkout], ^telemetry_ref, %{count: 1},
                    %{checkout_queue_length: 0, ready_conn_count: 1}}

    assert_receive {[:db_connection, :checkout], ^telemetry_ref, %{count: 1},
                    %{checkout_queue_length: 0, ready_conn_count: 0}}

    assert_receive {[:db_connection, :enqueue], ^telemetry_ref, %{count: 1},
                    %{checkout_queue_length: 1, ready_conn_count: 0}}

    assert_receive {[:db_connection, :dequeue], ^telemetry_ref, %{count: 1},
                    %{checkout_queue_length: 0, ready_conn_count: 0}}

    assert_receive {[:db_connection, :checkout], ^telemetry_ref, %{count: 1},
                    %{checkout_queue_length: 0, ready_conn_count: 0}}

    assert_receive {[:db_connection, :checkin], ^telemetry_ref, %{count: 1},
                    %{checkout_queue_length: 0, ready_conn_count: 1}}

    assert_receive {[:db_connection, :checkin], ^telemetry_ref, %{count: 1},
                    %{checkout_queue_length: 0, ready_conn_count: 2}}

    assert [
             connect: [_],
             connect: [_],
             handle_execute: [%Q{}, [:client], _, :state],
             handle_execute: [%Q{}, [:client], _, :state],
             handle_execute: [%Q{}, [:client], _, :state]
           ] = A.record(agent)
  end
end
