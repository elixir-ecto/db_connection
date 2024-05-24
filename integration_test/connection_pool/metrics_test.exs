defmodule TestPoolMetrics do
  import TestHelpers

  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestResult, as: R

  test "considers checkin/checkout in metrics when ready" do
    stack = [
      {:ok, :state},
      fn _, _, _, _ ->
        receive do
          :continue -> {:ok, %Q{}, %R{}, :state}
        end
      end
    ]

    {:ok, agent} = A.start_link(stack)
    {:ok, pool} = P.start_link(agent: agent, parent: self())

    poll(fn ->
      assert [%{source: {:pool, ^pool}, checkout_queue_length: 0, ready_conn_count: 1}] =
               DBConnection.get_connection_metrics(pool)
    end)

    query =
      spawn_link(fn ->
        Process.put(:agent, agent)
        assert P.execute(pool, %Q{}, [:client])
      end)

    poll(fn ->
      assert [%{source: {:pool, ^pool}, checkout_queue_length: 0, ready_conn_count: 0}] =
               DBConnection.get_connection_metrics(pool)
    end)

    send(query, :continue)

    poll(fn ->
      assert [%{source: {:pool, ^pool}, checkout_queue_length: 0, ready_conn_count: 1}] =
               DBConnection.get_connection_metrics(pool)
    end)

    assert [
             connect: [_],
             handle_execute: [%Q{}, [:client], _, :state]
           ] = A.record(agent)
  end

  test "considers idle connections on idle timeout in metrics" do
    stack = [
      {:ok, :state},
      {:ok, :state}
    ]

    idle_interval = 100

    {:ok, agent} = A.start_link(stack)
    {:ok, pool} = P.start_link(agent: agent, parent: self(), idle_interval: idle_interval)

    poll(fn ->
      assert [%{source: {:pool, ^pool}, checkout_queue_length: 0, ready_conn_count: 1}] =
               DBConnection.get_connection_metrics(pool)
    end)

    :timer.sleep(idle_interval)

    poll(fn ->
      assert [%{source: {:pool, ^pool}, checkout_queue_length: 0, ready_conn_count: 1}] =
               DBConnection.get_connection_metrics(pool)
    end)

    assert [
             connect: [_],
             ping: [:state]
           ] = A.record(agent)
  end

  test "considers enqueue/dequeue in metrics when busy" do
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

    poll(fn ->
      assert [%{source: {:pool, ^pool}, checkout_queue_length: 0, ready_conn_count: 2}] =
               DBConnection.get_connection_metrics(pool)
    end)

    run_query = fn ->
      spawn_link(fn ->
        Process.put(:agent, agent)
        assert P.execute(pool, %Q{}, [:client])
      end)
    end

    queries = [run_query.()]

    poll(fn ->
      assert [%{source: {:pool, ^pool}, checkout_queue_length: 0, ready_conn_count: 1}] =
               DBConnection.get_connection_metrics(pool)
    end)

    queries = [run_query.() | queries]

    poll(fn ->
      assert [%{source: {:pool, ^pool}, checkout_queue_length: 0, ready_conn_count: 0}] =
               DBConnection.get_connection_metrics(pool)
    end)

    queries = [run_query.() | queries]

    poll(fn ->
      assert [%{source: {:pool, ^pool}, checkout_queue_length: 1, ready_conn_count: 0}] =
               DBConnection.get_connection_metrics(pool)
    end)

    [query3, query2, query1] = queries
    send(query1, :continue)

    poll(fn ->
      assert [%{source: {:pool, ^pool}, checkout_queue_length: 0, ready_conn_count: 0}] =
               DBConnection.get_connection_metrics(pool)
    end)

    send(query2, :continue)

    poll(fn ->
      assert [%{source: {:pool, ^pool}, checkout_queue_length: 0, ready_conn_count: 1}] =
               DBConnection.get_connection_metrics(pool)
    end)

    send(query3, :continue)

    poll(fn ->
      assert [%{source: {:pool, ^pool}, checkout_queue_length: 0, ready_conn_count: 2}] =
               DBConnection.get_connection_metrics(pool)
    end)

    assert [
             connect: [_],
             connect: [_],
             handle_execute: [%Q{}, [:client], _, :state],
             handle_execute: [%Q{}, [:client], _, :state],
             handle_execute: [%Q{}, [:client], _, :state]
           ] = A.record(agent)
  end
end
