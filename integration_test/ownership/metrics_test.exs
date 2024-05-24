defmodule TestOwnershipMetrics do
  import TestHelpers

  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias TestQuery, as: Q
  alias TestResult, as: R

  test "starts without proxy processes" do
    stack = [
      {:ok, :state}
    ]

    {:ok, agent} = A.start_link(stack)
    {:ok, pool} = P.start_link(agent: agent, parent: self())

    poll(fn ->
      assert [%{source: {:pool, _}, checkout_queue_length: 0, ready_conn_count: 1}] =
               DBConnection.get_connection_metrics(pool, pool: DBConnection.Ownership)
    end)

    assert [
             connect: [_]
           ] = A.record(agent)
  end

  with "with `:ownership_mode` = :manual" do
    setup do
      stack = [
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
        end
      ]

      {:ok, agent} = A.start_link(stack)
      {:ok, pool} = P.start_link(agent: agent, parent: self(), ownership_mode: :manual)

      {:ok, %{agent: agent, pool: pool}}
    end

    test "considers a manual checkin and checkout", %{agent: agent, pool: pool} do
      poll(fn ->
        assert [%{source: {:pool, _}, checkout_queue_length: 0, ready_conn_count: 1}] =
                 DBConnection.get_connection_metrics(pool, pool: DBConnection.Ownership)
      end)

      assert :ok = DBConnection.Ownership.ownership_checkout(pool, [])

      poll(fn ->
        assert [
                 %{source: {:pool, _}, checkout_queue_length: 0, ready_conn_count: 0},
                 %{source: {:proxy, _}, checkout_queue_length: 0, ready_conn_count: 1}
               ] = DBConnection.get_connection_metrics(pool, pool: DBConnection.Ownership)
      end)

      assert :ok = DBConnection.Ownership.ownership_checkin(pool, [])

      poll(fn ->
        assert [
                 %{source: {:pool, _}, checkout_queue_length: 0, ready_conn_count: 1}
               ] = DBConnection.get_connection_metrics(pool, pool: DBConnection.Ownership)
      end)

      assert [
               connect: [_]
             ] = A.record(agent)
    end

    test "considers a query in progress", %{agent: agent, pool: pool} do
      poll(fn ->
        assert [%{source: {:pool, _}, checkout_queue_length: 0, ready_conn_count: 1}] =
                 DBConnection.get_connection_metrics(pool, pool: DBConnection.Ownership)
      end)

      assert :ok = DBConnection.Ownership.ownership_checkout(pool, [])

      parent = self()

      query =
        spawn_link(fn ->
          Process.put(:agent, agent)
          assert P.execute(pool, %Q{}, [:client], caller: parent, pool: DBConnection.Ownership)
        end)

      poll(fn ->
        assert [
                 %{source: {:pool, _}, checkout_queue_length: 0, ready_conn_count: 0},
                 %{source: {:proxy, _}, checkout_queue_length: 0, ready_conn_count: 0}
               ] = DBConnection.get_connection_metrics(pool, pool: DBConnection.Ownership)
      end)

      send(query, :continue)

      poll(fn ->
        assert [
                 %{source: {:pool, _}, checkout_queue_length: 0, ready_conn_count: 0},
                 %{source: {:proxy, _}, checkout_queue_length: 0, ready_conn_count: 1}
               ] = DBConnection.get_connection_metrics(pool, pool: DBConnection.Ownership)
      end)

      assert :ok = DBConnection.Ownership.ownership_checkin(pool, [])

      poll(fn ->
        assert [
                 %{source: {:pool, _}, checkout_queue_length: 0, ready_conn_count: 1}
               ] = DBConnection.get_connection_metrics(pool, pool: DBConnection.Ownership)
      end)

      assert [
               connect: [_],
               handle_execute: [%Q{}, [:client], _, :state]
             ] = A.record(agent)
    end
  end

  describe "with `:ownership_mode = :auto`" do
    setup do
      stack = [
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
        end
      ]

      {:ok, agent} = A.start_link(stack)
      {:ok, pool} = P.start_link(agent: agent, parent: self(), ownership_mode: :auto, name: :auto)

      {:ok, %{agent: agent, pool: pool}}
    end

    test "implicitly checkouts and considers a manual checkin", %{
      pool: pool,
      agent: agent
    } do
      poll(fn ->
        assert [%{source: {:pool, _}, checkout_queue_length: 0, ready_conn_count: 1}] =
                 DBConnection.get_connection_metrics(pool, pool: DBConnection.Ownership)
      end)

      query =
        spawn_link(fn ->
          Process.put(:agent, agent)
          assert P.execute(pool, %Q{}, [:client], pool: DBConnection.Ownership)
          DBConnection.Ownership.ownership_checkin(pool, [])
        end)

      poll(fn ->
        assert [
                 %{source: {:pool, _}, checkout_queue_length: 0, ready_conn_count: 0},
                 %{source: {:proxy, _}, checkout_queue_length: 0, ready_conn_count: 0}
               ] = DBConnection.get_connection_metrics(pool, pool: DBConnection.Ownership)
      end)

      send(query, :continue)

      poll(fn ->
        assert [
                 %{source: {:pool, _}, checkout_queue_length: 0, ready_conn_count: 1}
               ] = DBConnection.get_connection_metrics(pool, pool: DBConnection.Ownership)
      end)

      assert [
               connect: [_],
               handle_execute: [%Q{}, [:client], _, :state]
             ] = A.record(agent)
    end

    test "implicitly checkouts and checkins in case the proxy dies", %{
      pool: pool,
      agent: agent
    } do
      poll(fn ->
        assert [%{source: {:pool, _}, checkout_queue_length: 0, ready_conn_count: 1}] =
                 DBConnection.get_connection_metrics(pool, pool: DBConnection.Ownership)
      end)

      query =
        spawn_link(fn ->
          Process.put(:agent, agent)
          assert P.execute(pool, %Q{}, [:client], pool: DBConnection.Ownership)
        end)

      poll(fn ->
        assert [
                 %{source: {:pool, _}, checkout_queue_length: 0, ready_conn_count: 0},
                 %{source: {:proxy, _}, checkout_queue_length: 0, ready_conn_count: 0}
               ] = DBConnection.get_connection_metrics(pool, pool: DBConnection.Ownership)
      end)

      send(query, :continue)

      poll(fn ->
        assert [
                 %{source: {:pool, _}, checkout_queue_length: 0, ready_conn_count: 1}
               ] = DBConnection.get_connection_metrics(pool, pool: DBConnection.Ownership)
      end)

      assert [
               connect: [_],
               handle_execute: [%Q{}, [:client], _, :state]
             ] = A.record(agent)
    end

    test "implicitly checkouts and enqueues in case the proxy is busy", %{
      pool: pool,
      agent: agent
    } do
      poll(fn ->
        assert [%{source: {:pool, _}, checkout_queue_length: 0, ready_conn_count: 1}] =
                 DBConnection.get_connection_metrics(pool, pool: DBConnection.Ownership)
      end)

      parent = self()

      query_fn = fn ->
        Process.put(:agent, agent)

        assert P.execute(pool, %Q{}, [:client],
                 name: :auto,
                 caller: parent,
                 pool: DBConnection.Ownership
               )
      end

      query1 = spawn_link(query_fn)

      poll(fn ->
        assert [
                 %{source: {:pool, _}, checkout_queue_length: 0, ready_conn_count: 0},
                 %{source: {:proxy, _}, checkout_queue_length: 0, ready_conn_count: 0}
               ] = DBConnection.get_connection_metrics(pool, pool: DBConnection.Ownership)
      end)

      query2 = spawn_link(query_fn)

      poll(fn ->
        assert [
                 %{source: {:pool, _}, checkout_queue_length: 0, ready_conn_count: 0},
                 %{source: {:proxy, _}, checkout_queue_length: 1, ready_conn_count: 0}
               ] = DBConnection.get_connection_metrics(pool, pool: DBConnection.Ownership)
      end)

      send(query1, :continue)

      poll(fn ->
        assert [
                 %{source: {:pool, _}, checkout_queue_length: 0, ready_conn_count: 0},
                 %{source: {:proxy, _}, checkout_queue_length: 0, ready_conn_count: 0}
               ] = DBConnection.get_connection_metrics(pool, pool: DBConnection.Ownership)
      end)

      send(query2, :continue)

      poll(fn ->
        assert [
                 %{source: {:pool, _}, checkout_queue_length: 0, ready_conn_count: 0},
                 %{source: {:proxy, _}, checkout_queue_length: 0, ready_conn_count: 1}
               ] = DBConnection.get_connection_metrics(pool, pool: DBConnection.Ownership)
      end)

      assert :ok = DBConnection.Ownership.ownership_checkin(pool, [])

      poll(fn ->
        assert [
                 %{source: {:pool, _}, checkout_queue_length: 0, ready_conn_count: 1}
               ] = DBConnection.get_connection_metrics(pool, pool: DBConnection.Ownership)
      end)

      assert [
               connect: [_],
               handle_execute: [%Q{}, [:client], _, :state],
               handle_execute: [%Q{}, [:client], _, :state]
             ] = A.record(agent)
    end
  end
end
