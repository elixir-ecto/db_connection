defmodule ManagerTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias DBConnection.Ownership

  test "requires explicit checkout on manual mode" do
    {:ok, pool} = start_pool()
    refute_checked_out pool
    assert Ownership.ownership_checkout(pool, []) == :ok
    assert_checked_out pool
    assert Ownership.ownership_checkin(pool, []) == :ok
    refute_checked_out pool
    assert Ownership.ownership_checkin(pool, []) == :not_found
  end

  test "does not require explicit checkout on automatic mode" do
    {:ok, pool} = start_pool()
    refute_checked_out pool
    assert Ownership.ownership_mode(pool, :auto, []) == :ok
    assert_checked_out pool
  end

  test "returns {:already, status} when already checked out" do
    {:ok, pool} = start_pool()

    assert Ownership.ownership_checkout(pool, []) ==
           :ok
    assert Ownership.ownership_checkout(pool, []) ==
           {:already, :owner}
  end

  test "connection may be shared with other processes" do
    {:ok, pool} = start_pool()
    parent = self()

    Task.await Task.async fn ->
      assert Ownership.ownership_allow(pool, parent, self(), []) ==
             :not_found
    end

    :ok = Ownership.ownership_checkout(pool, [])
    assert Ownership.ownership_allow(pool, self(), self(), []) ==
           {:already, :owner}

    Task.await Task.async fn ->
      assert Ownership.ownership_allow(pool, parent, self(), []) ==
             :ok
      assert Ownership.ownership_allow(pool, parent, self(), []) ==
             {:already, :allowed}

      assert Ownership.ownership_checkin(pool, []) == :not_owner

      parent = self()
      Task.await Task.async fn ->
        assert Ownership.ownership_allow(pool, parent, self(), []) ==
               :ok
      end
    end
  end

  test "owner's crash automatically checks the connection back in" do
    {:ok, pool} = start_pool()
    parent = self()

    pid = spawn_link(fn() ->
      assert_receive :refute_checkout
      refute_checked_out pool
      send(parent, :no_checkout)
    end)

    {:ok, owner} = Task.start fn ->
      :ok = Ownership.ownership_checkout(pool, [])
      :ok = Ownership.ownership_allow(pool, self(), pid, [])
      send parent, :checked_out
    end

    assert_receive :checked_out
    ref = Process.monitor(owner)
    assert_receive {:DOWN, ^ref, _, _, _}

    :ok = Ownership.ownership_checkout(pool, [])

    send(pid, :refute_checkout)
    assert_receive :no_checkout
  end

  test "owner's checkin automatically revokes allowed access" do
    {:ok, pool} = start_pool()
    parent = self()

    Task.start_link fn ->
      :ok = Ownership.ownership_checkout(pool, [])
      :ok = Ownership.ownership_allow(pool, self(), parent, [])
      :ok = Ownership.ownership_checkin(pool, [])
      send parent, :checkin
      :timer.sleep(:infinity)
    end

    assert_receive :checkin
    refute_checked_out pool
  end

  test "owner's checkout automatically with caller option" do
    {:ok, pool} = start_pool()
    :ok = Ownership.ownership_checkout(pool, [])
    parent = self()

    Task.start_link fn ->
      assert_checked_out pool, [caller: parent]
      send parent, :checkin
    end
    assert_receive :checkin

    assert Ownership.ownership_mode(pool, :auto, [])
    Task.start_link fn ->
      assert_checked_out pool, [caller: parent]
      send parent, :checkin
    end
    assert_receive :checkin

    assert Ownership.ownership_mode(pool, {:shared, parent}, [])
    Task.start_link fn ->
      assert_checked_out pool, [caller: parent]
      send parent, :checkin
    end
    assert_receive :checkin

    assert_checked_out pool, [caller: parent]
  end

  test "uses ETS when the pool is named (with pid access)" do
    {:ok, pool} = start_pool(name: :ownership_pid_access)
    parent = self()

    :ok = Ownership.ownership_checkout(pool, [])
    assert_checked_out pool

    task = Task.async fn ->
      :ok = Ownership.ownership_allow(pool, parent, self(), [])
      assert_checked_out pool
      send parent, :allowed
      assert_receive :checked_in
      refute_checked_out pool
    end

    assert_receive :allowed
    :ok = Ownership.ownership_checkin(pool, [])
    send task.pid, :checked_in
    Task.await(task)
  end

  test "uses ETS when the pool is named (with named access)" do
    start_pool(name: :ownership_name_access)
    pool = :ownership_name_access
    parent = self()

    :ok = Ownership.ownership_checkout(pool, [])
    assert_checked_out pool

    task = Task.async fn ->
      :ok = Ownership.ownership_allow(pool, parent, self(), [])
      assert_checked_out pool
      send parent, :allowed
      assert_receive :checked_in
      refute_checked_out pool
    end

    assert_receive :allowed
    :ok = Ownership.ownership_checkin(pool, [])
    refute_checked_out pool

    send task.pid, :checked_in
    Task.await(task)
  end

  test "does not require explicit checkout on shared mode" do
    {:ok, pool} = start_pool()
    parent = self()

    # Cannot share if not owner
    assert Ownership.ownership_mode(pool, {:shared, self()}, []) == :not_found

    # Checkout but still do not share
    assert Ownership.ownership_checkout(pool, []) == :ok
    Task.async(fn -> refute_checked_out pool end) |> Task.await

    # Cannot change mode from allowed process as well
    Task.async(fn ->
      Ownership.ownership_allow(pool, parent, self(), [])
      assert Ownership.ownership_mode(pool, {:shared, self()}, []) == :not_owner
    end) |> Task.await

    # Finally enable shared mode
    assert Ownership.ownership_mode(pool, {:shared, self()}, []) == :ok
    Task.async(fn -> assert_checked_out pool end) |> Task.await
  end

  test "shared mode can be set back to manual" do
    {:ok, pool} = start_pool()
    parent = self()

    {:ok, pid} = Task.start fn ->
      assert Ownership.ownership_checkout(pool, []) == :ok
      assert Ownership.ownership_mode(pool, {:shared, self()}, []) == :ok
      send parent, :shared
      :timer.sleep(:infinity)
    end

    assert_receive :shared
    assert_checked_out pool
    assert Ownership.ownership_mode(pool, :manual, []) == :ok
    assert_checked_out pool

    :erlang.trace(pool, true, [:receive])
    Process.exit(pid, :shutdown)
    assert_receive {:trace, ^pool, :receive, {:DOWN, _, _, _, _}}

    refute_checked_out pool
    assert Ownership.ownership_checkout(pool, []) == :ok
  end

  test "shared mode automatically rolls back to manual on owner crash" do
    {:ok, pool} = start_pool()
    parent = self()

    {:ok, pid} = Task.start fn ->
      assert Ownership.ownership_checkout(pool, []) == :ok
      assert Ownership.ownership_mode(pool, {:shared, self()}, []) == :ok
      send parent, :shared
      :timer.sleep(:infinity)
    end

    assert_receive :shared
    assert Ownership.ownership_mode(pool, {:shared, self()}, []) == :already_shared

    :erlang.trace(pool, true, [:receive])
    Process.exit(pid, :shutdown)
    assert_receive {:trace, ^pool, :receive, {:DOWN, _, _, _, _}}

    refute_checked_out pool
    assert Ownership.ownership_checkout(pool, []) == :ok
    assert Ownership.ownership_mode(pool, {:shared, self()}, []) == :ok
  end

  defp start_pool(opts \\ []) do
    stack = [{:ok, :state}]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), ownership_mode: :manual] ++ opts
    P.start_link(opts)
  end

  defp assert_checked_out(pool, opts \\ []) do
     assert P.run(pool, fn _ -> :ok end, opts)
   end

  defp refute_checked_out(pool) do
    assert_raise DBConnection.OwnershipError, ~r/cannot find ownership process/, fn ->
      P.run(pool, fn _ -> :ok end)
    end
  end
end
