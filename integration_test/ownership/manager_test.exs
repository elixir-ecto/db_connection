defmodule ManagerTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias DBConnection.Ownership

  import ExUnit.CaptureLog

  test "requires explicit checkout on manual mode" do
    {:ok, pool, opts} = start_pool()
    refute_checked_out pool, opts
    assert Ownership.ownership_checkout(pool, []) == :ok
    assert_checked_out pool, opts
    assert Ownership.ownership_checkin(pool, []) == :ok
    refute_checked_out pool, opts
    assert Ownership.ownership_checkin(pool, []) == :not_found
  end

  test "does not require explicit checkout on automatic mode" do
    {:ok, pool, opts} = start_pool()
    refute_checked_out pool, opts
    assert Ownership.ownership_mode(pool, :auto, []) == :ok
    assert_checked_out pool, opts
  end

  test "returns {:already, status} when already checked out" do
    {:ok, pool, _opts} = start_pool()

    assert Ownership.ownership_checkout(pool, []) ==
           :ok
    assert Ownership.ownership_checkout(pool, []) ==
           {:already, :owner}
  end

  test "connection may be shared with other processes" do
    {:ok, pool, _opts} = start_pool()
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
    {:ok, pool, opts} = start_pool()
    parent = self()

    pid = spawn_link(fn() ->
      assert_receive :refute_checkout
      refute_checked_out pool, opts
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
    {:ok, pool, opts} = start_pool()
    parent = self()

    Task.start_link fn ->
      :ok = Ownership.ownership_checkout(pool, [])
      :ok = Ownership.ownership_allow(pool, self(), parent, [])
      :ok = Ownership.ownership_checkin(pool, [])
      send parent, :checkin
      :timer.sleep(:infinity)
    end

    assert_receive :checkin
    refute_checked_out pool, opts
  end

  test "owner's checkout automatically with caller option" do
    {:ok, pool, opts} = start_pool()
    parent = self()

    assert Ownership.ownership_mode(pool, :manual, [])
    :ok = Ownership.ownership_checkout(pool, [])
    Task.start_link fn ->
      assert_checked_out pool, [caller: parent] ++ opts
      send parent, :checkin
    end
    assert_receive :checkin

    assert Ownership.ownership_mode(pool, {:shared, parent}, [])
    Task.start_link fn ->
      assert_checked_out pool, [caller: parent] ++ opts
      send parent, :checkin
    end
    assert_receive :checkin

    assert Ownership.ownership_mode(pool, :auto, [])
    :ok = Ownership.ownership_checkout(pool, [])
    Task.start_link fn ->
      assert_checked_out pool, [caller: parent] ++ opts
      send parent, :checkin
    end
    assert_receive :checkin

    assert_checked_out pool, [caller: parent] ++ opts
  end

  test "automatically allows caller process with caller option" do
    {:ok, pool, opts} = start_pool()
    parent = self()

    assert Ownership.ownership_mode(pool, :manual, [])
    Task.start_link fn ->
      refute_checked_out pool, [caller: parent] ++ opts
      send parent, :checkin
    end
    assert_receive :checkin

    assert Ownership.ownership_checkout(pool, [])
    :ok = Ownership.ownership_mode(pool, {:shared, parent}, [])

    Task.start_link fn ->
      untracked = self()
      Task.start_link fn ->
        assert_checked_out pool, [caller: untracked] ++ opts
        send untracked, :checkin
      end
      assert_receive :checkin
      assert_checked_out pool, opts
      send parent, :checkin
    end

    assert_receive :checkin
    Ownership.ownership_checkin(pool, [])
    assert Ownership.ownership_mode(pool, :auto, [])

    Task.start_link fn ->
      assert_checked_out pool, [caller: parent] ++ opts
      send parent, :checkin
    end
    assert_receive :checkin

    assert_checked_out pool, [caller: parent] ++ opts
  end

  test "setting manual mode checks in previous connections" do
    {:ok, agent} = A.start_link([{:ok, :state}, {:ok, :state}] ++ List.duplicate({:idle, :state}, 10))
    opts = [agent: agent, parent: self(), ownership_mode: :auto, pool_size: 2]
    {:ok, pool} = P.start_link(opts)

    parent = self()
    assert Ownership.ownership_mode(pool, :auto, []) == :ok

    task = Task.async(fn ->
      assert_checked_out pool, opts
      send parent, :checked_out
      assert_receive :manual
      refute_checked_out pool, opts
    end)

    assert_receive :checked_out
    assert Ownership.ownership_mode(pool, :manual, []) == :ok
    send task.pid, :manual
    Task.await(task)
  end

  test "uses ETS when the pool is named (with pid access)" do
    {:ok, pool, opts} = start_pool(name: :ownership_pid_access)
    parent = self()

    :ok = Ownership.ownership_checkout(pool, [])
    assert_checked_out pool, opts

    task = Task.async fn ->
      :ok = Ownership.ownership_allow(pool, parent, self(), [])
      assert_checked_out pool, opts
      send parent, :allowed
      assert_receive :checked_in
      refute_checked_out pool, opts
    end

    assert_receive :allowed
    :ok = Ownership.ownership_checkin(pool, [])
    send task.pid, :checked_in
    Task.await(task)
  end

  test "uses ETS when the pool is named (with named access)" do
    {:ok, _pool, opts} = start_pool(name: :ownership_name_access)
    pool = :ownership_name_access
    parent = self()

    :ok = Ownership.ownership_checkout(pool, [])
    assert_checked_out pool, opts

    task = Task.async fn ->
      :ok = Ownership.ownership_allow(pool, parent, self(), [])
      assert_checked_out pool, opts
      send parent, :allowed
      assert_receive :checked_in
      refute_checked_out pool, opts
    end

    assert_receive :allowed
    :ok = Ownership.ownership_checkin(pool, [])
    refute_checked_out pool, opts

    send task.pid, :checked_in
    Task.await(task)
  end

  test "does not require explicit checkout on shared mode" do
    {:ok, pool, opts} = start_pool()
    parent = self()

    # Cannot share if not owner
    assert Ownership.ownership_mode(pool, {:shared, self()}, []) == :not_found

    # Checkout but still do not share
    assert Ownership.ownership_checkout(pool, []) == :ok
    Task.async(fn -> refute_checked_out(pool, opts) end) |> Task.await

    # Cannot change mode from allowed process as well
    Task.async(fn ->
      Ownership.ownership_allow(pool, parent, self(), [])
      assert Ownership.ownership_mode(pool, {:shared, self()}, []) == :not_owner
    end) |> Task.await

    # Finally enable shared mode
    assert Ownership.ownership_mode(pool, {:shared, self()}, []) == :ok
    Task.async(fn -> assert_checked_out pool, opts end) |> Task.await
  end

  test "shared mode checks in previous connections" do
    {:ok, agent} = A.start_link([{:ok, :state}, {:ok, :state}])
    opts = [agent: agent, parent: self(), ownership_mode: :manual, pool_size: 2]
    {:ok, pool} = P.start_link(opts)
    parent = self()

    task = Task.async(fn ->
      assert Ownership.ownership_checkout(pool, []) == :ok
      send parent, :checked_out
      assert_receive :shared
      refute_checked_out pool, opts
    end)

    assert_receive :checked_out
    assert Ownership.ownership_checkout(pool, []) == :ok
    assert Ownership.ownership_mode(pool, {:shared, self()}, []) == :ok
    assert Ownership.ownership_checkin(pool, []) == :ok
    send task.pid, :shared
    Task.await(task)
  end

  test "shared mode can be set back to manual" do
    {:ok, pool, opts} = start_pool()
    parent = self()

    Task.start fn ->
      assert Ownership.ownership_checkout(pool, []) == :ok
      assert Ownership.ownership_mode(pool, {:shared, self()}, []) == :ok
      send parent, :shared
      :timer.sleep(:infinity)
    end

    assert_receive :shared
    assert_checked_out pool, opts
    assert Ownership.ownership_mode(pool, :manual, []) == :ok
    refute_checked_out pool, opts
    assert Ownership.ownership_checkout(pool, []) == :ok
  end

  test "shared mode automatically rolls back to manual on owner crash" do
    {:ok, pool, opts} = start_pool()
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

    refute_checked_out pool, opts
    assert Ownership.ownership_checkout(pool, []) == :ok
    assert Ownership.ownership_mode(pool, {:shared, self()}, []) == :ok
  end

  ## Callbacks

  test "allows post_checkout callback" do
    {:ok, pool, opts} = start_pool()
    parent = self()

    post_checkout = fn TestConnection, :state ->
      send(parent, :post_checkout)
      {:ok, TestConnection, :state}
    end

    assert Ownership.ownership_checkout(pool, post_checkout: post_checkout) == :ok
    assert_checked_out(pool, opts)
    assert_receive :post_checkout
  end

  test "allows pre_checkin callback" do
    {:ok, pool, opts} = start_pool()
    parent = self()

    pre_checkin = fn :checkin, TestConnection, :state ->
      send(parent, :pre_checkin)
      {:ok, TestConnection, :state}
    end

    assert Ownership.ownership_checkout(pool, pre_checkin: pre_checkin) == :ok
    assert_checked_out(pool, opts)
    assert Ownership.ownership_checkin(pool, []) == :ok
    assert_receive :pre_checkin
  end

  test "allows connection to be replaced on post_checkout/pre_checkin" do
    {:ok, pool, opts} = start_pool()
    parent = self()

    post_checkout = fn TestConnection, :state ->
      send(parent, :post_checkout)
      {:ok, Unknown, :unknown}
    end

    pre_checkin = fn {:stop, _}, Unknown, :unknown ->
      send(parent, :pre_checkin)
      {:ok, TestConnection, :state}
    end

    checkout = [post_checkout: post_checkout, pre_checkin: pre_checkin]
    assert Ownership.ownership_checkout(pool, checkout) == :ok
    assert_raise UndefinedFunctionError, ~r"function Unknown.handle_status/2 is undefined", fn ->
      assert_checked_out(pool, opts)
    end
    _ = Ownership.ownership_checkin(pool, [])
    assert_receive :pre_checkin
    assert_receive :post_checkout
  end

  test "disconnects on bad post_checkout" do
    stack = [
      {:ok, :state},
      {:idle, :state},
      {:idle, :state},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :state}
      end]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), ownership_mode: :manual]
    {:ok, pool} = P.start_link(opts)
    parent = self()

    post_checkout = fn TestConnection, :state ->
      send(parent, :post_checkout)
      {:error, RuntimeError.exception("oops"), TestConnection, :state}
    end

    assert capture_log(fn ->
      assert Ownership.ownership_checkout(pool, post_checkout: post_checkout) == :ok
      assert_raise RuntimeError, "oops", fn ->
        assert_checked_out(pool, opts)
      end
      assert Ownership.ownership_checkin(pool, []) == :not_found
      assert_receive :post_checkout
      assert_receive :reconnected
    end) =~ "disconnected: ** (RuntimeError) oops"
  end

  test "disconnects on bad pre_checkin" do
    stack = [
      {:ok, :state},
      {:idle, :state},
      {:idle, :state},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :state}
      end]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), ownership_mode: :manual]
    {:ok, pool} = P.start_link(opts)
    parent = self()

    pre_checkin = fn :checkin, TestConnection, :state ->
      send(parent, :pre_checkin)
      {:error, RuntimeError.exception("oops"), TestConnection, :state}
    end

    assert capture_log(fn ->
      assert Ownership.ownership_checkout(pool, pre_checkin: pre_checkin) == :ok
      assert_checked_out(pool, opts)
      assert Ownership.ownership_checkin(pool, []) == :ok
      assert_receive :pre_checkin
      assert_receive :reconnected
    end) =~ "disconnected: ** (RuntimeError) oops"
  end

  test "disconnects on bad pre_checkin on disconnect" do
    stack = [
      {:ok, :state},
      {:idle, :state},
      {:disconnect, DBConnection.ConnectionError.exception("oops"), :state},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :state}
      end]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), ownership_mode: :manual]
    {:ok, pool} = P.start_link(opts)
    parent = self()

    pre_checkin = fn {:disconnect, _}, TestConnection, :state ->
      send(parent, :pre_checkin)
      {:error, RuntimeError.exception("oops"), TestConnection, :state}
    end

    assert capture_log(fn ->
      assert Ownership.ownership_checkout(pool, pre_checkin: pre_checkin) == :ok
      assert_checked_out(pool, opts)
      _ = Ownership.ownership_checkin(pool, [])
      assert_receive :pre_checkin
      assert_receive :reconnected
    end) =~ "disconnected: ** (RuntimeError) oops"
  end

  test "stops on bad pre_checkin on stop" do
    stack = [
      {:ok, :state},
      {:idle, :state},
      :oops,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :state}
      end]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), ownership_mode: :manual]
    {:ok, pool} = P.start_link(opts)
    parent = self()

    pre_checkin = fn {:stop, _}, TestConnection, :state ->
      send(parent, :pre_checkin)
      {:error, RuntimeError.exception("oops"), TestConnection, :state}
    end

    assert capture_log(fn ->
      assert Ownership.ownership_checkout(pool, pre_checkin: pre_checkin) == :ok
      assert_raise DBConnection.ConnectionError, "bad return value: :oops", fn ->
        assert_checked_out(pool, opts)
      end
      _ = Ownership.ownership_checkin(pool, [])
      assert_receive :pre_checkin
      assert_receive :reconnected
    end) =~ ~r"GenServer #PID<\d+\.\d+\.\d+> terminating\n\*\* \(RuntimeError\) oops"
  end

  defp start_pool(opts \\ []) do
    stack = [{:ok, :state}] ++ List.duplicate({:idle, :state}, 10)
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), ownership_mode: :manual] ++ opts
    {:ok, pid} = P.start_link(opts)
    {:ok, pid, opts}
  end

  defp assert_checked_out(pool, opts) do
     assert P.run(pool, fn _ -> :ok end, opts)
   end

  defp refute_checked_out(pool, opts) do
    assert_raise DBConnection.OwnershipError, ~r/cannot find ownership process/, fn ->
      P.run(pool, fn _ -> :ok end, opts)
    end
  end
end
