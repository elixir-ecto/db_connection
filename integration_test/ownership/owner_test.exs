defmodule OwnerTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A
  alias DBConnection.Ownership

  defmodule BadPool do
    def checkout(_, _) do
      :error
    end
  end

  test "allows a custom pool than the started one on checkout" do
    {:ok, pool} = start_pool()

    assert_raise UndefinedFunctionError, ~r"UnknownPool.checkout/2", fn ->
      Ownership.ownership_checkout(pool, [ownership_pool: UnknownPool])
    end
  end

  test "returns error on checkout" do
    {:ok, pool} = start_pool()
    assert Ownership.ownership_checkout(pool, [ownership_pool: BadPool]) == :error
  end

  test "reconnects when owner exits during a client checkout" do
    stack = [
      {:ok, :state},
      :ok,
      fn(opts) ->
        send(opts[:parent], :reconnected)
        {:ok, :state}
      end]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), ownership_mode: :manual]
    {:ok, pool} = P.start_link(opts)

    parent = self()
    {:ok, owner} = Task.start(fn() ->
      :ok = Ownership.ownership_checkout(pool, [])
      :ok = Ownership.ownership_allow(pool, self(), parent, [])
      send parent, :allowed
      :timer.sleep(:infinity)
    end)

    assert_receive :allowed

    assert P.run(pool, fn(_) ->
      Process.exit(owner, :shutdown)
      assert_receive :reconnected
      :ok
    end) == :ok

    assert [
      {:connect, _},
      {:disconnect, _},
      {:connect, _}] = A.record(agent)
  end

  defp start_pool do
    stack = [{:ok, :state}]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self()]
    P.start_link(opts)
  end
end
