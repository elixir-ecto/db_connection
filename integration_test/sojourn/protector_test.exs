defmodule TestProtector do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A

  test "protector drops requests" do
    stack = [{:ok, :state}]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), queue_timeout: 5, protector: true,
            protector_update: 1, protector_interval: 1, protector_target: 0]
    {:ok, pool} = P.start_link(opts)

    P.run(pool, fn(_) ->
        assert_raise DBConnection.ConnectionError,
          fn() -> P.run(pool, fn(_) -> :ok end) end

        :timer.sleep(200)

        assert_raise DBConnection.ConnectionError, ~r"after 0ms",
          fn() -> P.run(pool, fn(_) -> :ok end, [protector: true]) end
    end)

    assert [connect: [_]] = A.record(agent)
  end
end
