defmodule ErrorOnConnectTest do
  use ExUnit.Case, async: true

  alias TestPool, as: P
  alias TestAgent, as: A

  test "crash on connect doesn't leak sensitive data" do
    err = RuntimeError.exception("oops")
    stack = [
    fn(opts) ->
      Process.link(Keyword.get(opts, :parent))
      raise MatchError, term: :password
    end,
    {:ok, :state}
    ]
    {:ok, agent} = A.start_link(stack)

    opts = [agent: agent, parent: self(), backoff_min: 10]
    Process.flag(:trap_exit, true)
    {:ok, _} = P.start_link(opts)
    assert_receive {:EXIT, _, {%RuntimeError{}, _}}
  end
end
