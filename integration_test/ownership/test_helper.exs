ExUnit.start(
  capture_log: true,
  assert_receive_timeout: 500,
  exclude: [:idle_timeout, :queue_timeout_exit]
)

Code.require_file("../../test/test_support.exs", __DIR__)

defmodule TestPool do
  use TestConnection, pool: DBConnection.Ownership, pool_size: 1
end
