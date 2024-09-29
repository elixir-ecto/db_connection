ExUnit.start(
  capture_log: true,
  assert_receive_timeout: 1000,
  exclude: [:idle_time, :idle_interval]
)

Code.require_file("../../test/test_support.exs", __DIR__)

defmodule TestPool do
  use TestConnection, pool: DBConnection.Ownership, pool_size: 1, ownership_log: :debug
end
