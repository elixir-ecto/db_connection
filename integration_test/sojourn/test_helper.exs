ExUnit.start([capture_log: :true, assert_receive_timeout: 500,
              exclude: [:enqueue_disconnected, :queue_timeout_exit]])

Code.require_file "../../test/test_support.exs", __DIR__

{:ok, _} = Application.ensure_all_started(:sbroker)

defmodule TestPool do
  use TestConnection, [pool: DBConnection.Sojourn, pool_size: 1,
                       protector: false]
end
