ExUnit.start([capture_log: :true, assert_receive_timeout: 500,
              exclude: [:queue_disconnected]])



Code.require_file "../../test/test_support.exs", __DIR__

defmodule TestPool do
  use TestConnection, [pool: DBConnection.Sojourn, pool_size: 1]
end
