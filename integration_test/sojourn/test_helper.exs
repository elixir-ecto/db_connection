Code.require_file "../../test/test_support.exs", __DIR__

defmodule TestPool do
  use TestConnection, [pool: DBConnection.Sojourn, pool_size: 1,
    protector: false]
end

case :erlang.system_info(:otp_release) do
  '17' ->
    ExUnit.start([exclude: [:test]])
  _ ->
    ExUnit.start([capture_log: :true, assert_receive_timeout: 500,
                  exclude: [:enqueue_disconnected, :queue_timeout_exit]])

    {:ok, _} = TestPool.ensure_all_started()
end
