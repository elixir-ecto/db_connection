ExUnit.start(
  capture_log: true,
  assert_receive_timeout: 1000
)

Code.require_file("../../test/test_support.exs", __DIR__)
