Code.require_file "../../test/test_helper.exs", __DIR__

defmodule TestPool do
  def start_link(opts) do
    opts = [pool_mod: DBConnection.Sojourn, pool_size: 1] ++ opts
    TestConnection.start_link(opts)
  end
end
