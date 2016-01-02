Code.require_file "../../test/test_helper.exs", __DIR__

defmodule TestPool do
  use TestConnection, [pool: DBConnection.Connection, pool_size: 1]
end
