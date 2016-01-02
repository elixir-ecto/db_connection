Code.require_file "../../test/test_helper.exs", __DIR__

defmodule TestPool do
  @opts [pool: DBConnection.Sojourn, pool_size: 1]

  use TestConnection, @opts

  def start_link(opts) do
    {:ok, sup} = TestConnection.start_link(@opts ++ opts)
    children = Supervisor.which_children(sup)
    {_, broker, _, _} = List.keyfind(children, :sbroker, 0)
    {:ok, broker}
  end
end
