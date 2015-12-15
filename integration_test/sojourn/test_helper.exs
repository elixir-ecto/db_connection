Code.require_file "../../test/test_helper.exs", __DIR__

defmodule TestPool do
  @opts [pool_mod: DBConnection.Sojourn, pool_size: 1]

  use TestConnection, @opts

  def start_link(opts) do
    do_start(:start_link, opts)
  end

  def sandbox_link(opts) do
    do_start(:sandbox_link, opts)
  end

  defp do_start(start, opts) do
    {:ok, sup} = apply(TestConnection, start, [@opts ++ opts])
    children = Supervisor.which_children(sup)
    {_, broker, _, _} = List.keyfind(children, :sbroker, 0)
    {:ok, broker}
  end
end
