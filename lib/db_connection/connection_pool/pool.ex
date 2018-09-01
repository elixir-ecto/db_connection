defmodule DBConnection.ConnectionPool.Pool do
  @moduledoc false

  def start_link(owner, tag, mod, opts) do
    size = Keyword.get(opts, :pool_size, 1)
    children = for id <- 1..size, do: conn(owner, tag, id, mod, opts)
    sup_opts = [strategy: :one_for_one] ++ Keyword.take(opts, [:max_restarts, :max_seconds])
    Supervisor.start_link(children, sup_opts)
  end

  ## Helpers

  defp conn(owner, tag, id, mod, opts) do
    child_opts = [id: {mod, owner, id}] ++ Keyword.take(opts, [:shutdown])
    DBConnection.Connection.child_spec(mod, opts, owner, tag, child_opts)
  end
end
