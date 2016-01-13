defmodule DBConnection.Sojourn.Pool do
  @moduledoc false

  import Supervisor.Spec

  def start_link(owner, mod, opts) do
    children   = [watcher(owner), conn_sup(mod, opts), starter(owner, opts)]
    opts       = [strategy: :rest_for_one, max_restarts: 0]
    Supervisor.start_link(children, opts)
  end

  ## Helpers

  defp watcher(owner) do
    worker(DBConnection.Watcher, [owner])
  end

  defp conn_sup(mod, opts) do
    child_opts = Keyword.take(opts, [:shutdown])
    conn = DBConnection.Connection.child_spec(mod, opts, :sojourn, child_opts)
    sup_opts  = Keyword.take(opts, [:max_restarts, :max_seconds])
    sup_opts  = [strategy: :simple_one_for_one] ++ sup_opts
    supervisor(Supervisor, [[conn], sup_opts])
  end

  defp starter(owner, opts) do
    worker(DBConnection.Sojourn.Starter, [owner, opts], [restart: :transient])
  end
end
