defmodule DBConnection.Ownership.Pool do
  @moduledoc false

  import Supervisor.Spec

  def start_link(owner, mod, opts) do
    children = [watcher(owner), DBConnection.child_spec(mod, opts, [id: :pool])]
    sup_opts = [strategy: :rest_for_one, max_restarts: 0]
    Supervisor.start_link(children, sup_opts)
  end

  def pool_pid(pool) do
    children = Supervisor.which_children(pool)
    case List.keyfind(children, :pool, 0) do
      {_, pool, _, _} when is_pid(pool) ->
        pool
      _ ->
        raise "pool is not running"
    end
  end

  ## Helpers

  defp watcher(owner) do
    worker(DBConnection.Watcher, [owner])
  end
end
