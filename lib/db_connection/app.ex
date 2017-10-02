defmodule DBConnection.App do
  @moduledoc false
  use Application

  import Supervisor.Spec

  def start(_, _) do
    children = [
      worker(DBConnection.Watcher, []),
      supervisor(DBConnection.Task, []),
      supervisor(DBConnection.Sojourn.Supervisor, []),
      supervisor(DBConnection.Ownership.PoolSupervisor, [])
    ]
    Supervisor.start_link(children, strategy: :rest_for_one, name: __MODULE__)
  end
end
