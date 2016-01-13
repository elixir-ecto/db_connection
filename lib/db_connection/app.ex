defmodule DBConnection.App do
  use Application

  import Supervisor.Spec

  def start(_, _) do
    children = [
      supervisor(DBConnection.Task, []),
      supervisor(DBConnection.Sojourn.Supervisor, []),
      supervisor(DBConnection.Ownership.PoolSupervisor, []),
      supervisor(DBConnection.Ownership.OwnerSupervisor, [])
    ]
    Supervisor.start_link(children, [strategy: :one_for_one, name: __MODULE__])
  end
end
