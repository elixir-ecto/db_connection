defmodule DBConnection.App do
  @moduledoc false
  use Application

  def start(_, _) do
    children = [
      {Task.Supervisor, name: DBConnection.Task},
      {DynamicSupervisor, name: DBConnection.Ownership.Supervisor, strategy: :one_for_one},
      {DynamicSupervisor, name: DBConnection.ConnectionPool.Supervisor, strategy: :one_for_one},
      DBConnection.Watcher,
    ]

    Supervisor.start_link(children, strategy: :one_for_all, name: __MODULE__)
  end
end
