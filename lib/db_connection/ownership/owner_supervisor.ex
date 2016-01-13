defmodule DBConnection.Ownership.OwnerSupervisor do
  import Supervisor.Spec

  def start_link do
    children  = [supervisor(DBConnection.Ownership.Owner, [], [restart: :temporary])]
    opts      = [strategy: :simple_one_for_one, name: __MODULE__]
    Supervisor.start_link(children, opts)
  end

  def start_owner(manager, from, pool, opts) do
    Supervisor.start_child(__MODULE__, [manager, from, pool, opts])
  end
end