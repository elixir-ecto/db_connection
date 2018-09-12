defmodule DBConnection.Ownership.ProxySupervisor do
  @moduledoc false

  @name __MODULE__
  import Supervisor.Spec

  def start_link do
    children  = [worker(DBConnection.Ownership.Proxy, [], [restart: :temporary])]
    Supervisor.start_link(children, name: @name, strategy: :simple_one_for_one)
  end

  def start_owner(from, pool, opts) do
    Supervisor.start_child(@name, [from, pool, opts])
  end
end