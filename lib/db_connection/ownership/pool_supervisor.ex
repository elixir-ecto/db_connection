defmodule DBConnection.Ownership.PoolSupervisor do
  @moduledoc false

  import Supervisor.Spec

  def start_link() do
    pool = supervisor(DBConnection.Ownership.Pool, [], [restart: :temporary])
    sup_opts = [strategy: :simple_one_for_one, name: __MODULE__]
    Supervisor.start_link([pool], sup_opts)
  end

  def start_pool(mod, opts) do
    case Supervisor.start_child(__MODULE__, [self(), mod, opts]) do
      {:ok, pid} ->
        {:ok, DBConnection.Ownership.Pool.pool_pid(pid)}
      other ->
        other
    end
  end
end
