defmodule DBConnection.ConnectionPool.PoolSupervisor do
  @moduledoc false

  alias DBConnection.ConnectionPool.Pool
  import Supervisor.Spec

  def start_link() do
    pool = supervisor(Pool, [], [restart: :temporary])
    sup_opts = [strategy: :simple_one_for_one, name: __MODULE__]
    Supervisor.start_link([pool], sup_opts)
  end

  def start_pool(tag, mod, opts) do
    DBConnection.Watcher.watch(__MODULE__, [self(), tag, mod, opts])
  end
end
