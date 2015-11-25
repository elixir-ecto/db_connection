defmodule DBConnection.Pool do

  @callback start_link(module, Keyword.t) ::
    GenServer.on_start

  @callback child_spec(module, Keyword.t, Keyword.t) ::
    Supervisor.Spec.spec

  @callback checkout(GenServer.server, Keyword.t) ::
    {:ok, pool_ref :: any, module, state :: any} | :error

  @callback checkin(pool_ref :: any, state :: any, Keyword.t) :: :ok

  @callback disconnect(pool_ref :: any, Exception.t, state :: any, Keyword.t) ::
    :ok

  @callback stop(pool_ref :: any, reason :: any, state :: any, Keyword.t) :: :ok
end
