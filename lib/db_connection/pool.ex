defmodule DBConnection.Pool do
  @moduledoc false

  @type pool :: GenServer.server()

  @callback disconnect_all(pool, interval :: term, options :: keyword) :: :ok

  @callback checkout(pool, callers :: [pid], options :: keyword) ::
              {:ok, pool_ref :: term, module, checkin_time :: non_neg_integer() | nil,
               state :: term}
              | {:error, Exception.t()}
end
