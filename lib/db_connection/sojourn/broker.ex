defmodule DBConnection.Sojourn.Broker do
  @moduledoc false

  if Code.ensure_loaded?(:sbroker) do
    @behaviour :sbroker
  end

  @pool {__MODULE__, :pool_sup}

  @doc false
  def init({broker, mod, opts}) do
    opts = Keyword.put(opts, :pool_sup, pool(mod, opts))
    apply(broker, :init, [opts])
  end

  ## Helpers

  defp pool(mod, opts) do
    Process.get(@pool) || start_pool(mod, opts)
  end

  defp start_pool(mod, opts) do
    {:ok, pid} = DBConnection.Sojourn.Supervisor.start_pool(mod, opts)
    _ = Process.put(@pool, pid)
    pid
  end
end
