defmodule DBConnection.Sojourn.Timeout do
  @moduledoc """
  A sbroker callback module with a timeout based queue.

  ### Options

    * `:queue_timeout` - The time to wait for control of the connection's
    state (default: `5_000`)
    * `:queue_out` - Either `:out` for a FIFO queue or `:out_r` for a
    LIFO queue (default: `:out`)
    * `:queue_drop` - Either `:drop` for head drop on max size or
    `:drop_r` for tail drop (default: `:drop`)
    * `:queue_size` - The maximum size of the queue (default: `128`)
  """

  if Code.ensure_loaded?(:sbroker) do
    @behaviour :sbroker
  end

  @doc false
  def init(opts) do
    out      = Keyword.get(opts, :queue_out, :out)
    timeout  = timeout(opts, :queue_timeout, 5_000)
    drop     = Keyword.get(opts, :queue_drop, :drop)
    size     = Keyword.get(opts, :queue_size, 128)
    timeout2 = timeout(opts, :idle_timeout, 15_000)

    client_queue = {:sbroker_timeout_queue, {out, timeout, drop, size}}
    conn_queue = {:sbroker_timeout_queue, {:out, timeout2, :drop, :infinity}}

    {:ok, {client_queue, conn_queue, 50}}
  end

  ## Helpers

  defp timeout(opts, key, default) do
    case Keyword.get(opts, key, default) do
      :infinity -> :infinity
      timeout   -> timeout * 1_000
    end
  end
end
