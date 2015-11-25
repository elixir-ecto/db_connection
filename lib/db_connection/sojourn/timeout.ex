defmodule DBConnection.Sojourn.Timeout do

 @behaviour :sbroker

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
