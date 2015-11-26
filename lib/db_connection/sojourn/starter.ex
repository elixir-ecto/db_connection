defmodule DBConnection.Sojourn.Starter do
  @moduledoc false

  use Connection

  def start_link(opts), do: Connection.start_link(__MODULE__, {self(), opts})

  def init(args), do: {:connect, :init, args}

  def connect(:init, {sup, opts}) do
    size = Keyword.get(opts, :pool_size, 10)
    conn_sup = conn_sup(sup)
    %{workers: conns} = Supervisor.count_children(conn_sup)
    start_conns(size - conns, conn_sup, broker(sup))
  end

  defp start_conns(n, conn_sup, broker) when n > 0 do
    case Supervisor.start_child(conn_sup, [broker]) do
      {:ok, _} ->
        start_conns(n - 1, conn_sup, broker)
      {:error, reason} ->
        {:stop, {:failed_to_start_connection, reason}, broker}
    end
  end
  defp start_conns(_, _, _), do: {:stop, :normal, nil}

  ## Helpers

  defp conn_sup(sup) do
    children = Supervisor.which_children(sup)
    {Supervisor, conn_sup, _, _}  = List.keyfind(children, Supervisor, 0)
    conn_sup
  end

  defp broker(sup) do
    children = Supervisor.which_children(sup)
    {:sbroker, broker, _, _}  = List.keyfind(children, :sbroker, 0)
    broker
  end
end
