defmodule DBConnection.NotifyListeners do
  @moduledoc false
  # This server is responsible for:
  # - Handling requests to add connection listeners to a connection
  # - Attaching a telemetry handler to forward messages to connection
  # listeners (but the forwarding is performed at the connection process)
  # - Automatically removing connection listeners for connections that were removed,
  # and detaching the telemetry handler if it's not needed anymore

  use GenServer

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: server_name())
  end

  def add_listeners(_, []) do
    :ok
  end

  def add_listeners(connection_pid, listeners) do
    GenServer.call(server_name(), {:add_listeners, connection_pid, listeners})
  end

  @impl GenServer
  def init(_opts) do
    start_ets()
    {:ok, %{attached?: false}}
  end

  @impl GenServer
  def handle_info({:DOWN, _, :process, pid, _}, state) do
    :ets.delete(ets_name(), pid)

    case maybe_detach_handlers() do
      :detached -> {:noreply, %{state | attached?: false}}
      :attached -> {:noreply, state}
    end
  end

  @impl GenServer
  def handle_call({:add_listeners, connection_pid, listeners}, _from, state) do
    unless state.attached? do
      attach_handlers()
    end

    :ets.insert(ets_name(), {connection_pid, listeners})
    Process.monitor(connection_pid)

    {:reply, :ok, %{state | attached?: true}}
  end

  defp maybe_detach_handlers() do
    case :ets.info(ets_name()) do
      %{size: 0} ->
        detach_handlers()
        :detached

      _ ->
        :attached
    end
  end

  defp start_ets() do
    :ets.new(ets_name(), [:public, :named_table])
  end

  defp attach_handlers() do
    :telemetry.attach_many(
      handler_name(),
      [
        [:db_connection, :connected],
        [:db_connection, :disconnected]
      ],
      &__MODULE__.notify_listeners/4,
      %{}
    )
  end

  defp detach_handlers() do
    :telemetry.detach(handler_name())
  end

  def notify_listeners([:db_connection, :connected], _, _, _) do
    do_notify_listeners(:connected, self())
  end

  def notify_listeners([:db_connection, :disconnected], _, _, _) do
    do_notify_listeners(:disconnected, self())
  end

  defp do_notify_listeners(action, conn_pid) do
    [{_, connection_listeners}] = :ets.lookup(ets_name(), conn_pid)

    {listeners, message} =
      case connection_listeners do
        listeners when is_list(listeners) ->
          {listeners, {action, conn_pid}}

        {listeners, tag} when is_list(listeners) ->
          {listeners, {action, conn_pid, tag}}
      end

    Enum.each(listeners, &send(&1, message))
  end

  defp ets_name(), do: __MODULE__.Ets
  defp handler_name(), do: inspect(__MODULE__.TelemetryHandler)
  defp server_name(), do: __MODULE__
end
