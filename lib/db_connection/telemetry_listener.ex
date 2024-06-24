defmodule DbConnection.TelemetryListener do
  @moduledoc """
  A connection listener that emits telemetry events for connection and disconnection
  """

  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, nil, opts)
  end

  @impl GenServer
  def init(_) do
    {:ok, %{monitoring: %{}}}
  end

  @impl GenServer
  def handle_info({:connected, pid, tag}, state) do
    handle_connected(pid, tag, state)
  end

  def handle_info({:connected, pid}, state) do
    handle_connected(pid, nil, state)
  end

  def handle_info({:disconnected, pid, _}, state) do
    handle_disconnected(pid, state)
  end

  def handle_info({:disconnected, pid}, state) do
    handle_disconnected(pid, state)
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    handle_disconnected(pid, state)
  end

  defp handle_connected(pid, tag, state) do
    :telemetry.execute([:db_connection, :connected], %{count: 1}, %{tag: tag, pid: pid})

    ref = Process.monitor(pid)
    monitoring = Map.put(state.monitoring, pid, {ref, tag})

    {:noreply, %{state | monitoring: monitoring}}
  end

  def handle_disconnected(pid, state) do
    case state.monitoring[pid] do
      # Already handled. We may receive two messages: one from monitor and one
      # from listener. For this reason, we need to handle both.
      nil ->
        {:noreply, state}

      {ref, tag} ->
        Process.demonitor(ref, [:flush])
        :telemetry.execute([:db_connection, :disconnected], %{count: 1}, %{tag: tag, pid: pid})
        monitoring = Map.delete(state.monitoring, pid)
        {:noreply, %{state | monitoring: monitoring}}
    end
  end
end
