defmodule DBConnection.ConnectionPool.Metrics do
  @moduledoc false

  @active_counter_idx 1
  @waiting_counter_idx 2

  def new(capacity) do
    {capacity, :counters.new(2, [])}
  end

  def get({capacity, metrics}) do
    active = :counters.get(metrics, @active_counter_idx)
    waiting = :counters.get(metrics, @waiting_counter_idx)
    %{active: active, waiting: waiting, capacity: capacity}
  end

  def checkin({_capacity, metrics}) do
    :counters.sub(metrics, @active_counter_idx, 1)
  end

  def dequeue({_capacity, metrics}), do: :counters.sub(metrics, @waiting_counter_idx, 1)
  def queue({_capacity, metrics}), do: :counters.add(metrics, @waiting_counter_idx, 1)

  def checkout({_capacity, metrics}, queue?) do
    :counters.add(metrics, @active_counter_idx, 1)

    if queue? do
      :counters.sub(metrics, @waiting_counter_idx, 1)
    end
  end
end
