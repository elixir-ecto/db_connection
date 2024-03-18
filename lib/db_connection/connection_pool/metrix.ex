defmodule DBConnection.ConnectionPool.Metrics do
  @moduledoc false

  @active_counter_idx 1
  @waiting_counter_idx 2
  @capacity_counter_idx 3

  def new(capacity) do
    {capacity, :counters.new(3, [])}
  end

  def get({capacity, metrics}) do
    active = :counters.get(metrics, @active_counter_idx)
    waiting = :counters.get(metrics, @waiting_counter_idx)
    current_capacity = :counters.get(metrics, @capacity_counter_idx)
    %{active: active, waiting: waiting, init?: capacity == current_capacity, capacity: capacity}
  end

  def checkin({capacity, metrics}) do
    current_capacity = :counters.get(metrics, @capacity_counter_idx)

    if current_capacity == capacity do
      :counters.sub(metrics, @active_counter_idx, 1)
    else
      :counters.add(metrics, @capacity_counter_idx, 1)
    end
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
