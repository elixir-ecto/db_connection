defmodule DBConnection.Util do
  @moduledoc false

  @doc """
  Inspect a pid, including the process label if possible.
  """
  def inspect_pid(pid) when is_pid(pid) do
    with :undefined <- get_label(pid),
         :undefined <- get_name(pid),
         :undefined <- get_initial_call(pid) do
      inspect(pid)
    else
      label_or_name_or_call -> "#{inspect(pid)} (#{inspect(label_or_name_or_call)})"
    end
  end

  def inspect_pid(other), do: inspect(other)

  defp get_name(pid) do
    try do
      Process.info(pid, :registered_name)
    rescue
      _ -> :undefined
    else
      {:registered_name, name} when is_atom(name) -> name
      _ -> :undefined
    end
  end

  @doc """
  Set a process label if `Process.set_label/1` is available.
  """
  def set_label(label) do
    if function_exported?(Process, :set_label, 1) do
      Process.set_label(label)
    else
      :ok
    end
  end

  # Get a process label if `:proc_lib.get_label/1` is available.
  defp get_label(pid) do
    if function_exported?(:proc_lib, :get_label, 1) do
      # Avoid a compiler warning if the function isn't
      # defined in your version of Erlang/OTP
      apply(:proc_lib, :get_label, [pid])
    else
      # mimic return value of
      # `:proc_lib.get_label/1` when none is set.
      # Don't resort to using `Process.info(pid, :dictionary)`,
      # as this is not efficient.
      :undefined
    end
  end

  defp get_initial_call(pid) do
    case Process.info(pid, :initial_call) do
      {:initial_call, {mod, _, _}} -> mod
      _ -> :undefined
    end
  end
end
