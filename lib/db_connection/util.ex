defmodule DBConnection.Util do
  @moduledoc """
  Shared functions.
  """

  @doc """
  Inspect a pid, including the process label if possible.
  """
  def inspect_pid(pid) do
    case get_label(pid) do
      :undefined -> inspect(pid)
      label -> "#{inspect(pid)} (#{inspect(label)})"
    end
  end

  @doc """
  Set a process label if `Process.set_label/1` is available.
  """
  def set_label(label) do
    if function_exported?(:proc_lib, :set_label, 1) do
      :proc_lib.set_label(label)
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
end
