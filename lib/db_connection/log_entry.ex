defmodule DBConnection.LogEntry do
  @moduledoc """
  Struct containing log entry information.
  """

  defstruct [:call, :query, :params, :result, :pool_time, :connection_time, :decode_time]

  @typedoc """
  Log entry information.

    * `:call` - The `DBConnection` function called
    * `:query` - The query used by the function
    * `:params` - The params passed to the function (if any)
    * `:result` - The result of the call
    * `:pool_time` - The length of time awaiting a connection from the pool (if
    the connection was not already checked out)
    * `:connection_time` - The length of time using the connection (if a
    connection was used)
    * `:decode_time` - The length of time decoding the result (if decoded the
    result using `DBConnection.Query.decode/3`)

  All times are in the native time units of the VM, see
  `System.monotonic_time/0`.
  """
  @type t :: %__MODULE__{call: atom,
                         query: any,
                         params: any,
                         result: {:ok, any} | {:ok, any, any} | {:error, Exception.t},
                         pool_time: non_neg_integer | nil,
                         connection_time: non_neg_integer | nil,
                         decode_time: non_neg_integer | nil}

  @doc false
  def new(call, query, params, times, result) do
    entry = %__MODULE__{call: call, query: query, params: params, result: result}
    parse_times(times, entry)
  end

  ## Helpers

  defp parse_times([], entry), do: entry
  defp parse_times(times, entry) do
    stop = :erlang.monotonic_time()
    {_, entry} = Enum.reduce(times, {stop, entry}, &parse_time/2)
    entry
  end

  defp parse_time({:decode, start}, {stop, entry}) do
    {start, %{entry | decode_time: stop - start}}
  end
  defp parse_time({:checkout, start}, {stop, entry}) do
    {start, %{entry | pool_time: stop - start}}
  end
  defp parse_time({_, start}, {stop, entry}) do
    %{connection_time: connection_time} = entry
    {start, %{entry | connection_time: (connection_time || 0) + (stop - start)}}
  end
end
