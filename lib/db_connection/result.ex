defprotocol DBConnection.Result do
  @moduledoc """
  The `DBConnection.query` protocol is responsible for preparing and
  encoding queries before they are passed to a connection.
  """

  @fallback_to_any true

  @doc """
  Decode a query.

  This function is called to decode a result after it is returned by a
  connection callback module.

  See `DBConnection.execute/3` and `DBConnection.execute_close/3`.
  """
  @spec decode(any, Keyword.t) :: any
  def decode(result, opts)
end

defimpl DBConnection.Result, for: Any do
  def decode(result, _), do: result
end
