defprotocol DBConnection.Query do
  @moduledoc """
  The `DBConnection.query` protocol is responsible for preparing and
  encoding queries before they are passed to a connection.
  """

  @fallback_to_any true

  @doc """
  Prepare a query.

  This function is called to prepare a query term for use with a connection
  callback module, either to prepare it further using the connection
  itself or for a query.

  See `DBConnection.query/3` and `DBConnection.prepare/3`.
  """
  @spec prepare(any, Keyword.t) :: any
  def prepare(query, opts)

  @doc """
  Encodes a query.

  This function is called to encode a query once it has been prepared by
  a connection.

  See `DBConnection.execute/3`.
  """
  @spec encode(any, Keyword.t) :: any
  def encode(query, opts)
end

defimpl DBConnection.Query, for: Any do
  def prepare(query, _), do: query
  def encode(query, _), do: query
end
