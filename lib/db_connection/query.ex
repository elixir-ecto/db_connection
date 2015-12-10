defprotocol DBConnection.Query do
  @moduledoc """
  The `DBConnection.Query` protocol is responsible for preparing and
  encoding queries.
  """

  @fallback_to_any true

  @doc """
  Parse a query.

  This function is called to parse a query term before it is prepared using a
  connection callback module.

  See `DBConnection.prepare/3`.
  """
  @spec parse(any, Keyword.t) :: any
  def parse(query, opts)

  @doc """
  Describe a query.

  This function is called to describe a query after it is prepared using a
  connection callback module.

  See `DBConnection.prepare/3`.
  """
  @spec describe(any, Keyword.t) :: any
  def describe(query, opts)

  @doc """
  Encodes a query.

  This function is called to encode a query before it is executed using a
  connection callback module.

  See `DBConnection.execute/3`.
  """
  @spec encode(any, Keyword.t) :: any
  def encode(query, opts)
end

defimpl DBConnection.Query, for: Any do
  def parse(query, _), do: query
  def describe(query, _), do: query
  def encode(query, _), do: query
end
