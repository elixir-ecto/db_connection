defprotocol DBConnection.Query do
  @spec prepare(any, Keyword.t) :: any
  def prepare(query, opts)

  @spec encode(any, Keyword.t) :: any
  def encode(query, opts)
end
