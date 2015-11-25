defprotocol DBConnection.Result do
  @spec decode(any, Keyword.t) :: any
  def decode(result, opts)
end
