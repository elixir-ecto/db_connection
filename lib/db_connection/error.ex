defmodule DBConnection.Error do
  defexception message: "connection error"

  def exception(message), do: %DBConnection.Error{message: message}
end
