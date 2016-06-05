defmodule DBConnection.Sojourn.Error do
  defexception message: "sojourn pool error"

  def exception(message), do: %DBConnection.Sojourn.Error{message: message}
end
