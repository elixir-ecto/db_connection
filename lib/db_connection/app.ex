defmodule DBConnection.App do
  use Application

  def start(_, _) do
    DBConnection.Task.start_link()
  end
end
