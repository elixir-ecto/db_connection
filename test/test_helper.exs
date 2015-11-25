ExUnit.start([capture_log: true])

defmodule TestConnection do

  def start_link(opts), do: DBConnection.start_link(__MODULE__, opts)

  def connect(opts) do
    agent = Keyword.fetch!(opts, :agent)
    _ = Process.put(:agent, agent)
    TestAgent.eval(:connect, [opts])
  end

  def disconnect(err, state) do
    TestAgent.eval(:disconnect, [err, state])
  end

  def checkout(state) do
    {:ok, state}
  end

  def ping(state) do
    TestAgent.eval(:ping, [state])
  end

  def handle_info(msg, state) do
    TestAgent.eval(:handle_info, [msg, state])
  end
end

defmodule TestAgent do

  def start_link(stack), do: Agent.start_link(fn() -> {stack, []} end)

  def eval(agent \\ Process.get(:agent), fun, args) do
    action = {fun, args}
    case Agent.get_and_update(agent, &get_and_update(&1, action)) do
      fun when is_function(fun) ->
        apply(fun, args)
      result ->
        result
    end
  end

  def record(agent) do
    Enum.reverse(Agent.get(agent, &elem(&1, 1)))
  end

  defp get_and_update({[next | stack], record}, action) do
    {next, {stack, [action | record]}}
  end
end
