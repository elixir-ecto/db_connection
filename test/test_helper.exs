ExUnit.start([capture_log: true])

defmodule TestConnection do

  defmacro __using__(opts) do
    quote do
      def start_link(opts2) do
        TestConnection.start_link(unquote(opts) ++ opts2)
      end

      def query(pool, query, params, opts2 \\ []) do
        DBConnection.query(pool, query, params, opts2 ++ unquote(opts))
      end

      def query!(pool, query, params, opts2 \\ []) do
        DBConnection.query!(pool, query, params, opts2 ++ unquote(opts))
      end

      def run(pool, fun, opts2 \\ []) do
        DBConnection.run(pool, fun, opts2 ++ unquote(opts))
      end

      def transaction(pool, fun, opts2 \\ []) do
        DBConnection.transaction(pool, fun, opts2 ++ unquote(opts))
      end

      defdelegate rollback(conn, reason), to: DBConnection

      def prepare(pool, query, opts2 \\ []) do
        DBConnection.prepare(pool, query, opts2 ++ unquote(opts))
      end

      def prepare!(pool, query, opts2 \\ []) do
        DBConnection.prepare!(pool, query, opts2 ++ unquote(opts))
      end

      def prepare_execute(pool, query, params, opts2 \\ []) do
        DBConnection.prepare_execute(pool, query, params,
          opts2 ++ unquote(opts))
      end

      def prepare_execute!(pool, query, params, opts2 \\ []) do
        DBConnection.prepare_execute!(pool, query, params,
          opts2 ++ unquote(opts))
      end

      def execute(pool, query, params, opts2 \\ []) do
        DBConnection.execute(pool, query, params, opts2 ++ unquote(opts))
      end

      def execute!(pool, query, params, opts2 \\ []) do
        DBConnection.execute!(pool, query, params, opts2 ++ unquote(opts))
      end

      def close(pool, query, opts2 \\ []) do
        DBConnection.close(pool, query, opts2 ++ unquote(opts))
      end

      def close!(pool, query, opts2 \\ []) do
        DBConnection.close!(pool, query, opts2 ++ unquote(opts))
      end

      defoverridable [start_link: 1]
    end
  end

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

  def checkin(state) do
    {:ok, state}
  end

  def ping(state) do
    TestAgent.eval(:ping, [state])
  end

  def handle_begin(opts, state) do
    TestAgent.eval(:handle_begin, [opts, state])
  end

  def handle_commit(opts, state) do
    TestAgent.eval(:handle_commit, [opts, state])
  end

  def handle_rollback(opts, state) do
    TestAgent.eval(:handle_rollback, [opts, state])
  end

  def handle_prepare(query, opts, state) do
    TestAgent.eval(:handle_prepare, [query, opts, state])
  end

  def handle_execute(query, params, opts, state) do
    TestAgent.eval(:handle_execute, [query, params, opts, state])
  end

  def handle_execute_close(query, params, opts, state) do
    TestAgent.eval(:handle_execute_close, [query, params, opts, state])
  end

  def handle_close(query, opts, state) do
    TestAgent.eval(:handle_close, [query, opts, state])
  end

  def handle_info(msg, state) do
    TestAgent.eval(:handle_info, [msg, state])
  end
end


defmodule TestQuery do
  defstruct [:state]
end

defmodule TestResult do
  defstruct []
end

defimpl DBConnection.Query, for: TestQuery do
  def parse(query, opts) do
    parse = Keyword.get(opts, :parse, &(&1))
    parse.(query)
  end

  def describe(query, opts) do
    describe = Keyword.get(opts, :describe, &(&1))
    describe.(query)
  end

  def encode(_, params, opts) do
    encode = Keyword.get(opts, :encode, &(&1))
    encode.(params)
  end

  def decode(_, result, opts) do
    decode = Keyword.get(opts, :decode, &(&1))
    decode.(result)
  end
end

defmodule TestAgent do

  def start_link(stack) do
    {:ok, agent} = ok = Agent.start_link(fn() -> {stack, []} end)
    _ = Process.put(:agent, agent)
    ok
  end

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

defmodule TestProxy do
  use DBConnection.Proxy

  def init(opts) do
    TestAgent.eval(:init, [opts])
  end
  def checkout(mod, opts, conn, state) do
    TestAgent.eval(:checkout, [mod, opts, conn, state])
  end
  def checkin(mod, opts, conn, state) do
    TestAgent.eval(:checkin, [mod, opts, conn, state])
  end
end
