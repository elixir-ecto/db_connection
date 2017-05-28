defmodule TestConnection do

  defmacro __using__(opts) do
    quote do
      def ensure_all_started do
        DBConnection.ensure_all_started(unquote(opts))
      end

      def start_link(opts2) do
        defaults = [backoff_type: :exp, backoff_min: 200]
        TestConnection.start_link(opts2 ++ unquote(opts) ++ defaults)
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

      def stream(conn, query, params, opts2 \\ []) do
        DBConnection.stream(conn, query, params, opts2 ++ unquote(opts))
      end

      def prepare_stream(conn, query, params, opts2 \\ []) do
        DBConnection.prepare_stream(conn, query, params, opts2 ++ unquote(opts))
      end

      def close(pool, query, opts2 \\ []) do
        DBConnection.close(pool, query, opts2 ++ unquote(opts))
      end

      def close!(pool, query, opts2 \\ []) do
        DBConnection.close!(pool, query, opts2 ++ unquote(opts))
      end

      def start_producer(pool, query, params, opts2 \\ []) do
        DBConnection.Producer.start_link(pool, query, params, opts2 ++ unquote(opts))
      end

      def checkout_begin(pool, opts2 \\ []) do
        DBConnection.checkout_begin(pool, opts2 ++ unquote(opts))
      end

      def commit_checkin(conn, opts2 \\ []) do
        DBConnection.commit_checkin(conn, opts2 ++ unquote(opts))
      end

      def rollback_checkin(conn, reason, opts2 \\ []) do
        DBConnection.rollback_checkin(conn, reason, opts2 ++ unquote(opts))
      end

      def checkin(conn, opts2 \\ []) do
        DBConnection.checkin(conn, opts2 ++ unquote(opts))
      end

      defoverridable [start_link: 1]
    end
  end

  def start_link(opts), do: DBConnection.start_link(__MODULE__, opts)

  def connect(opts) do
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

  def handle_close(query, opts, state) do
    TestAgent.eval(:handle_close, [query, opts, state])
  end

  def handle_declare(query, params, opts, state) do
    TestAgent.eval(:handle_declare, [query, params, opts, state])
  end

  def handle_first(query, cursor, opts, state) do
    TestAgent.eval(:handle_first, [query, cursor, opts, state])
  end

  def handle_next(query, cursor, opts, state) do
    TestAgent.eval(:handle_next, [query, cursor, opts, state])
  end

  def handle_deallocate(query, cursor, opts, state) do
    TestAgent.eval(:handle_deallocate, [query, cursor, opts, state])
  end

  def handle_info(msg, state) do
    TestAgent.eval(:handle_info, [msg, state])
  end
end

defmodule TestQuery do
  defstruct [:state]
end

defmodule TestCursor do
  defstruct []
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

  def eval(fun, args) do
    eval(get_agent(args), fun, args)
  end

  def eval(agent, fun, args) do
    action = {fun, args}
    case Agent.get_and_update(agent, &get_and_update(&1, action)) do
      fun when is_function(fun) ->
        apply(fun, args)
      result ->
        result
    end
  end

  defp get_agent(args) do
    case Process.get(:agent) do
      agent when is_pid(agent) ->
        agent
      nil ->
        opts = get_opts(args)
        agent = Keyword.fetch!(opts, :agent)
        _ = Process.put(:agent, agent)
        agent
    end
  end

  defp get_opts([opts]),    do: opts
  defp get_opts([opts, _]), do: opts
  defp get_opts([_, opts, _]), do: opts
  defp get_opts([_, _, opts, _]), do: opts

  def record(agent) do
    Enum.reverse(Agent.get(agent, &elem(&1, 1)))
  end

  defp get_and_update({[next | stack], record}, action) do
    {next, {stack, [action | record]}}
  end
end
