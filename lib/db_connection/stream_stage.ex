defmodule DBConnection.StreamStage do
  @moduledoc """
  A `GenStage` producer that streams the results of a query inside a
  transaction.
  """
  alias __MODULE__, as: Stage

  use GenStage

  @enforce_keys [:conn, :state, :opts]
  defstruct [:conn, :state, :opts]

  @start_opts [:name, :spawn_opt, :debug]
  @stage_opts [:demand, :buffer_size, :buffer_keep, :dispatcher]

  @doc """
  Start link a `GenStage` producer that will prepare a query, execute it and
  stream results using a cursor.

  ### Options

    * `:pool_timeout` - The maximum time to wait for a reply when making a
    synchronous call to the pool (default: `5_000`)
    * `:queue` - Whether to block waiting in an internal queue for the
    connection's state (boolean, default: `true`)
    * `:timeout` - The maximum time that the caller is allowed the
    to hold the connection's state (ignored when using a run/transaction
    connection, default: `15_000`)
    * `:log` - A function to log information about a call, either
    a 1-arity fun, `{module, function, args}` with `DBConnection.LogEntry.t`
    prepended to `args` or `nil`. See `DBConnection.LogEntry` (default: `nil`)
    * `:flat_map` - A function to flat map stream results, either a 2-arity fun
    or `{module, function, args}` with `conn` and `result` prepended to `args`,
    or `nil`.

  The pool and connection module may support other options. All options
  are passed to `handle_begin/2`, `handle_prepare/3, `handle_close/3,
  `handle_declare/4`, `handle_first/4`, `handle_next/4`, `handle_deallocate/4`,
  `handle_commit/2` and `handle_rollback/2`.

  ### Example

      query  = %Query{statement: "SELECT id FROM table"}
      {:ok, stage} = DBConnection.StreamStage.prepare_stream_link(conn, query, [])
      stage |> Flow.from_stage() |> Enum.to_list()
      end)
  """
  def prepare_stream_link(pool, query, params, opts \\ []) do
    start_link(pool, &DBConnection.prepare_stream/4, query, params, opts)
  end

  @doc """
  Start link a `GenStage` producer that will execute a query and stream results
  using a cursor.

  ### Options

    * `:pool_timeout` - The maximum time to wait for a reply when making a
    synchronous call to the pool (default: `5_000`)
    * `:queue` - Whether to block waiting in an internal queue for the
    connection's state (boolean, default: `true`)
    * `:timeout` - The maximum time that the caller is allowed the
    to hold the connection's state (ignored when using a run/transaction
    connection, default: `15_000`)
    * `:log` - A function to log information about a call, either
    a 1-arity fun, `{module, function, args}` with `DBConnection.LogEntry.t`
    prepended to `args` or `nil`. See `DBConnection.LogEntry` (default: `nil`)
    * `:flat_map` - A function to flat map stream results, either a 2-arity fun
    or `{module, function, args}` with `conn` and `result` prepended to `args`,
    or `nil`.

  The pool and connection module may support other options. All options
  are passed to `handle_begin/2`, `handle_declare/4`, `handle_first/4`,
  `handle_next/4`, `handle_deallocate/4`, `handle_commit/2` and
  `handle_rollback/2`.

  ### Example

      query  = %Query{statement: "SELECT id FROM table"}
      {:ok, stage} = DBConnection.StreamStage.stream_link(conn, query, [])
      stage |> Flow.from_stage() |> Enum.to_list()
      end)
  """
  def stream_link(pool, query, params, opts \\ []) do
    start_link(pool, &DBConnection.stream/4, query, params, opts)
  end

  @doc false
  def init({pool, stream, query, params, opts}) do
    {stage_opts, db_opts} = Keyword.split(opts, @stage_opts)
    conn = DBConnection.stage_begin(pool, db_opts)
    init = &start(&1, stream, query, params, db_opts)
    case DBConnection.stage_transaction(conn, init, db_opts) do
      {:ok, state} ->
        {:producer, %Stage{conn: conn, state: state, opts: db_opts}, stage_opts}
      {:error, reason} ->
        exit(reason)
    end
  end

  @doc false
  def handle_demand(demand, stage) do
    %Stage{conn: conn, state: state, opts: opts} = stage
    fun = fn(_) -> next(state, demand) end
    case DBConnection.stage_transaction(conn, fun, opts) do
      {:ok, {events, state}} ->
        {:noreply, events, %Stage{stage | state: state}}
      {:error, reason} ->
        exit(reason)
      :closed ->
        raise DBConnection.ConnectionError, "connection is closed"
    end
  end

  @doc false
  def terminate(reason, %Stage{conn: conn, state: state, opts: opts}) do
    case DBConnection.stage_transaction(conn, fn(_) -> stop(state) end, opts) do
      {:ok, _} when reason == :normal ->
        DBConnection.stage_commit(conn, opts)
      {:ok, _} ->
        DBConnection.stage_rollback(conn, opts)
      {:error, reason} ->
        exit(reason)
      :closed ->
        :ok
    end
  end

  ## Helpers

  defp start_link(pool, stream_fun, query, params, opts) do
    {start_opts, opts} = Keyword.split(opts, @start_opts)
    args =  {pool, stream_fun, query, params, opts}
    GenStage.start_link(__MODULE__, args, start_opts)
  end

  defp start(conn, stream_fun, query, params, opts) do
    stream = stream_fun.(conn, query, params, opts)
    {:suspended, _, cont} = start(conn, stream, opts)
    {:cont, cont}
  end

  defp start(conn, stream, opts) do
    stream
    |> flat_map(conn, opts)
    |> Enumerable.reduce({:suspend, {0, []}}, &stream_reduce/2)
  end

  defp flat_map(stream, conn, opts) do
    case Keyword.get(opts, :flat_map) do
      nil ->
        stream
      map when is_function(map, 2) ->
        Stream.flat_map(stream, fn(elem) -> map.(conn, elem) end)
      {mod, fun, args} ->
        map = fn(elem) -> apply(mod, fun, [conn, elem | args]) end
        Stream.flat_map(stream, map)
    end
  end

  defp stream_reduce(v, {1, acc}) do
    {:suspend, {0, [v | acc]}}
  end
  defp stream_reduce(v, {n, acc}) do
    {:cont, {n-1, [v | acc]}}
  end

  defp next({:cont, cont}, n) when n > 0 do
    case cont.({:cont, {n, []}}) do
      {:suspended, {0, acc}, cont} ->
        {Enum.reverse(acc), {:cont, cont}}
      {state, {_, acc}} when state in [:halted, :done] ->
        GenStage.async_notify(self(), {:producer, state})
        {Enum.reverse(acc), state}
    end
  end
  defp next(state, _) when state in [:halted, :done] do
    GenStage.async_notify(self(), {:producer, state})
    {[], state}
  end

  defp stop({:cont, cont}) do
    _ = cont.({:halt, {0, []}})
    :ok
  end
  defp stop(state) when state in [:halted, :done] do
    :ok
  end
end
