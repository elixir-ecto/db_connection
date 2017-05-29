defmodule DBConnection.Producer do
  @moduledoc """
  A `GenStage` producer that streams the result of a query, optionally
  encapsulated in a transaction.

  ### Options

    * `:stream_mapper` - A function to flat map the results of the query, either
    a 2-arity fun, `{module, function, args}` with `DBConnection.t` and the
    result prepended to `args` or `nil` (default: `nil`)
    * `:stage_prepare` - Whether the producer should prepare the query before
    streaming it (default: `false`)
    * `:stage_transaction` - Whether the producer should encapsulate the query
    in a transaction (default: `true`)
    * `:pool_timeout` - The maximum time to wait for a reply when making a
    synchronous call to the pool (default: `5_000`)
    * `:queue` - Whether to block waiting in an internal queue for the
    connection's state (boolean, default: `true`)
    * `:timeout` - The maximum time that the caller is allowed the
    to hold the connection's state (ignored when using a run/transaction
    connection, default: `15_000`)
    * `:log` - A function to log information about a call, either
    a 1-arity fun or `{module, function, args}` with `DBConnection.LogEntry.t`
    prepended to `args` or `nil`. See `DBConnection.LogEntry` (default: `nil`)

  The pool and connection module may support other options. All options
  are passed to `handle_begin/2`, `handle_prepare/3`, `handle_declare/4`,
  `handle_first/4`, `handle_next/4`, `handle_deallocate/4`, `handle_commit/2`
  and `handle_rollback/2`. In addition, the demand will be passed to
  `handle_first/4` and `handle_next/4` by adding `fetch: demand` to the options.
  """
  alias __MODULE__, as: Producer

  use GenStage

  @enforce_keys [:conn, :state, :transaction?, :opts]
  defstruct [:conn, :state, :transaction?, :opts]

  @start_opts [:name, :spawn_opt, :debug]
  @stage_opts [:demand, :buffer_size, :buffer_keep, :dispatcher]

  @doc """
  Start link a `GenStage` producer that will optionally prepare a query, execute
  it and stream results using a cursor inside a transaction.

  The transaction is rolled back if the process terminates with a reason other
  than `:normal`.

  For options see "Options" in the module documentation.

  ### Example

      query = %Query{statement: "SELECT id FROM table"}
      opts = [stream_mapper: &Map.fetch!(&1, :rows)]
      {:ok, stage} = DBConnection.Producer.start_link(pool, query, [], opts)
      stage |> GenStage.stream() |> Enum.to_list()
  """
  def start_link(pool, query, params, opts \\ []) do
    start_opts = Keyword.take(opts, @start_opts)
    args = {pool, query, params, opts}
    GenStage.start_link(__MODULE__, args, start_opts)
  end

  @doc false
  def init({pool, query, params, opts}) do
    stage_opts = Keyword.take(opts, @stage_opts)
    stage = init(pool, opts)
    state = run(&declare(&1, query, params, opts), opts, stage)
    {:producer, %Producer{stage | state: state}, stage_opts}
  end

  @doc false
  def handle_info(:stop, stage) do
    {:stop, :normal, stage}
  end
  def handle_info({:fetch, conn, pending}, %Producer{conn: conn} = stage) do
    handle_demand(pending, stage)
  end
  def handle_info(_, stage) do
    {:noreply, [], stage}
  end

  @doc false
  def handle_demand(demand, stage) do
    %Producer{conn: conn, state: state, opts: opts} = stage
    case run(&fetch(&1, demand, state, opts), opts, stage) do
      {:halt, state} ->
        GenStage.async_info(self(), :stop)
        {:noreply, [], %Producer{stage | state: state}}
      {events, state} ->
        # stream_mapper may not produce the desired number of events, i.e. at
        # the end of the results, so we can close the cursor as soon as
        # possible.
        pending = demand - length(events)
        _ = if pending > 0, do: send(self(), {:fetch, conn, pending})
        {:noreply, events, %Producer{stage | state: state}}
    end
  end

  @doc false
  def terminate(reason, %Producer{transaction?: true} = stage) do
    %Producer{conn: conn, state: state, opts: opts} = stage
    deallocate = &deallocate(&1, reason, state, opts)
    case DBConnection.transaction(conn, deallocate, opts) do
      {:ok, :normal} ->
        DBConnection.commit_checkin(conn, opts)
      {:ok, reason} ->
        DBConnection.rollback_checkin(conn, reason, opts)
      {:error, :rollback} ->
        :ok
    end
  end
  def terminate(reason, %Producer{transaction?: false} = stage) do
    %Producer{conn: conn, state: state, opts: opts} = stage
    try do
      deallocate(conn, reason, state, opts)
    after
      DBConnection.checkin(conn, opts)
    end
  end

  ## Helpers

  defp init(pool, opts) do
    case Keyword.get(opts, :stage_transaction, true) do
      true ->
        conn = DBConnection.checkout_begin(pool, opts)
        %Producer{conn: conn, transaction?: true, state: :declare, opts: opts}
      false ->
        conn = DBConnection.checkout(pool, opts)
        %Producer{conn: conn, transaction?: false, state: :declare, opts: opts}
    end
  end

  defp run(fun, opts, %Producer{conn: conn, transaction?: true}) do
    case DBConnection.transaction(conn, fun, opts) do
      {:ok, result} ->
         result
      {:error, reason} ->
        exit(reason)
    end
  end
  defp run(fun, opts, %Producer{conn: conn, transaction?: false}) do
    try do
      fun.(conn)
    catch
      kind, reason ->
        stack = System.stacktrace()
        DBConnection.checkin(conn, opts)
        :erlang.raise(kind, reason, stack)
    end
  end

  defp declare(conn, query, params, opts) do
    case Keyword.get(opts, :stage_prepare, false) do
      true ->
        DBConnection.prepare_declare(conn, query, params, opts)
      false ->
        DBConnection.declare(conn, query, params, opts)
    end
  end

  defp fetch(conn, demand, state, opts) do
    try do
      DBConnection.fetch(conn, state, [fetch: demand] ++ opts)
    catch
      kind, reason ->
        stack = System.stacktrace()
        DBConnection.deallocate(conn, state, opts)
        :erlang.raise(kind, reason, stack)
    end
  end

  defp deallocate(conn, reason, state, opts) do
    :ok = DBConnection.deallocate(conn, state, opts)
    reason
  end
end
