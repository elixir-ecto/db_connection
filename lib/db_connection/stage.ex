defmodule DBConnection.Stage do
  @moduledoc """
  A `GenStage` producer that encapsulates a transaction and streams the
  result of a query.

  ### Options

    * `:stream_map` - A function to flat map the results of the query, either a
    2-arity fun, `{module, function, args}` with `DBConnection.t` and the result
    prepended to `args` or `nil` (default: `nil`)
    * `:stage_prepare` - Whether the consumer should prepare the query before
    streaming it (default: `false`)
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
  alias __MODULE__, as: Stage

  use GenStage

  @enforce_keys [:conn, :state, :opts]
  defstruct [:conn, :state, :opts]

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
      opts = [stream_map: &Map.fetch!(&1, :rows)]
      {:ok, stage} = DBConnection.Stage.start_link(pool, query, [], opts)
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
    conn = DBConnection.resource_begin(pool, opts)
    declare = &declare(&1, query, params, opts)
    case DBConnection.resource_transaction(conn, declare, opts) do
      {:ok, state} ->
        {:producer, %Stage{conn: conn, state: state, opts: opts}, stage_opts}
      {:error, reason} ->
        exit(reason)
    end
  end

  @doc false
  def handle_info(:stop, stage) do
    {:stop, :normal, stage}
  end
  def handle_info({:fetch, conn, pending}, %Stage{conn: conn} = stage) do
    handle_demand(pending, stage)
  end
  def handle_info(_, stage) do
    {:noreply, [], stage}
  end

  @doc false
  def handle_demand(demand, stage) do
    %Stage{conn: conn, state: state, opts: opts} = stage
    fetch = &fetch(&1, demand, state, opts)
    case DBConnection.resource_transaction(conn, fetch, opts) do
      {:ok, {:halt, state}} ->
        GenStage.async_info(self(), :stop)
        {:noreply, [], %Stage{stage | state: state}}
      {:ok, {events, state}} ->
        # stream_map may produce the desired number of events, i.e. at the end
        # of the results so we can close the cursor as soon as possible.
        pending = demand - length(events)
        _ = if pending > 0, do: send(self(), {:fetch, conn, pending})
        {:noreply, events, %Stage{stage | state: state}}
      {:error, reason} ->
        exit(reason)
      :closed ->
        raise DBConnection.ConnectionError, "connection is closed"
    end
  end

  @doc false
  def terminate(reason, stage) do
    %Stage{conn: conn, state: state, opts: opts} = stage
    deallocate = &deallocate(&1, reason, state, opts)
    case DBConnection.resource_transaction(conn, deallocate, opts) do
      {:ok, :normal} ->
        DBConnection.resource_commit(conn, opts)
      {:ok, reason} ->
        DBConnection.resource_rollback(conn, reason, opts)
      {:error, :rollback} ->
        :ok
    end
  end

  ## Helpers

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
