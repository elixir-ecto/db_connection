defmodule DBConnection.Consumer do
  @moduledoc """
  A `GenStage` consumer that runs a fun for each batch of events, optionally
  encapsulated in a transaction.

  ### Options

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
  are passed to `handle_begin/2`, `handle_commit/2` and `handle_rollback/2`.
  """
  alias __MODULE__, as: Consumer

  use GenStage

  @enforce_keys [:conn, :fun, :transaction?, :opts, :done?, :producers]
  defstruct [:conn, :fun, :transaction?, :opts, :done?, :producers]

  @start_opts [:name, :spawn_opt, :debug]
  @stage_opts [:subscribe_to]

  @doc """
  Start link a `GenStage` consumer thay will run an anonymous for each batch of
  events, optionally inside a transaction.

  The transaction is rolled back if the process terminates with a reason other
  than `:normal`. If the transaction is rolled back the process exits with the
  same reason.

  For options see "Options" in the module documentation.

  ### Example

      fun = fn(conn, rows) ->
        DBConnection.execute!(conn, statement, [rows])
      end
      {:ok, stage} = DBConnection.Consumer.start_link(pool, fun, opts)
      enum
      |> Flow.from_enumerable()
      |> Flow.map(&process_row/1)
      |> Flow.into_stages([{stage, cancel: :transient}]
  """
  def start_link(pool, fun, opts \\ []) when is_function(fun, 2) do
    start_opts = Keyword.take(opts, @start_opts)
    GenStage.start_link(__MODULE__, {pool, fun, opts}, start_opts)
  end

  @doc false
  def init({pool, fun, opts}) do
    stage_opts = Keyword.take(opts, @stage_opts)
    {:consumer, init(pool, fun, opts), stage_opts}
  end

  @doc false
  def handle_subscribe(:producer, _, {pid, ref}, consumer) do
    case consumer do
      %Consumer{done?: true} = consumer ->
        GenStage.cancel({pid, ref}, :normal, [:noconnect])
        {:manual, consumer}
      %Consumer{done?: false, producers: producers} = consumer ->
        new_producers = Map.put(producers, ref, pid)
        {:automatic, %Consumer{consumer | producers: new_producers}}
    end
  end

  @doc false
  def handle_cancel(_, {_, ref}, consumer) do
    %Consumer{producers: producers} = consumer
    case Map.delete(producers, ref) do
      new_producers when new_producers == %{} and producers != %{} ->
        GenStage.async_info(self(), :stop)
        {:noreply, [], %Consumer{consumer | done?: true, producers: %{}}}
      new_producers ->
        {:noreply, [], %Consumer{consumer | producers: new_producers}}
    end
  end

  @doc false
  def handle_info(:stop, state) do
    {:stop, :normal, state}
  end
  def handle_info(_msg, state) do
    {:noreply, [], state}
  end

  @doc false
  def handle_events(events, _, %Consumer{transaction?: true} = consumer) do
    %Consumer{conn: conn, fun: fun, opts: opts} = consumer
    case DBConnection.transaction(conn, &fun.(&1, events), opts) do
      {:ok, _} ->
        {:noreply, [], consumer}
      {:error, reason} ->
        exit(reason)
    end
  end
  def handle_events(events, _, %Consumer{transaction?: false} = consumer) do
    %Consumer{conn: conn, fun: fun, opts: opts} = consumer
    _ = DBConnection.run(conn, &fun.(&1, events), opts)
    {:noreply, [], consumer}
  end

  @doc false
  def terminate(reason, %Consumer{transaction?: true} = stage) do
    %Consumer{conn: conn, opts: opts} = stage
    case DBConnection.transaction(conn, fn(_) -> reason end, opts) do
      {:ok, :normal} ->
        DBConnection.commit_checkin(conn, opts)
      {:ok, reason} ->
        DBConnection.rollback_checkin(conn, reason, opts)
      {:error, :rollback} ->
        :ok
    end
  end
  def terminate(_, %Consumer{transaction?: false} = stage) do
    %Consumer{conn: conn, opts: opts} = stage
    DBConnection.checkin(conn, opts)
  end

  ## Helpers

  defp init(pool, fun, opts) do
    case Keyword.get(opts, :stage_transaction, true) do
      true ->
        pool
        |> DBConnection.checkout_begin(opts)
        |> init(true, fun, opts)
      false ->
        pool
        |> DBConnection.checkout(opts)
        |> init(false, fun, opts)
    end
  end

  defp init(conn, transaction?, fun, opts) do
    %Consumer{conn: conn, transaction?: transaction?, fun: fun, opts: opts,
              done?: false, producers: %{}}
  end
end
