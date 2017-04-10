defmodule DBConnection.Stage do
  @moduledoc """
  A `GenStage` process that encapsulates a transaction.
  """
  alias __MODULE__, as: Stage

  use GenStage

  @enforce_keys [:conn, :handle, :stop, :state, :opts, :type]
  defstruct [:conn, :handle, :stop, :state, :opts, :type,
             consumers: [], producers: %{}, active: [], done?: false]

  @start_opts [:name, :spawn_opt, :debug]
  @stage_opts [:demand, :buffer_size, :buffer_keep, :dispatcher, :subscribe_to]

  @doc """
  Start link a `GenStage` producer that will prepare a query, execute it and
  stream results using a cursor inside a transaction.

  The transaction is rolled back if the process terminates with a reason other
  than `:normal`.

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
  def prepare_stream(pool, query, params, opts \\ []) do
    stream(pool, &DBConnection.prepare_stream/4, query, params, opts)
  end

  @doc """
  Start link a `GenStage` producer that will execute a query and stream results
  using a cursor inside a transaction.

  The transaction is rolled back if the process terminates with a reason other
  than `:normal`.

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
  def stream(pool, query, params, opts \\ []) do
    stream(pool, &DBConnection.stream/4, query, params, opts)
  end

  @doc """
  Start link a `GenStage` process that will run a transaction for its duration.

  The first argument is the pool, the second argument is the `GenStage` type,
  the third argument is the start function, the fourth argument is the handle
  function, the fifth argument is the stop function and the optional sixth
  argument are the options.

  The start function is a 1-arity anonymous function with argument
  `DBConnection.t`. This is called after the transaction begins but before
  `start_link/6` returns. It should return the `state` or call
  `DBConnection.rollback/2` to stop the `GenStage`.

  The handle function is a 3-arity anonymous function. The first argument is the
  `DBConnection.t` for the transaction and the third argument is the state.
  If the `GenStage` type is a `:producer`, then the second argument is the
  `demand` from a `GenStage` `handle_demand` callback. Otherwise the second
  argument is the events from a `GenStage` `handle_events` callback. This
  function returns a 2-tuple, with first element as events (empty list for
  `:consumer`) and second element as the `state`. This function can roll back
  and stop the `GenStage` using `DBConnection.rollback/2`.

  The stop function is a 3-arity anonymous function. The first argument is the
  `DBConnection.t` for the transaction, the second argument is the terminate
  reason and the third argument is the `state`. This function will only be
  called if connection is alive and the transaction has not been rolled back. If
  this function returns the transaction is commited. This function can roll back
  and stop the `GenStage` using `DBConnection.rollback/2`.

  The `GenStage` process will behave like a `Flow` stage:

    * It will stop with reason `:normal` when the last consumer cancels
    * It will notify consumers that it is done when all producers have cancelled
    or notified that they are done or halted
    * It will cancel all remaining producers when all producers have notified
    that they are done or halted

  ### Options

    * `:name` - A name to register the started process (see the `:name` option
    in `GenServer.start_link/3`)
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

  The pool and connection module may support other options. All options
  are passed to `handle_begin/2`, `handle_commit/2` and `handle_rollback/2`.
  All options are passed to the `GenStage` on init.

  ### Example

      start = &DBConnection.prepare!(&1, %2, opts)
      handle =
        fn(conn, param, query) ->
          {[DBConection.execute!(conn, param, query, opts)], query}
        end
      stop = %DBConnection.close(&1, &2, opts)
      DBConnection.Stage.start_link(pool, :producer_consumer,
                                    start, handle, stop, opts)
  """
  @spec start_link(GenServer.server, :producer,
    ((DBConnection.t) -> state),
    ((DBConnection.t, demand :: pos_integer, state) -> {[any], state}),
    ((DBConnection.t, reason :: any, state) -> any), Keyword.t) ::
    GenServer.on_start when state: var
  @spec start_link(GenServer.server, :producer_consumer,
    ((DBConnection.t) -> state),
    ((DBConnection.t, [any], state) -> {[any], state}),
    ((DBConnection.t, reason :: any, state) -> any), Keyword.t) ::
    GenServer.on_start when state: var
  @spec start_link(GenServer.server, :consumer,
    ((DBConnection.t) -> state),
    ((DBConnection.t, [any], state) -> {[], state}),
    ((DBConnection.t, reason :: any, state) -> any), Keyword.t) ::
    GenServer.on_start when state: var
  def start_link(pool, type, start, handle, stop, opts \\ []) do
    start_opts = Keyword.take(opts, @start_opts)
    args = {pool, type, start, handle, stop, opts}
    GenStage.start_link(__MODULE__, args, start_opts)
  end

  @doc false
  def init({pool, type, start, handle, stop, opts}) do
    stage_opts = Keyword.take(opts, @stage_opts)
    conn = DBConnection.stage_begin(pool, opts)
    case DBConnection.stage_transaction(conn, start, opts) do
      {:ok, state} ->
        stage = %Stage{conn: conn, handle: handle, stop: stop, state: state,
                       opts: opts, type: type}
        {type, stage, stage_opts}
      {:error, reason} ->
        exit(reason)
    end
  end

  @doc false
  def handle_subscribe(:producer, _, {pid, ref} = from, stage) do
    case stage do
      %Stage{done?: true, type: :consumer, producers: producers} ->
        GenStage.cancel(from, :normal, [:noconnect])
        stage = %Stage{stage | producers: Map.put(producers, ref, pid)}
        {:manual, stage}
      %Stage{done?: true, type: :producer_consumer, producers: producers} ->
        stage = %Stage{stage | producers: Map.put(producers, ref, pid)}
        {:manual, stage}
      %Stage{done?: false, producers: producers, active: active} ->
        stage = %Stage{stage | producers: Map.put(producers, ref, pid),
                               active: [ref | active]}
        {:automatic, stage}
    end
  end
  def handle_subscribe(:consumer, _, {_, ref}, stage) do
    %Stage{consumers: consumers} = stage
    {:automatic, %Stage{stage | consumers: [ref | consumers]}}
  end

  @doc false
  def handle_cancel(_, {_, ref}, stage) do
    %Stage{type: type, consumers: consumers, producers: producers,
           active: active, done?: done?} = stage
    case producers do
      %{^ref => _} when active != [ref] or done? ->
        producers = Map.delete(producers, ref)
        active = List.delete(active, ref)
        {:noreply, [], %Stage{stage | active: active, producers: producers}}
      %{^ref => _} when type == :consumer ->
        producers = Map.delete(producers, ref)
        for {ref, pid} <- producers do
          GenStage.cancel({pid, ref}, :normal, [:noconnect])
        end
        stage = %Stage{stage | active: [], done?: true, producers: producers}
        {:noreply, [], stage}
      %{^ref => _} when type == :producer_consumer ->
        producers = Map.delete(producers, ref)
        GenStage.async_notify(self(), {:producer, :done})
        stage = %Stage{stage | active: [], done?: true, producers: producers}
        {:noreply, [], stage}
      %{} when consumers == [ref] ->
        {:stop, :normal, %Stage{stage | consumers: []}}
      %{} ->
        consumers = List.delete(consumers, ref)
        {:noreply, [], %Stage{stage | consumers: consumers}}
    end
  end

  def handle_info({{_, ref}, {:producer, state}}, stage) when state in [:halted, :done] do
    %Stage{type: type, producers: producers, active: active,
           done?: done?} = stage
    case producers do
      %{^ref => _} when active != [ref] or done? ->
        active = List.delete(active, ref)
        {:noreply, [], %Stage{stage | active: active}}
      %{^ref => _} when type == :consumer ->
        for {ref, pid} <- producers do
          GenStage.cancel({pid, ref}, :normal, [:noconnect])
        end
        {:noreply, [], %Stage{stage | active: [], done?: true}}
      %{^ref => _} when type == :producer_consumer ->
        GenStage.async_notify(self(), {:producer, :done})
        {:noreply, [], %Stage{stage | active: [], done?: true}}
      %{} ->
        {:noreply, [], stage}
    end
  end
  def handle_info(_, stage) do
    {:noreply, [], stage}
  end

  @doc false
  def handle_demand(demand, stage) do
    %Stage{conn: conn, handle: handle, state: state, opts: opts} = stage
    fun = &handle.(&1, demand, state)
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
  def handle_events(events, _, stage) do
    %Stage{conn: conn, handle: handle, state: state, opts: opts} = stage
    fun = &handle.(&1, events, state)
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
  def terminate(reason, stage) do
    %Stage{conn: conn, stop: stop, state: state, opts: opts} = stage
    fun = &stop.(&1, reason, state)
    case DBConnection.stage_transaction(conn, fun, opts) do
      {:ok, _} ->
        case DBConnection.stage_commit(conn, opts) do
          :ok ->
            :ok
          {:error, :rollback} ->
            exit(:rollback)
        end
      {:error, new_reason} ->
        DBConnection.stage_rollback(conn, opts)
        if new_reason != reason, do: exit(reason)
      :closed ->
        :ok
    end
  end

  ## Helpers

  defp stream(pool, stream_fun, query, params, opts) do
    start = &stream_start(&1, stream_fun, query, params, opts)
    start_link(pool, :producer, start, &stream_next/3, &stream_stop/3, opts)
  end

  defp stream_start(conn, stream_fun, query, params, opts) do
    stream = stream_fun.(conn, query, params, opts)
    {:suspended, _, cont} = Enumerable.reduce(stream, {:suspend, {0, []}}, &stream_reduce/2)
    {:cont, cont}
  end

  defp stream_reduce(v, {1, acc}) do
    {:suspend, {0, [v | acc]}}
  end
  defp stream_reduce(v, {n, acc}) do
    {:cont, {n-1, [v | acc]}}
  end

  defp stream_next(_, n, {:cont, cont}) when n > 0 do
    case cont.({:cont, {n, []}}) do
      {:suspended, {0, acc}, cont} ->
        {Enum.reverse(acc), {:cont, cont}}
      {state, {_, acc}} when state in [:halted, :done] ->
        GenStage.async_notify(self(), {:producer, state})
        {Enum.reverse(acc), state}
    end
  end
  defp stream_next(_, _, state) when state in [:halted, :done] do
    GenStage.async_notify(self(), {:producer, state})
    {[], state}
  end

  defp stream_stop(conn, reason, {:cont, cont}) do
    _ = cont.({:halt, {0, []}})
    stream_stop(conn, reason)
  end
  defp stream_stop(conn, reason, state) when state in [:halted, :done] do
    stream_stop(conn, reason)
  end

  defp stream_stop(_, :normal) do
    :ok
  end
  defp stream_stop(conn, reason) do
    DBConnection.rollback(conn, reason)
  end
end
