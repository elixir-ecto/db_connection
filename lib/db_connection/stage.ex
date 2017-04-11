defmodule DBConnection.Stage do
  @moduledoc """
  A `GenStage` process that encapsulates a transaction.

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
  are passed to `handle_begin/2`, `handle_commit/2` and `handle_rollback/2`.
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

  All options are passed to `DBConnection.prepare_stream/4`. For options see
  "Options" in the module documentation.

  ### Example

      query = %Query{statement: "SELECT id FROM table"}
      {:ok, stage} = DBConnection.Stage.prepare_stream(pool, query, [])
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

  All options are passed to `DBConnection.stream/4`. For options see "Options"
  in the module documentation

  ### Example

      query = DBConnection.prepare!(pool, %Query{statement: "SELECT id FROM table"})
      {:ok, stage} = DBConnection.Stage.stream(pool, query, [])
      stage |> Flow.from_stage() |> Enum.to_list()
      end)
  """
  def stream(pool, query, params, opts \\ []) do
    stream(pool, &DBConnection.stream/4, query, params, opts)
  end

  @doc """
  Start link a `GenStage` producer that will run a transaction for its duration.

  The first argument is the pool, the second argument is the start function,
  the third argument is the handle demand function, the fourth argument is the
  stop function and the optional fiftth argument are the options.

  The start function is a 1-arity anonymous function with argument
  `DBConnection.t`. This is called after the transaction begins but before
  `producer/5` returns. It should return the accumulator or call
  `DBConnection.rollback/2` to stop the `GenStage`.

  The handle demand function is a 3-arity anonymous function. The first argument
  is the `DBConnection.t` for the transaction, the second argument is the
  `demand`, and the third argument is the accumulator. This function returns a
  2-tuple, with first element as list of events to fulfil the demand and second
  element as the accumulator. If the producer has emitted all events (and so
  not fulfilled demand) it should call
  `GenStage.async_notify(self(), {:producer, :done | :halted}` to signal to
  consumers that it has finished. Also this function can rollback and stop the
  `GenStage` using `DBConnection.rollback/2`.

  The stop function is a 3-arity anonymous function. The first argument is the
  `DBConnection.t` for the transaction, the second argument is the terminate
  reason and the third argument is the accumulator. This function will only be
  called if connection is alive and the transaction has not been rolled back. If
  this function returns the transaction is committed. This function can rollback
  and stop the `GenStage` using `DBConnection.rollback/2`.

  For options see "Options" in the module documentation.

  The `GenStage` process will behave like a `Flow` stage:

    * It will stop with reason `:normal` when the last consumer cancels

  ### Example

      start =
        fn(conn) ->
          {ids, DBConnection.prepare!(conn, query, opts)}
        end
      handle_demand =
        fn(conn, demand, {ids, query}) ->
          {param, rem} = Enum.split(ids, demand)
          %{rows: rows} = DBConection.execute!(conn, query, [param])
          if rem == [], do: GenStage.async_notify(self(), {:producer, :done})
          {rows, {rem, query}}
        end
      stop =
        fn(conn, reason, {rem, query}) ->
          DBConnection.close!(conn, query, opts)
          if reason != :normal or rem != [] do
            DBConnection.rollback(conn, reason)
          end
        end
      DBConnection.Stage.producer(pool, start, handle_demand, stop)
  """
  @spec producer(GenServer.server, ((DBConnection.t) -> acc),
    ((DBConnection.t, demand :: pos_integer, acc) -> {[any], acc}),
    ((DBConnection.t, reason :: any, acc) -> any), Keyword.t) ::
    GenServer.on_start when acc: var
  def producer(pool, start, handle_demand, stop, opts \\ []) do
    start_link(pool, :producer, start, handle_demand, stop, opts)
  end

  @doc """
  Start link a `GenStage` producer consumer that will run a transaction for its
  duration.

  The first argument is the pool, the second argument is the start function,
  the third argument is the handle events function, the fourth argument is the
  stop function and the optional fiftth argument are the options.

  The start function is a 1-arity anonymous function with argument
  `DBConnection.t`. This is called after the transaction begins but before
  `producer_consumer/5` returns. It should return the accumulator or call
  `DBConnection.rollback/2` to stop the `GenStage`.

  The handle events function is a 3-arity anonymous function. The first argument
  is the `DBConnection.t` for the transaction, the second argument is a list of
  incoming events, and the third argument is the accumulator. This function
  returns a 2-tuple, with first element as list of outgoing events and second
  element as the accumulator. Also this function can rollback and stop the
  `GenStage` using `DBConnection.rollback/2`.

  The stop function is a 3-arity anonymous function. The first argument is the
  `DBConnection.t` for the transaction, the second argument is the terminate
  reason and the third argument is the accumulator. This function will only be
  called if connection is alive and the transaction has not been rolled back. If
  this function returns the transaction is committed. This function can rollback
  and stop the `GenStage` using `DBConnection.rollback/2`.

  For options see "Options" in the module documentation.

  The `GenStage` process will behave like a `Flow` stage:

    * It will stop with reason `:normal` when the last consumer cancels
    * It will notify consumers that it is done when all producers have cancelled
    or notified that they are done or halted
    * It will not send demand to new producers when all producers have notified
    that they are done or halted

  ### Example

      start =
        fn(conn) ->
          DBConnection.prepare!(conn, query, opts)
        end
      handle_events =
        fn(conn, ids, query) ->
          %{rows: rows} = DBConection.execute!(conn, query, [ids])
          {rows, query}
        end
      stop =
        fn(conn, reason, query) ->
          DBConnection.close!(conn, query, opts)
          if reason != :normal do
            DBConnection.rollback(conn, reason)
          end
        end
      DBConnection.Stage.consumer_producer(pool, start, handle_events, stop)
  """
  @spec producer_consumer(GenServer.server, ((DBConnection.t) -> acc),
    ((DBConnection.t, events_in :: [any], acc) -> {events_out :: [any], acc}),
    ((DBConnection.t, reason :: any, acc) -> any), Keyword.t) ::
    GenServer.on_start when acc: var
  def producer_consumer(pool, start, handle_events, stop, opts \\ []) do
    start_link(pool, :producer_consumer, start, handle_events, stop, opts)
  end

  @doc """
  Start link a `GenStage` consumer that will run a transaction for its duration.

  The first argument is the pool, the second argument is the start function,
  the third argument is the handle events function, the fourth argument is the
  stop function and the optional fiftth argument are the options.

  The start function is a 1-arity anonymous function with argument
  `DBConnection.t`. This is called after the transaction begins but before
  `consumer/5` returns. It should return the accumulator or call
  `DBConnection.rollback/2` to stop the `GenStage`.

  The handle events function is a 3-arity anonymous function. The first argument
  is the `DBConnection.t` for the transaction, the second argument is the
  events, and the third argument is the accumulator. This function returns a
  2-tuple, with first element is an empty list (as no outgoing events) and
  second element as the accumulator. Also this function can rollback and stop
  the `GenStage` using `DBConnection.rollback/2`.

  The stop function is a 3-arity anonymous function. The first argument is the
  `DBConnection.t` for the transaction, the second argument is the terminate
  reason and the third argument is the accumulator. This function will only be
  called if connection is alive and the transaction has not been rolled back. If
  this function returns the transaction is committed. This function can rollback
  and stop the `GenStage` using `DBConnection.rollback/2`.

  For options see "Options" in the module documentation.

  The `GenStage` process will behave like a `Flow` stage:

    * It will cancel new and remaining producers when all producers have
    notified that they are done or halted and it is a `:consumer`

  ### Example

      start =
        fn(conn) ->
          DBConnection.prepare!(conn, query, opts)
        end
      handle_events =
        fn(conn, ids, query) ->
          DBConection.execute!(conn, query, [ids])
          {[], query}
        end
      stop =
        fn(conn, reason, query) ->
          DBConnection.close!(conn, query, opts)
          if reason != :normal do
            DBConnection.rollback(conn, reason)
          end
        end
      DBConnection.Stage.consumer(pool, start, handle_events, stop)
  """
  @spec consumer(GenServer.server, ((DBConnection.t) -> acc),
    ((DBConnection.t, events_in :: [any], acc) -> {[], acc}),
    ((DBConnection.t, reason :: any, acc) -> any), Keyword.t) ::
    GenServer.on_start when acc: var
  def consumer(pool, start, handle_events, stop, opts \\ []) do
    start_link(pool, :consumer, start, handle_events, stop, opts)
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
  def handle_subscribe(:consumer, _, {pid, ref}, stage) do
    %Stage{done?: done?, consumers: consumers} = stage
    if done?, do: send(pid, {{self(), ref}, {:producer, :done}})
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

  @doc false
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
    producer(pool, start, &stream_next/3, &stream_stop/3, opts)
  end

  defp start_link(pool, type, start, handle, stop, opts) do
    start_opts = Keyword.take(opts, @start_opts)
    args = {pool, type, start, handle, stop, opts}
    GenStage.start_link(__MODULE__, args, start_opts)
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
