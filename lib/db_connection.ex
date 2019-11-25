defmodule DBConnection.Stream do
  defstruct [:conn, :query, :params, :opts]

  @type t :: %__MODULE__{conn: DBConnection.conn,
                         query: any,
                         params: any,
                         opts: Keyword.t}
end

defimpl Enumerable, for: DBConnection.Stream do
  def count(_), do: {:error, __MODULE__}

  def member?(_, _), do: {:error, __MODULE__}

  def slice(_), do: {:error, __MODULE__}

  def reduce(stream, acc, fun), do: DBConnection.reduce(stream, acc, fun)
end

defmodule DBConnection.PrepareStream do
  defstruct [:conn, :query, :params, :opts]

  @type t :: %__MODULE__{conn: DBConnection.conn,
                         query: any,
                         params: any,
                         opts: Keyword.t}
end

defimpl Enumerable, for: DBConnection.PrepareStream do
  def count(_), do: {:error, __MODULE__}

  def member?(_, _), do: {:error, __MODULE__}

  def slice(_), do: {:error, __MODULE__}

  def reduce(stream, acc, fun), do: DBConnection.reduce(stream, acc, fun)
end

defmodule DBConnection do
  @moduledoc """
  A behaviour module for implementing efficient database connection
  client processes, pools and transactions.

  `DBConnection` handles callbacks differently to most behaviours. Some
  callbacks will be called in the calling process, with the state
  copied to and from the calling process. This is useful when the data
  for a request is large and means that a calling process can interact
  with a socket directly.

  A side effect of this is that query handling can be written in a
  simple blocking fashion, while the connection process itself will
  remain responsive to OTP messages and can enqueue and cancel queued
  requests.

  If a request or series of requests takes too long to handle in the
  client process a timeout will trigger and the socket can be cleanly
  disconnected by the connection process.

  If a calling process waits too long to start its request it will
  timeout and its request will be cancelled. This prevents requests
  building up when the database can not keep up.

  If no requests are received for an idle interval, the pool will
  ping all stale connections which can then ping the database to keep
  the connection alive.

  Should the connection be lost, attempts will be made to reconnect with
  (configurable) exponential random backoff to reconnect. All state is
  lost when a connection disconnects but the process is reused.

  The `DBConnection.Query` protocol provide utility functions so that
  queries can be encoded and decoded without blocking the connection or pool.
  """
  require Logger

  alias DBConnection.Holder
  defstruct [:pool_ref, :conn_ref, :conn_mode]

  defmodule EncodeError do
    defexception [:message]
  end

  defmodule TransactionError do
    defexception [:status, :message]

    def exception(:idle),
      do: %__MODULE__{status: :idle, message: "transaction is not started"}
    def exception(:transaction),
      do: %__MODULE__{status: :transaction, message: "transaction is already started"}
    def exception(:error),
      do: %__MODULE__{status: :error, message: "transaction is aborted"}
  end

  @typedoc """
  Run or transaction connection reference.
  """
  @type t :: %__MODULE__{pool_ref: any, conn_ref: reference}
  @type conn :: GenServer.server | t
  @type query :: DBConnection.Query.t
  @type params :: any
  @type result :: any
  @type cursor :: any
  @type status :: :idle | :transaction | :error

  @type start_option ::
          {:after_connect, (t -> any) | {module, atom, [any]} | nil}
          | {:after_connect_timeout, timeout}
          | {:backoff_max, non_neg_integer}
          | {:backoff_min, non_neg_integer}
          | {:backoff_type, :stop | :exp | :rand | :rand_exp}
          | {:configure, (keyword -> keyword) | {module, atom, [any]} | nil}
          | {:idle_interval, non_neg_integer}
          | {:max_restarts, non_neg_integer}
          | {:max_seconds, pos_integer}
          | {:name, GenServer.name()}
          | {:pool, module}
          | {:pool_size, pos_integer}
          | {:queue_interval, non_neg_integer}
          | {:queue_target, non_neg_integer}
          | {:show_sensitive_data_on_connection_error, boolean}

  @type option ::
          {:log, (DBConnection.LogEntry.t -> any) | {module, atom, [any]} | nil}
          | {:queue, boolean}
          | {:timeout, timeout}
          | {:deadline, integer | nil}

  @doc """
  Connect to the database. Return `{:ok, state}` on success or
  `{:error, exception}` on failure.

  If an error is returned it will be logged and another
  connection attempt will be made after a backoff interval.

  This callback is called in the connection process.
  """
  @callback connect(opts :: Keyword.t) ::
    {:ok, state :: any} | {:error, Exception.t}

  @doc """
  Checkouts the state from the connection process. Return `{:ok, state}`
  to allow the checkout or `{:disconnect, exception, state}` to disconnect.

  This callback is called when the control of the state is passed to
  another process. `c:checkin/1` is called with the new state when control
  is returned to the connection process.

  This callback is called in the connection process.
  """
  @callback checkout(state :: any) ::
    {:ok, new_state :: any} | {:disconnect, Exception.t, new_state :: any}

  @doc """
  Checks in the state to the connection process. Return `{:ok, state}`
  to allow the checkin or `{:disconnect, exception, state}` to disconnect.

  This callback is called when the control of the state is passed back
  to the connection process. It should reverse any changes to the
  connection state made in `c:checkout/1`.

  This callback is called in the connection process.
  """
  @callback checkin(state :: any) ::
    {:ok, new_state :: any} | {:disconnect, Exception.t, new_state :: any}

  @doc """
  Called when the connection has been idle for a period of time. Return
  `{:ok, state}` to continue or `{:disconnect, exception, state}` to
  disconnect.

  This callback is called if no callbacks have been called after the
  idle timeout and a client process is not using the state. The idle
  timeout can be configured by the `:idle_interval` option. This function
  can be called whether the connection is checked in or checked out.

  This callback is called in the connection process.
  """
  @callback ping(state :: any) ::
    {:ok, new_state :: any} | {:disconnect, Exception.t, new_state :: any}

  @doc """
  Handle the beginning of a transaction.

  Return `{:ok, result, state}` to continue, `{status, state}` to notify caller
  that the transaction can not begin due to the transaction status `status`,
  `{:error, exception, state}` (deprecated) to error without beginning the
  transaction, or `{:disconnect, exception, state}` to error and disconnect.

  A callback implementation should only return `status` if it
  can determine the database's transaction status without side effect.

  This callback is called in the client process.
  """
  @callback handle_begin(opts :: Keyword.t, state :: any) ::
    {:ok, result, new_state :: any} |
    {status, new_state :: any} |
    {:disconnect, Exception.t, new_state :: any}

  @doc """
  Handle committing a transaction. Return `{:ok, result, state}` on successfully
  committing transaction, `{status, state}` to notify caller that the
  transaction can not commit due to the transaction status `status`,
  `{:error, exception, state}` (deprecated) to error and no longer be inside
  transaction, or `{:disconnect, exception, state}` to error and disconnect.

  A callback implementation should only return `status` if it
  can determine the database's transaction status without side effect.

  This callback is called in the client process.
  """
  @callback handle_commit(opts :: Keyword.t, state :: any) ::
    {:ok, result, new_state :: any} |
    {status, new_state :: any} |
    {:disconnect, Exception.t, new_state :: any}

  @doc """
  Handle rolling back a transaction. Return `{:ok, result, state}` on successfully
  rolling back transaction, `{status, state}` to notify caller that the
  transaction can not rollback due to the transaction status `status`,
  `{:error, exception, state}` (deprecated) to
  error and no longer be inside transaction, or
  `{:disconnect, exception, state}` to error and disconnect.

  A callback implementation should only return `status` if it
  can determine the database' transaction status without side effect.

  This callback is called in the client and connection process.
  """
  @callback handle_rollback(opts :: Keyword.t, state :: any) ::
    {:ok, result, new_state :: any} |
    {status, new_state :: any} |
    {:disconnect, Exception.t, new_state :: any}

  @doc """
  Handle getting the transaction status. Return `{:idle, state}` if outside a
  transaction, `{:transaction, state}` if inside a transaction,
  `{:error, state}` if inside an aborted transaction, or
  `{:disconnect, exception, state}` to error and disconnect.

  If the callback returns a `:disconnect` tuples then `status/2` will return
  `:error`.
  """
  @callback handle_status(opts :: Keyword.t, state :: any) ::
    {status, new_state :: any} |
    {:disconnect, Exception.t, new_state :: any}

  @doc """
  Prepare a query with the database. Return `{:ok, query, state}` where
  `query` is a query to pass to `execute/4` or `close/3`,
  `{:error, exception, state}` to return an error and continue or
  `{:disconnect, exception, state}` to return an error and disconnect.

  This callback is intended for cases where the state of a connection is
  needed to prepare a query and/or the query can be saved in the
  database to call later.

  This callback is called in the client process.
  """
  @callback handle_prepare(query, opts :: Keyword.t, state :: any) ::
    {:ok, query, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @doc """
  Execute a query prepared by `c:handle_prepare/3`. Return
  `{:ok, query, result, state}` to return altered query `query` and result
  `result` and continue, `{:error, exception, state}` to return an error and
  continue or `{:disconnect, exception, state}` to return an error and
  disconnect.

  This callback is called in the client process.
  """
  @callback handle_execute(query, params, opts :: Keyword.t, state :: any) ::
    {:ok, query, result, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @doc """
  Close a query prepared by `c:handle_prepare/3` with the database. Return
  `{:ok, result, state}` on success and to continue,
  `{:error, exception, state}` to return an error and continue, or
  `{:disconnect, exception, state}` to return an error and disconnect.

  This callback is called in the client process.
  """
  @callback handle_close(query, opts :: Keyword.t, state :: any) ::
    {:ok, result, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @doc """
  Declare a cursor using a query prepared by `c:handle_prepare/3`. Return
  `{:ok, query, cursor, state}` to return altered query `query` and cursor
  `cursor` for a stream and continue, `{:error, exception, state}` to return an
  error and continue or `{:disconnect, exception, state}` to return an error
  and disconnect.

  This callback is called in the client process.
  """
  @callback handle_declare(query, params, opts :: Keyword.t, state :: any) ::
    {:ok, query, cursor, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @doc """
  Fetch the next result from a cursor declared by `c:handle_declare/4`. Return
  `{:cont, result, state}` to return the result `result` and continue using
  cursor, `{:halt, result, state}` to return the result `result` and close the
  cursor, `{:error, exception, state}` to return an error and close the
  cursor, `{:disconnect, exception, state}` to return an error and disconnect.

  This callback is called in the client process.
  """
  @callback handle_fetch(query, cursor, opts :: Keyword.t, state :: any) ::
    {:cont | :halt, result, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @doc """
  Deallocate a cursor declared by `c:handle_declare/4` with the database. Return
  `{:ok, result, state}` on success and to continue,
  `{:error, exception, state}` to return an error and continue, or
  `{:disconnect, exception, state}` to return an error and disconnect.

  This callback is called in the client process.
  """
  @callback handle_deallocate(query, cursor, opts :: Keyword.t, state :: any) ::
    {:ok, result, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @doc """
  Disconnect from the database. Return `:ok`.

  The exception as first argument is the exception from a `:disconnect`
  3-tuple returned by a previous callback.

  If the state is controlled by a client and it exits or takes too long
  to process a request the state will be last known state. In these
  cases the exception will be a `DBConnection.ConnectionError`.

  This callback is called in the connection process.
  """
  @callback disconnect(err :: Exception.t, state :: any) :: :ok

  @doc """
  Use `DBConnection` to set the behaviour.
  """
  defmacro __using__(_) do
    quote location: :keep do
      @behaviour DBConnection
    end
  end

  @doc """
  Starts and links to a database connection process.

  By default the `DBConnection` starts a pool with a single connection.
  The size of the pool can be increased with `:pool_size`. A separate
  pool can be given with the `:pool` option.

  ### Options

    * `:backoff_min` - The minimum backoff interval (default: `1_000`)
    * `:backoff_max` - The maximum backoff interval (default: `30_000`)
    * `:backoff_type` - The backoff strategy, `:stop` for no backoff and
    to stop, `:exp` for exponential, `:rand` for random and `:rand_exp` for
    random exponential (default: `:rand_exp`)
    * `:configure` - A function to run before every connect attempt to
    dynamically configure the options, either a 1-arity fun,
    `{module, function, args}` with options prepended to `args` or `nil` where
    only returned options are passed to connect callback (default: `nil`)
    * `:after_connect` - A function to run on connect using `run/3`, either
    a 1-arity fun, `{module, function, args}` with `t:DBConnection.t/0` prepended
    to `args` or `nil` (default: `nil`)
    * `:after_connect_timeout` - The maximum time allowed to perform
    function specified by `:after_connect` option (default: `15_000`)
    * `:name` - A name to register the started process (see the `:name` option
      in `GenServer.start_link/3`)
    * `:pool` - Chooses the pool to be started
    * `:pool_size` - Chooses the size of the pool
    * `:idle_interval` - Controls the frequency we ping the database when the
      connection is idle
    * `:queue_target` and `:queue_interval` - See "Queue config" below
    * `:max_restarts` and `:max_seconds` - Configures the `:max_restarts` and
      `:max_seconds` for the connection pool supervisor (see the `Supervisor` docs)
    * `:show_sensitive_data_on_connection_error` - By default, `DBConnection`
      hides all information during connection errors to avoid leaking credentials
      or other sensitive information. You can set this option if you wish to
      see complete errors and stacktraces during connection errors

  ### Example

      {:ok, conn} = DBConnection.start_link(mod, [idle_interval: 5_000])

  ## Queue config

  Handling requests is done through a queue. When DBConnection is
  started, there are two relevant options to control the queue:

    * `:queue_target` in milliseconds, defaults to 50ms
    * `:queue_interval` in milliseconds, defaults to 1000ms

  Our goal is to wait at most `:queue_target` for a connection.
  If all connections checked out during a `:queue_interval` takes
  more than `:queue_target`, then we double the `:queue_target`.
  If checking out connections take longer than the new target,
  then we start dropping messages.

  For example, by default our target is 50ms. If all connections
  checkouts take longer than 50ms for a whole second, we double
  the target to 100ms and we start dropping messages if the
  time to checkout goes above the new limit.

  This allows us to better plan for overloads as we can refuse
  requests before they are sent to the database, which would
  otherwise increase the burden on the database, making the
  overload worse.
  """
  @spec start_link(module, opts :: Keyword.t) :: GenServer.on_start
  def start_link(conn_mod, opts) do
    case child_spec(conn_mod, opts) do
      {_, {m, f, args}, _, _, _, _} -> apply(m, f, args)
      %{start: {m, f, args}} -> apply(m, f, args)
    end
  end

  @doc """
  Creates a supervisor child specification for a pool of connections.

  See `start_link/2` for options.
  """
  @spec child_spec(module, opts :: Keyword.t) :: :supervisor.child_spec()
  def child_spec(conn_mod, opts) do
    pool = Keyword.get(opts, :pool, DBConnection.ConnectionPool)
    pool.child_spec({conn_mod, opts})
  end

  @doc """
  Prepare a query with a database connection for later execution.

  It returns `{:ok, query}` on success or `{:error, exception}` if there was
  an error.

  The returned `query` can then be passed to `execute/4` and/or `close/3`

  ### Options

    * `:queue` - Whether to block waiting in an internal queue for the
    connection's state (boolean, default: `true`). See "Queue config" in
    `start_link/2` docs
    * `:timeout` - The maximum time that the caller is allowed to perform
    this operation (default: `15_000`)
    * `:deadline` - If set, overrides `:timeout` option and specifies absolute
    monotonic time in milliseconds by which caller must perform operation.
    See `System` module documentation for more information on monotonic time
    (default: `nil`)
    * `:log` - A function to log information about a call, either
    a 1-arity fun, `{module, function, args}` with `t:DBConnection.LogEntry.t/0`
    prepended to `args` or `nil`. See `DBConnection.LogEntry` (default: `nil`)

  The pool and connection module may support other options. All options
  are passed to `c:handle_prepare/3`.

  ### Example

      DBConnection.transaction(pool, fn conn ->
        query = %Query{statement: "SELECT * FROM table"}
        query = DBConnection.prepare!(conn, query)
        try do
          DBConnection.execute!(conn, query, [])
        after
          DBConnection.close(conn, query)
        end
      end)

  """
  @spec prepare(conn, query, opts :: Keyword.t) ::
    {:ok, query} | {:error, Exception.t}
  def prepare(conn, query, opts \\ []) do
    meter = meter(opts)
    result =
      with {:ok, query, meter} <- parse(query, meter, opts) do
        run(conn, &run_prepare/4, query, meter, opts)
      end
    log(result, :prepare, query, nil)
  end

  @doc """
  Prepare a query with a database connection and return the prepared
  query. An exception is raised on error.

  See `prepare/3`.
  """
  @spec prepare!(conn, query, opts :: Keyword.t) :: query
  def prepare!(conn, query, opts \\ []) do
    case prepare(conn, query, opts) do
      {:ok, result} -> result
      {:error, err} -> raise err
    end
  end

  @doc """
  Prepare a query and execute it with a database connection and return both the
  prepared query and the result, `{:ok, query, result}` on success or
  `{:error, exception}` if there was an error.

  The returned `query` can be passed to `execute/4` and `close/3`.

  ### Options

    * `:queue` - Whether to block waiting in an internal queue for the
    connection's state (boolean, default: `true`). See "Queue config" in
    `start_link/2` docs
    * `:timeout` - The maximum time that the caller is allowed to perform
    this operation (default: `15_000`)
    * `:deadline` - If set, overrides `:timeout` option and specifies absolute
    monotonic time in milliseconds by which caller must perform operation.
    See `System` module documentation for more information on monotonic time
    (default: `nil`)
    * `:log` - A function to log information about a call, either
    a 1-arity fun, `{module, function, args}` with `t:DBConnection.LogEntry.t/0`
    prepended to `args` or `nil`. See `DBConnection.LogEntry` (default: `nil`)

  ### Example

      query                = %Query{statement: "SELECT id FROM table WHERE id=$1"}
      {:ok, query, result} = DBConnection.prepare_execute(conn, query, [1])
      {:ok, result2}       = DBConnection.execute(conn, query, [2])
      :ok                  = DBConnection.close(conn, query)
  """
  @spec prepare_execute(conn, query, params, Keyword.t) ::
    {:ok, query, result} |
    {:error, Exception.t}
  def prepare_execute(conn, query, params, opts \\ []) do
    result =
      with {:ok, query, meter} <- parse(query, meter(opts), opts) do
        parsed_prepare_execute(conn, query, params, meter, opts)
      end

    log(result, :prepare_execute, query, params)
  end

  defp parsed_prepare_execute(conn, query, params, meter, opts) do
    with {:ok, query, result, meter} <- run(conn, &run_prepare_execute/5, query, params, meter, opts),
         {:ok, result, meter} <- decode(query, result, meter, opts) do
      {:ok, query, result, meter}
    end
  end

  @doc """
  Prepare a query and execute it with a database connection and return both the
  prepared query and result. An exception is raised on error.

  See `prepare_execute/4`.
  """
  @spec prepare_execute!(conn, query, Keyword.t) :: {query, result}
  def prepare_execute!(conn, query, params, opts \\ []) do
    case prepare_execute(conn, query, params, opts) do
      {:ok, query, result} -> {query, result}
      {:error, err}        -> raise err
    end
  end

  @doc """
  Execute a prepared query with a database connection and return
  `{:ok, query, result}` on success or `{:error, exception}` if there was an error.

  If the query is not prepared on the connection an attempt may be made to
  prepare it and then execute again.

  ### Options

    * `:queue` - Whether to block waiting in an internal queue for the
    connection's state (boolean, default: `true`). See "Queue config" in
    `start_link/2` docs
    * `:timeout` - The maximum time that the caller is allowed to perform
    this operation (default: `15_000`)
    * `:deadline` - If set, overrides `:timeout` option and specifies absolute
    monotonic time in milliseconds by which caller must perform operation.
    See `System` module documentation for more information on monotonic time
    (default: `nil`)
    * `:log` - A function to log information about a call, either
    a 1-arity fun, `{module, function, args}` with `t:DBConnection.LogEntry.t/0`
    prepended to `args` or `nil`. See `DBConnection.LogEntry` (default: `nil`)

  The pool and connection module may support other options. All options
  are passed to `handle_execute/4`.

  See `prepare/3`.
  """
  @spec execute(conn, query, params, opts :: Keyword.t) ::
    {:ok, query, result} | {:error, Exception.t}
  def execute(conn, query, params, opts \\ []) do
    result =
      case maybe_encode(query, params, meter(opts), opts) do
        {:prepare, meter} ->
          parsed_prepare_execute(conn, query, params, meter, opts)

        {:ok, params, meter} ->
          with {:ok, query, result, meter} <- run(conn, &run_execute/5, query, params, meter, opts),
               {:ok, result, meter} <- decode(query, result, meter, opts) do
            {:ok, query, result, meter}
          end

        {_, _, _, _} = error ->
          error
      end

    log(result, :execute, query, params)
  end

  @doc """
  Execute a prepared query with a database connection and return the
  result. Raises an exception on error.

  See `execute/4`
  """
  @spec execute!(conn, query, params, opts :: Keyword.t) :: result
  def execute!(conn, query, params, opts \\ []) do
    case execute(conn, query, params, opts) do
      {:ok, _query, result} -> result
      {:error, err} -> raise err
    end
  end

  @doc """
  Close a prepared query on a database connection and return `{:ok, result}` on
  success or `{:error, exception}` on error.

  This function should be used to free resources held by the connection
  process and/or the database server.

  ## Options

    * `:queue` - Whether to block waiting in an internal queue for the
    connection's state (boolean, default: `true`). See "Queue config" in
    `start_link/2` docs
    * `:timeout` - The maximum time that the caller is allowed to perform
    this operation (default: `15_000`)
    * `:deadline` - If set, overrides `:timeout` option and specifies absolute
    monotonic time in milliseconds by which caller must perform operation.
    See `System` module documentation for more information on monotonic time
    (default: `nil`)
    * `:log` - A function to log information about a call, either
    a 1-arity fun, `{module, function, args}` with `t:DBConnection.LogEntry.t/0`
    prepended to `args` or `nil`. See `DBConnection.LogEntry` (default: `nil`)

  The pool and connection module may support other options. All options
  are passed to `c:handle_close/3`.

  See `prepare/3`.
  """
  @spec close(conn, query, opts :: Keyword.t) ::
    {:ok, result} | {:error, Exception.t}
  def close(conn, query, opts \\ []) do
    conn
    |> run_cleanup(&run_close/4, [query], meter(opts), opts)
    |> log(:close, query, nil)
  end

  @doc """
  Close a prepared query on a database connection and return the result. Raises
  an exception on error.

  See `close/3`.
  """
  @spec close!(conn, query, opts :: Keyword.t) :: result
  def close!(conn, query, opts \\ []) do
    case close(conn, query, opts) do
      {:ok, result} -> result
      {:error, err} -> raise err
    end
  end

  @doc """
  Acquire a lock on a connection and run a series of requests on it.

  The return value of this function is the return value of `fun`.

  To use the locked connection call the request with the connection
  reference passed as the single argument to the `fun`. If the
  connection disconnects all future calls using that connection
  reference will fail.

  `run/3` and `transaction/3` can be nested multiple times but a
  `transaction/3` call inside another `transaction/3` will be treated
  the same as `run/3`.

  ### Options

    * `:queue` - Whether to block waiting in an internal queue for the
    connection's state (boolean, default: `true`). See "Queue config" in
    `start_link/2` docs
    * `:timeout` - The maximum time that the caller is allowed to perform
    this operation (default: `15_000`)
    * `:deadline` - If set, overrides `:timeout` option and specifies absolute
    monotonic time in milliseconds by which caller must perform operation.
    See `System` module documentation for more information on monotonic time
    (default: `nil`)

  The pool may support other options.

  ### Example

      {:ok, res} = DBConnection.run(conn, fn conn ->
        DBConnection.execute!(conn, query, [])
      end)

  """
  @spec run(conn, (t -> result), opts :: Keyword.t) :: result when result: var
  def run(conn, fun, opts \\ [])
  def run(%DBConnection{} = conn, fun, _) do
    fun.(conn)
  end
  def run(pool, fun, opts) do
    case checkout(pool, nil, opts) do
      {:ok, conn, _} ->
        old_status = status(conn, opts)

        try do
          result = fun.(conn)
          {result, run(conn, &run_status/3, nil, opts)}
        catch
          kind, error ->
            stacktrace = System.stacktrace()
            checkin(conn)
            :erlang.raise(kind, error, stacktrace)
        else
          {result, {:error, _, _}} ->
            checkin(conn)
            result

          {result, {^old_status, _meter}} ->
            checkin(conn)
            result

          {_result, {new_status, _meter}} ->
            err = DBConnection.ConnectionError.exception(
              "connection was checked out with status #{inspect(old_status)} " <>
                "but it was checked in with status #{inspect(new_status)}"
            )
            disconnect(conn, err)
            raise err

          {_result, {kind, reason, stack, _meter}} ->
            :erlang.raise(kind, reason, stack)
        end

      {:error, err, _} ->
        raise err

      {kind, reason, stack, _} ->
        :erlang.raise(kind, reason, stack)
    end
  end

  @doc """
  Acquire a lock on a connection and run a series of requests inside a
  transaction. The result of the transaction fun is return inside an `:ok`
  tuple: `{:ok, result}`.

  To use the locked connection call the request with the connection
  reference passed as the single argument to the `fun`. If the
  connection disconnects all future calls using that connection
  reference will fail.

  `run/3` and `transaction/3` can be nested multiple times. If a transaction is
  rolled back or a nested transaction `fun` raises the transaction is marked as
  failed. All calls except `run/3`, `transaction/3`, `rollback/2`, `close/3` and
  `close!/3` will raise an exception inside a failed transaction until the outer
  transaction call returns. All `transaction/3` calls will return
  `{:error, :rollback}` if the transaction failed or connection closed and
  `rollback/2` is not called for that `transaction/3`.

  ### Options

    * `:queue` - Whether to block waiting in an internal queue for the
    connection's state (boolean, default: `true`). See "Queue config" in
    `start_link/2` docs
    * `:timeout` - The maximum time that the caller is allowed to perform
    this operation (default: `15_000`)
    * `:deadline` - If set, overrides `:timeout` option and specifies absolute
    monotonic time in milliseconds by which caller must perform operation.
    See `System` module documentation for more information on monotonic time
    (default: `nil`)
    * `:log` - A function to log information about begin, commit and rollback
    calls made as part of the transaction, either a 1-arity fun,
    `{module, function, args}` with `t:DBConnection.LogEntry.t/0` prepended to
    `args` or `nil`. See `DBConnection.LogEntry` (default: `nil`)

  The pool and connection module may support other options. All options
  are passed to `c:handle_begin/2`, `c:handle_commit/2` and
  `c:handle_rollback/2`.

  ### Example

      {:ok, res} = DBConnection.transaction(conn, fn conn ->
        DBConnection.execute!(conn, query, [])
      end)
  """
  @spec transaction(conn, (t -> result), opts :: Keyword.t) ::
    {:ok, result} | {:error, reason :: any} when result: var
  def transaction(conn, fun, opts \\ [])

  def transaction(%DBConnection{conn_mode: :transaction} = conn, fun, _opts) do
    %DBConnection{conn_ref: conn_ref} = conn
    try do
      result = fun.(conn)
      conclude(conn, result)
    catch
      :throw, {__MODULE__, ^conn_ref, reason} ->
        fail(conn)
        {:error, reason}
      kind, reason ->
        stack = System.stacktrace()
        fail(conn)
        :erlang.raise(kind, reason, stack)
    else
      result ->
        {:ok, result}
    end
  end
  def transaction(%DBConnection{} = conn, fun, opts) do
    case begin(conn, &run/4, opts) do
      {:ok, _} ->
        run_transaction(conn, fun, &run/4, opts)
      {:error, %DBConnection.TransactionError{}} ->
        {:error, :rollback}
      {:error, err} ->
        raise err
    end
  end
  def transaction(pool, fun, opts) do
    case begin(pool, &checkout/4, opts) do
      {:ok, conn, _} ->
        run_transaction(conn, fun, &checkin/4, opts)
      {:error, %DBConnection.TransactionError{}} ->
        {:error, :rollback}
      {:error, err} ->
        raise err
    end
  end

  @doc """
  Rollback a database transaction and release lock on connection.

  When inside of a `transaction/3` call does a non-local return, using a
  `throw/1` to cause the transaction to enter a failed state and the
  `transaction/3` call returns `{:error, reason}`. If `transaction/3` calls are
  nested the connection is marked as failed until the outermost transaction call
  does the database rollback.

  ### Example

      {:error, :oops} = DBConnection.transaction(pool, fun(conn) ->
        DBConnection.rollback(conn, :oops)
      end)
  """
  @spec rollback(t, reason :: any) :: no_return
  def rollback(conn, reason)

  def rollback(%DBConnection{conn_mode: :transaction} = conn, reason) do
    %DBConnection{conn_ref: conn_ref}  = conn
    throw({__MODULE__, conn_ref, reason})
  end

  def rollback(%DBConnection{} = _conn, _reason) do
    raise "not inside transaction"
  end

  @doc """
  Return the transaction status of a connection.

  The callback implementation should return the transaction status according to
  the database, and not make assumptions based on client-side state.

  This function will raise a `DBConnection.ConnectionError` when called inside a
  deprecated `transaction/3`.

  ### Options

  See module documentation. The pool and connection module may support other
  options. All options are passed to `c:handle_status/2`.

  ### Example

      # outside of the transaction, the status is `:idle`
      DBConnection.status(conn) #=> :idle

      DBConnection.transaction(conn, fn conn ->
        DBConnection.status(conn) #=> :transaction

        # run a query that will cause the transaction to rollback, e.g.
        # uniqueness constraint violation
        DBConnection.execute(conn, bad_query, [])

        DBConnection.status(conn) #=> :error
      end)

      DBConnection.status(conn) #=> :idle
  """
  @spec status(conn, opts :: Keyword.t) :: status
  def status(conn, opts \\ []) do
    case run(conn, &run_status/3, nil, opts) do
      {status, _meter} ->
        status
      {:error, _err, _meter} ->
        :error
      {kind, reason, stack, _meter} ->
        :erlang.raise(kind, reason, stack)
    end
  end

  @doc """
  Create a stream that will prepare a query, execute it and stream results
  using a cursor.

  ### Options

    * `:queue` - Whether to block waiting in an internal queue for the
    connection's state (boolean, default: `true`). See "Queue config" in
    `start_link/2` docs
    * `:timeout` - The maximum time that the caller is allowed to perform
    this operation (default: `15_000`)
    * `:deadline` - If set, overrides `:timeout` option and specifies absolute
    monotonic time in milliseconds by which caller must perform operation.
    See `System` module documentation for more information on monotonic time
    (default: `nil`)
    * `:log` - A function to log information about a call, either
    a 1-arity fun, `{module, function, args}` with `t:DBConnection.LogEntry.t/0`
    prepended to `args` or `nil`. See `DBConnection.LogEntry` (default: `nil`)

  The pool and connection module may support other options. All options
  are passed to `c:handle_prepare/3`, `c:handle_close/3`, `c:handle_declare/4`,
  and `c:handle_deallocate/4`.

  ### Example

      {:ok, results} = DBConnection.transaction(conn, fn conn ->
        query = %Query{statement: "SELECT id FROM table"}
        stream = DBConnection.prepare_stream(conn, query, [])
        Enum.to_list(stream)
      end)
  """
  @spec prepare_stream(t, query, params, opts :: Keyword.t) ::
    DBConnection.PrepareStream.t
  def prepare_stream(%DBConnection{} = conn, query, params, opts \\ []) do
    %DBConnection.PrepareStream{conn: conn, query: query, params: params,
                                opts: opts}
  end

  @doc """
  Create a stream that will execute a prepared query and stream results using a
  cursor.

  ### Options

    * `:queue` - Whether to block waiting in an internal queue for the
    connection's state (boolean, default: `true`). See "Queue config" in
    `start_link/2` docs
    * `:timeout` - The maximum time that the caller is allowed to perform
    this operation (default: `15_000`)
    * `:deadline` - If set, overrides `:timeout` option and specifies absolute
    monotonic time in milliseconds by which caller must perform operation.
    See `System` module documentation for more information on monotonic time
    (default: `nil`)
    * `:log` - A function to log information about a call, either
    a 1-arity fun, `{module, function, args}` with `t:DBConnection.LogEntry.t/0`
    prepended to `args` or `nil`. See `DBConnection.LogEntry` (default: `nil`)

  The pool and connection module may support other options. All options
  are passed to `c:handle_declare/4` and `c:handle_deallocate/4`.

  ### Example

      DBConnection.transaction(pool, fn conn ->
        query = %Query{statement: "SELECT id FROM table"}
        query = DBConnection.prepare!(conn, query)
        try do
          stream = DBConnection.stream(conn, query, [])
          Enum.to_list(stream)
        after
          # Make sure query is closed!
          DBConnection.close(conn, query)
        end
      end)
  """
  @spec stream(t, query, params, opts :: Keyword.t) :: DBConnection.Stream.t
  def stream(%DBConnection{} = conn, query, params, opts \\ []) do
    %DBConnection.Stream{conn: conn, query: query, params: params, opts: opts}
  end

  @doc """
  Reduces a previously built stream or prepared stream.
  """
  def reduce(%DBConnection.PrepareStream{} = stream, acc, fun) do
    %DBConnection.PrepareStream{conn: conn, query: query, params: params,
                                opts: opts} = stream
    declare =
      fn(conn, opts) ->
        {query, cursor} = prepare_declare!(conn, query, params, opts)
        {:cont, query, cursor}
      end
    enum = resource(conn, declare, &stream_fetch/3, &stream_deallocate/3, opts)
    enum.(acc, fun)
  end
  def reduce(%DBConnection.Stream{} = stream, acc, fun) do
    %DBConnection.Stream{conn: conn, query: query, params: params,
                         opts: opts} = stream
    declare =
      fn(conn, opts) ->
        case declare(conn, query, params, opts) do
          {:ok, query, cursor} ->
            {:cont, query, cursor}
          {:ok, cursor} ->
            {:cont, query, cursor}
          {:error, err} ->
            raise err
        end
      end
    enum = resource(conn, declare, &stream_fetch/3, &stream_deallocate/3, opts)
    enum.(acc, fun)
  end

  ## Helpers

  defp checkout(pool, meter, opts) do
    checkout = System.monotonic_time()

    # The holder is only used internally by DBConnection.Task
    holder = Keyword.get(opts, :holder, DBConnection.Holder)

    try do
      holder.checkout(pool, opts)
    catch
      kind, reason ->
        stack = System.stacktrace()
        {kind, reason, stack, past_event(meter, :checkout, checkout)}
    else
      {:ok, pool_ref, _conn_mod, checkin, _conn_state} ->
        conn = %DBConnection{pool_ref: pool_ref, conn_ref: make_ref()}
        meter = meter |> past_event(:checkin, checkin) |> past_event(:checkout, checkout)
        {:ok, conn, meter}
      {:error, err} ->
        {:error, err, past_event(meter, :checkout, checkout)}
    end
  end

  defp checkout(%DBConnection{} = conn, fun, meter, opts) do
    with {:ok, result, meter} <- fun.(conn, meter, opts) do
      {:ok, conn, result, meter}
    end
  end
  defp checkout(pool, fun, meter, opts) do
    with {:ok, conn, meter} <- checkout(pool, meter, opts) do
      case fun.(conn, meter, opts) do
        {:ok, result, meter} ->
          {:ok, conn, result, meter}
        error ->
          checkin(conn)
          error
      end
    end
  end

  defp checkin(%DBConnection{pool_ref: pool_ref}) do
    Holder.checkin(pool_ref)
  end

  defp checkin(%DBConnection{} = conn, fun, meter, opts) do
    return = fun.(conn, meter, opts)
    checkin(conn)
    return
  end
  defp checkin(pool, fun, meter, opts) do
    run(pool, fun, meter, opts)
  end

  defp disconnect(%DBConnection{pool_ref: pool_ref}, err) do
    _ = Holder.disconnect(pool_ref, err)
    :ok
  end

  defp stop(%DBConnection{pool_ref: pool_ref}, kind, reason, stack) do
    msg = "client #{inspect self()} stopped: " <> Exception.format(kind, reason, stack)
    exception = DBConnection.ConnectionError.exception(msg)
    _ = Holder.stop(pool_ref, exception)
    :ok
  end

  defp handle_common_result(return, conn, meter) do
    case return do
      {:ok, result, _conn_state} ->
        {:ok, result, meter}

      {:error, err, _conn_state} ->
        {:error, err, meter}

      {:disconnect, err, _conn_state} ->
        disconnect(conn, err)
        {:error, err, meter}

      {:catch, kind, reason, stack} ->
        stop(conn, kind, reason, stack)
        {kind, reason, stack, meter}

      other ->
        bad_return!(other, conn, meter)
    end
  end

  @compile {:inline, bad_return!: 3}

  defp bad_return!(other, conn, meter) do
    try do
      raise DBConnection.ConnectionError, "bad return value: #{inspect other}"
    catch
      :error, reason ->
        stack = System.stacktrace()
        stop(conn, :error, reason, stack)
        {:error, reason, stack, meter}
    end
  end

  defp parse(query, meter, opts) do
    try do
      DBConnection.Query.parse(query, opts)
    catch
      kind, reason ->
        stack = System.stacktrace()
        {kind, reason, stack, meter}
    else
      query ->
        {:ok, query, meter}
    end
  end

  defp describe(conn, query, meter, opts) do
    try do
      DBConnection.Query.describe(query, opts)
    catch
      kind, reason ->
        stack = System.stacktrace()
        raised_close(conn, query, meter, opts, kind, reason, stack)
    else
      query ->
        {:ok, query, meter}
    end
  end

  defp encode(conn, query, params, meter, opts) do
    try do
      DBConnection.Query.encode(query, params, opts)
    catch
      kind, reason ->
        stack = System.stacktrace()
        raised_close(conn, query, meter, opts, kind, reason, stack)
    else
      params ->
        {:ok, params, meter}
    end
  end

  defp maybe_encode(query, params, meter, opts) do
    try do
      DBConnection.Query.encode(query, params, opts)
    rescue
      DBConnection.EncodeError -> {:prepare, meter}
    catch
      kind, reason ->
        stack = System.stacktrace()
        {kind, reason, stack, meter}
    else
      params ->
        {:ok, params, meter}
    end
  end

  defp decode(query, result, meter, opts) do
    meter = event(meter, :decode)
    try do
      DBConnection.Query.decode(query, result, opts)
    catch
      kind, reason ->
        stack = System.stacktrace()
        {kind, reason, stack, meter}
    else
      result ->
        {:ok, result, meter}
    end
  end

  defp prepare_declare(conn, query, params, opts) do
    result =
      with {:ok, query, meter} <- parse(query, meter(opts), opts) do
        parsed_prepare_declare(conn, query, params, meter, opts)
      end

    log(result, :prepare_declare, query, params)
  end

  defp parsed_prepare_declare(conn, query, params, meter, opts) do
    run(conn, &run_prepare_declare/5, query, params, meter, opts)
  end

  defp prepare_declare!(conn, query, params, opts) do
    case prepare_declare(conn, query, params, opts) do
      {:ok, query, cursor} ->
        {query, cursor}
      {:error, err} ->
        raise err
    end
  end

  defp declare(conn, query, params, opts) do
    result =
      case maybe_encode(query, params, meter(opts), opts) do
        {:prepare, meter} ->
          parsed_prepare_declare(conn, query, params, meter, opts)

        {:ok, params, meter} ->
          run(conn, &run_declare/5, query, params, meter, opts)

        {_, _, _, _} = error ->
          error
      end

    log(result, :declare, query, params)
  end

  defp deallocate(conn, query, cursor, opts) do
    conn
    |> run_cleanup(&run_deallocate/4, [query, cursor], meter(opts), opts)
    |> log(:deallocate, query, cursor)
  end

  defp run_prepare(conn, query, meter, opts) do
    with {:ok, query, meter} <- prepare(conn, query, meter, opts) do
      describe(conn, query, meter, opts)
    end
  end

  defp prepare(%DBConnection{pool_ref: pool_ref} = conn, query, meter, opts) do
    pool_ref
    |> Holder.handle(:handle_prepare, [query], opts)
    |> handle_common_result(conn, event(meter, :prepare))
  end

  defp run_prepare_execute(conn, query, params, meter, opts) do
    with {:ok, query, meter} <- run_prepare(conn, query, meter, opts),
         {:ok, params, meter} <- encode(conn, query, params, meter, opts) do
      run_execute(conn, query, params, meter, opts)
    end
  end

  defp run_execute(conn, query, params, meter, opts) do
    %DBConnection{pool_ref: pool_ref} = conn
    meter = event(meter, :execute)

    case Holder.handle(pool_ref, :handle_execute, [query, params], opts) do
      {:ok, query, result, _conn_state} ->
        {:ok, query, result, meter}

      {:ok, _, _} = other ->
        bad_return!(other, conn, meter)

      other ->
        handle_common_result(other, conn, meter)
    end
  end

  defp raised_close(conn, query, meter, opts, kind, reason, stack) do
    with {:ok, _, meter} <- run_close(conn, [query], meter, opts) do
      {kind, reason, stack, meter}
    end
  end

  defp run_close(conn, args, meter, opts) do
    meter = event(meter, :close)
    cleanup(conn, :handle_close, args, meter, opts)
  end

  defp run_cleanup(%DBConnection{} = conn, fun, args, meter, opts) do
    fun.(conn, args, meter, opts)
  end
  defp run_cleanup(pool, fun, args, meter, opts) do
    with {:ok, conn, meter} <- checkout(pool, meter, opts) do
      try do
        fun.(conn, args, meter, opts)
      after
        checkin(conn)
      end
    end
  end

  defp cleanup(conn, fun, args, meter, opts) do
    %DBConnection{pool_ref: pool_ref} = conn

    case Holder.cleanup(pool_ref, fun, args, opts) do
      {:ok, result, _conn_state} ->
        {:ok, result, meter}
      {:error, err, _conn_state} ->
        {:error, err, meter}
      {:disconnect, err, _conn_state} ->
        disconnect(conn, err)
        {:error, err, meter}
      {:catch, kind, reason, stack} ->
        stop(conn, kind, reason, stack)
        {kind, reason, stack, meter}
      other ->
        bad_return!(other, conn, meter)
    end
  end

  defp run(%DBConnection{} = conn, fun, meter, opts) do
    fun.(conn, meter, opts)
  end
  defp run(pool, fun, meter, opts) do
    with {:ok, conn, meter} <- checkout(pool, meter, opts) do
      try do
        fun.(conn, meter, opts)
      after
        checkin(conn)
      end
    end
  end

  defp run(%DBConnection{} = conn, fun, arg, meter, opts) do
    fun.(conn, arg, meter, opts)
  end
  defp run(pool, fun, arg, meter, opts) do
    with {:ok, conn, meter} <- checkout(pool, meter, opts) do
      try do
        fun.(conn, arg, meter, opts)
      after
        checkin(conn)
      end
    end
  end

  defp run(%DBConnection{} = conn, fun, arg1, arg2, meter, opts) do
    fun.(conn, arg1, arg2, meter, opts)
  end
  defp run(pool, fun, arg1, arg2, meter, opts) do
    with {:ok, conn, meter} <- checkout(pool, meter, opts) do
      try do
        fun.(conn, arg1, arg2, meter, opts)
      after
        checkin(conn)
      end
    end
  end

  defp meter(opts) do
    case Keyword.get(opts, :log) do
      nil -> nil
      log -> {log, []}
    end
  end

  defp event(nil, _),
    do: nil
  defp event({log, events}, event),
    do: {log, [{event, System.monotonic_time()} | events]}

  defp past_event(nil, _, _),
    do: nil
  defp past_event(log_events, _, nil),
    do: log_events
  defp past_event({log, events}, event, time),
    do: {log, [{event, time} | events]}

  defp log({:ok, res, meter}, call, query, params),
    do: log(meter, call, query, params, {:ok, res})
  defp log({:ok, res1, res2, meter}, call, query, params),
    do: log(meter, call, query, params, {:ok, res1, res2})
  defp log({ok, res, meter}, call, query, cursor) when ok in [:cont, :halt],
    do: log(meter, call, query, cursor, {ok, res})
  defp log({:error, err, meter}, call, query, params),
    do: log(meter, call, query, params, {:error, err})
  defp log({kind, reason, stack, meter}, call, query, params),
    do: log(meter, call, query, params, {kind, reason, stack})

  defp log(nil, _, _, _, result),
    do: log_result(result)
  defp log({log, times}, call, query, params, result) do
    entry = DBConnection.LogEntry.new(call, query, params, times, entry_result(result))
    try do
      log(log, entry)
    catch
      kind, reason ->
        stack = System.stacktrace()
        log_raised(entry, kind, reason, stack)
    end
    log_result(result)
  end

  defp entry_result({kind, reason, stack})
  when kind in [:error, :exit, :throw] do
    msg = "an exception was raised: " <> Exception.format(kind, reason, stack)
    {:error, %DBConnection.ConnectionError{message: msg}}
  end
  defp entry_result({ok, res}) when ok in [:cont, :halt],
    do: {:ok, res}
  defp entry_result(other), do: other

  defp log({mod, fun, args}, entry), do: apply(mod, fun, [entry | args])
  defp log(fun, entry), do: fun.(entry)

  defp log_result({kind, reason, stack}) when kind in [:error, :exit, :throw] do
    :erlang.raise(kind, reason, stack)
  end
  defp log_result(other), do: other

  defp log_raised(entry, kind, reason, stack) do
    reason = Exception.normalize(kind, reason, stack)

    Logger.error(fn() ->
      "an exception was raised logging #{inspect entry}: " <> Exception.format(kind, reason, stack)
    end, crash_reason: {crash_reason(kind, reason), stack})
  catch
    _, _ ->
      :ok
  end

  defp crash_reason(:throw, value), do: {:nocatch, value}
  defp crash_reason(_, value), do: value

  defp run_transaction(conn, fun, run, opts) do
    %DBConnection{conn_ref: conn_ref} = conn
    try do
      result = fun.(%{conn | conn_mode: :transaction})
      conclude(conn, result)
    catch
      :throw, {__MODULE__, ^conn_ref, reason} ->
        reset(conn)
        case rollback(conn, run, opts) do
          {:ok, _} ->
            {:error, reason}
          {:error, %DBConnection.TransactionError{}} ->
            {:error, reason}
          {:error, %DBConnection.ConnectionError{}} ->
            {:error, reason}
          {:error, err} ->
            raise err
        end
      kind, reason ->
        stack = System.stacktrace()
        reset(conn)
        _ = rollback(conn, run, opts)
        :erlang.raise(kind, reason, stack)
    else
      result ->
        case commit(conn, run, opts) do
          {:ok, _} ->
            {:ok, result}
          {:error, %DBConnection.TransactionError{}} ->
            {:error, :rollback}
          {:error, err} ->
            raise err
        end
    after
      reset(conn)
    end
  end

  defp fail(%DBConnection{pool_ref: pool_ref}) do
    case Holder.status?(pool_ref, :ok) do
      true -> Holder.put_status(pool_ref, :aborted)
      false -> :ok
    end
  end

  defp conclude(%DBConnection{pool_ref: pool_ref, conn_ref: conn_ref}, result) do
    case Holder.status?(pool_ref, :ok) do
      true -> result
      false -> throw({__MODULE__, conn_ref, :rollback})
    end
  end

  defp reset(%DBConnection{pool_ref: pool_ref}) do
    case Holder.status?(pool_ref, :aborted) do
      true -> Holder.put_status(pool_ref, :ok)
      false -> :ok
    end
  end

  defp begin(conn, run, opts) do
    conn
    |> run.(&run_begin/3, meter(opts), opts)
    |> log(:begin, :begin, nil)
  end

  defp run_begin(conn, meter, opts) do
    %DBConnection{pool_ref: pool_ref} = conn
    meter = event(meter, :begin)

    case Holder.handle(pool_ref, :handle_begin, [], opts) do
      {status, _conn_state} when status in [:idle, :transaction, :error] ->
        status_disconnect(conn, status, meter)

      other ->
        handle_common_result(other, conn, meter)
    end
  end

  defp rollback(conn, run, opts) do
    conn
    |> run.(&run_rollback/3, meter(opts), opts)
    |> log(:rollback, :rollback, nil)
  end

  defp run_rollback(conn, meter, opts) do
    %DBConnection{pool_ref: pool_ref} = conn
    meter = event(meter, :rollback)

    case Holder.handle(pool_ref, :handle_rollback, [], opts) do
      {status, _conn_state} when status in [:idle, :transaction, :error] ->
        status_disconnect(conn, status, meter)

      other ->
        handle_common_result(other, conn, meter)
    end
  end

  defp commit(conn, run, opts) do
    case run.(conn, &run_commit/3, meter(opts), opts) do
      {:rollback, {:ok, result, meter}} ->
        log(meter, :commit, :rollback, nil, {:ok, result})
        err = DBConnection.TransactionError.exception(:error)
        {:error, err}
      {query, other} ->
        log(other, :commit, query, nil)
      {:error, err, meter} ->
        log(meter, :commit, :commit, nil, {:error, err})
      {kind, reason, stack, meter} ->
        log(meter, :commit, :commit, nil, {kind, reason, stack})
    end
  end

  defp run_commit(conn, meter, opts) do
    %DBConnection{pool_ref: pool_ref} = conn
    meter = event(meter, :commit)

    case Holder.handle(pool_ref, :handle_commit, [], opts) do
      {:error, _conn_state} ->
        {:rollback, run_rollback(conn, meter, opts)}

      {status, _conn_state} when status in [:idle, :transaction] ->
        {:commit, status_disconnect(conn, status, meter)}

      other ->
        {:commit, handle_common_result(other, conn, meter)}
    end
  end

  defp status_disconnect(conn, status, meter) do
    err = DBConnection.TransactionError.exception(status)
    disconnect(conn, err)
    {:error, err, meter}
  end

  defp run_status(conn, meter, opts) do
    %DBConnection{pool_ref: pool_ref} = conn

    case Holder.handle(pool_ref, :handle_status, [], opts) do
      {status, _conn_state} when status in [:idle, :transaction, :error] ->
        {status, meter}
      {:disconnect, err, _conn_state} ->
        disconnect(conn, err)
        {:error, err, meter}
      {:catch, kind, reason, stack} ->
        stop(conn, kind, reason, stack)
        {kind, reason, stack, meter}
      other ->
        bad_return!(other, conn, meter)
    end
  end

  defp run_prepare_declare(conn, query, params, meter, opts) do
    with {:ok, query, meter} <- prepare(conn, query, meter, opts),
         {:ok, query, meter} <- describe(conn, query, meter, opts),
         {:ok, params, meter} <- encode(conn, query, params, meter, opts),
         {:ok, query, cursor, meter} <- run_declare(conn, query, params, meter, opts) do
      {:ok, query, cursor, meter}
    end
  end

  defp run_declare(conn, query, params, meter, opts) do
    %DBConnection{pool_ref: pool_ref} = conn
    meter = event(meter, :declare)

    case Holder.handle(pool_ref, :handle_declare, [query, params], opts) do
      {:ok, query, result, _conn_state} ->
        {:ok, query, result, meter}

      {:ok, _, _} = other ->
        bad_return!(other, conn, meter)

      other ->
        handle_common_result(other, conn, meter)
    end
  end

  defp stream_fetch(conn, {:cont, query, cursor}, opts) do
    conn
    |> run(&run_stream_fetch/4, [query, cursor], meter(opts), opts)
    |> log(:fetch, query, cursor)
    |> case do
      {ok, result} when ok in [:cont, :halt] ->
        {[result], {ok, query, cursor}}

      {:error, err} ->
        raise err
    end
  end
  defp stream_fetch(_, {:halt, _,  _} = state, _) do
    {:halt, state}
  end

  defp run_stream_fetch(conn, args, meter, opts) do
    [query, _] = args

    with {ok, result, meter} when ok in [:cont, :halt] <- run_fetch(conn, args, meter, opts),
         {:ok, result, meter} <- decode(query, result, meter, opts) do
      {ok, result, meter}
    end
  end

  defp run_fetch(conn, args, meter, opts) do
    %DBConnection{pool_ref: pool_ref} = conn
    meter = event(meter, :fetch)

    case Holder.handle(pool_ref, :handle_fetch, args, opts) do
      {:cont, result, _conn_state}  ->
        {:cont, result, meter}

      {:halt, result, _conn_state}  ->
        {:halt, result, meter}

      other ->
        handle_common_result(other, conn, meter)
    end
  end

  defp stream_deallocate(conn, {_status, query, cursor}, opts),
    do: deallocate(conn, query, cursor, opts)

  defp run_deallocate(conn, args, meter, opts) do
    meter = event(meter, :deallocate)
    cleanup(conn, :handle_deallocate, args, meter, opts)
  end

  defp resource(%DBConnection{} = conn, start, next, stop, opts) do
    start = fn -> start.(conn, opts) end
    next = fn state -> next.(conn, state, opts) end
    stop = fn state -> stop.(conn, state, opts) end
    Stream.resource(start, next, stop)
  end
end
