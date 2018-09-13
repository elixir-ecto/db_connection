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

  If no requests are received for a period of time the connection will
  trigger an idle timeout and the database can be pinged to keep the
  connection alive.

  Should the connection be lost, attempts will be made to reconnect with
  (configurable) exponential random backoff to reconnect. All state is
  lost when a connection disconnects but the process is reused.

  The `DBConnection.Query` protocol provide utility functions so that
  queries can be prepared or encoded and results decoding without
  blocking the connection or pool.
  """
  require Logger

  defstruct [:holder, :pool_ref, :conn_mod, :conn_ref, :conn_mode]

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
  @type t :: %__MODULE__{holder: module,
                         pool_ref: any,
                         conn_mod: any,
                         conn_ref: reference}
  @type conn :: GenServer.server | t
  @type query :: any
  @type params :: any
  @type result :: any
  @type cursor :: any
  @type status :: :idle | :transaction | :error

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
  another process. `checkin/1` is called with the new state when control
  is returned to the connection process.

  This callback is called in the connection process.
  """
  @callback checkout(state :: any) ::
    {:ok, new_state :: any} | {:disconnect, Exception.t, new_state :: any}

  @doc """
  Checks in the state to the connection process. Return `{:ok, state}`
  to allow the checkin or `{:disconnect, exception, state}` to disconnect.

  This callback is called when the control of the state is passed back
  to the connection process. It should reverse any changes made in
  `checkout/2`.

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
  timeout can be configured by the `:idle_timeout` option. This function
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
    {:error | :disconnect, Exception.t, new_state :: any}

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
    {:error | :disconnect, Exception.t, new_state :: any}

  @doc """
  Handle committing a transaction. Return `{:ok, result, state}` on successfully
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
    {:idle, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @doc """
  Handle getting the transaction status. Return `{:idle, state}` if outside a
  transaction, `{:transaction, state}` if inside a transaction,
  `{:error, state}` if inside an aborted transaction, or
  `{:disconnect, exception, state}` to error and disconnect.

  If the callback returns a `:disconnect` tuples then `status/2` will return
  `:error`.
  """
  @callback handle_status(opts :: Keyword.t, state :: any) ::
    {:idle | :transaction | :error, new_state :: any} |
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
  Execute a query prepared by `handle_prepare/3`. Return
  `{:ok, result, state}` to return the result `result` and continue,
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
  Close a query prepared by `handle_prepare/3` with the database. Return
  `{:ok, result, state}` on success and to continue,
  `{:error, exception, state}` to return an error and continue, or
  `{:disconnect, exception, state}` to return an error and disconnect.

  This callback is called in the client process.
  """
  @callback handle_close(query, opts :: Keyword.t, state :: any) ::
    {:ok, result, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @doc """
  Declare a cursor using a query prepared by `handle_prepare/3`. Return
  `{:ok, cursor, state}` to start a cursor for a stream and continue,
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
  Fetch the next result from a cursor declared by `handle_declare/4`. Return
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
  Deallocate a cursor declared by `handle_declare/4` with the database. Return
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
    a 1-arity fun, `{module, function, args}` with `DBConnection.t` prepended
    to `args` or `nil` (default: `nil`)
    * `:name` - A name to register the started process (see the `:name` option
    in `GenServer.start_link/3`)
    * `:pool` - Chooses the pool to be started
    * `:pool_size` - Chooses the size of the pool

  ### Example

      {:ok, conn} = DBConnection.start_link(mod, [idle_timeout: 5_000])
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

  The returned `query` can then be passed to `execute/3` and/or `close/3`

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
  are passed to `handle_prepare/3`.

  ### Example

      DBConnection.transaction(pool, fn conn ->
        query = DBConnection.prepare!(conn)
        try do
          DBConnection.execute!(conn, "SELECT * FROM table", [])
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
        run(conn, &run_prepare/5, query, meter, opts)
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
    with {:ok, query, result, meter} <- run(conn, &run_prepare_execute/6, query, params, meter, opts),
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
  `{:ok, result}` or `{:ok, query, result}` on success or
  `{:error, exception}` if there was an error.

  If the query is not prepared on the connection an attempt may be made to
  prepare it and then execute again.

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
          with {:ok, query, result, meter} <- run(conn, &run_execute/6, query, params, meter, opts),
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
  are passed to `handle_close/3`.

  See `prepare/3`.
  """
  @spec close(conn, query, opts :: Keyword.t) ::
    {:ok, result} | {:error, Exception.t}
  def close(conn, query, opts \\ []) do
    conn
    |> cleanup(&run_close/6, [query], meter(opts), opts)
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

    * `:pool_timeout` - The maximum time to wait for a reply when making a
    synchronous call to the pool (default: `5_000`)
    * `:queue` - Whether to block waiting in an internal queue for the
    connection's state (boolean, default: `true`)
    * `:timeout` - The maximum time that the caller is allowed the
    to hold the connection's state (default: `15_000`)

  The pool may support other options.

  ### Example

      {:ok, res} = DBConnection.run(conn, fn(conn) ->
        DBConnection.execute!(conn, "SELECT id FROM table", [])
      end)
  """
  @spec run(conn, (t -> result), opts :: Keyword.t) :: result when result: var
  def run(conn, fun, opts \\ [])
  def run(%DBConnection{} = conn, fun, _) do
    case fetch_info(conn, nil) do
      {:ok, _conn_state, _meter} ->
        fun.(conn)
      {:error, err, _meter} ->
        raise err
    end
  end
  def run(pool, fun, opts) do
    case checkout(pool, nil, opts) do
      {:ok, conn, _, _} ->
        try do
          status = status(conn, opts)
          {fun.(conn), status, status(conn, opts)}
        catch
          kind, error ->
            stacktrace = System.stacktrace()
            checkin(conn)
            :erlang.raise(kind, error, stacktrace)
        else
          {result, status, status} ->
            checkin(conn)
            result

          {result, old_status, new_status} ->
            case fetch_info(conn, nil) do
              {:ok, conn_state, _meter} ->
                err = DBConnection.ConnectionError.exception(
                  "connection was checked out with status #{inspect(old_status)} " <>
                    "but it was checked in with status #{inspect(new_status)}"
                )
                delete_disconnect(conn, conn_state, err)
                raise err

              {:error, _, _} ->
                result
            end
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

    * `:pool_timeout` - The maximum time to wait for a reply when making a
    synchronous call to the pool (default: `5_000`)
    * `:queue` - Whether to block waiting in an internal queue for the
    connection's state (boolean, default: `true`)
    * `:timeout` - The maximum time that the caller is allowed the
    to hold the connection's state (default: `15_000`)
    * `:log` - A function to log information about begin, commit and rollback
    calls made as part of the transaction, either a 1-arity fun,
    `{module, function, args}` with `DBConnection.LogEntry.t` prepended to
    `args` or `nil`. See `DBConnection.LogEntry` (default: `nil`)

  The pool and connection module may support other options. All options
  are passed to `handle_begin/2`, `handle_commit/2` and
  `handle_rollback/2`.

  ### Example

      {:ok, res} = DBConnection.transaction(conn, fn(conn) ->
        DBConnection.execute!(conn, "SELECT id FROM table", [])
      end)
  """
  @spec transaction(conn, (conn -> result), opts :: Keyword.t) ::
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
      {:ok, conn, _} ->
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

  def rollback(%DBConnection{} = conn, _reason) do
    case get_info(conn, nil) do
      {:error, err, _meter} ->
        raise err
      {_status, _conn_state, _meter} ->
        raise "not inside transaction"
    end
  end

  @doc """
  Return the transaction status of a connection.

  The callback implementation should return the transaction status according to
  the database, and not make assumption based.

  This function will raise a `DBConnection.ConnectionError` when called inside a
  deprecated `transaction/3`.

  ### Options

  See module documentation. The pool and connection module may support other
  options. All options are passed to `handle_status/2`.

  ### Example

      {:ok, conn, _result} = DBConnection.begin(pool)
      try do
        fun.(conn)
      after
        case DBConnection.status(conn) do
          :transaction ->
            DBConnection.commit(conn)
          :error ->
            DBConnection.rollback(conn)
            raise "transaction is aborted!"
          :idle ->
            raise "transaction is not started!"
        end
      end
  """
  @spec status(conn, opts :: Keyword.t) :: status
  def status(conn, opts \\ []) do
    case run(conn, &run_status/4, nil, opts) do
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
  are passed to `handle_prepare/3, `handle_close/3, `handle_declare/4`,
  and `handle_deallocate/4`.

  ### Example

      {:ok, results} = DBConnection.transaction(conn, fn(conn) ->
        query  = %Query{statement: "SELECT id FROM table"}
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
  are passed to `handle_declare/4` and `handle_deallocate/4`.

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

  @doc false
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
    meter = event(meter, :checkout)

    # The holder is only used internally by DBConnection.Task
    holder = Keyword.get(opts, :holder, DBConnection.Holder)

    try do
      holder.checkout(pool, opts)
    catch
      kind, reason ->
        stack = System.stacktrace()
        {kind, reason, stack, meter}
    else
      {:ok, pool_ref, conn_mod, conn_state} ->
        conn = %DBConnection{
          holder: holder,
          pool_ref: pool_ref,
          conn_mod: conn_mod,
          conn_ref: make_ref()
        }

        put_info(conn, conn_state)
        {:ok, conn, conn_state, meter}
      {:error, err} ->
        {:error, err, meter}
    end
  end

  defp checkout(%DBConnection{} = conn, fun, meter, opts) do
    with {:ok, conn_state, meter} <- fetch_info(conn, meter),
         {:ok, result, meter} <- fun.(conn, conn_state, meter, opts) do
      {:ok, conn, result, meter}
    end
  end
  defp checkout(pool, fun, meter, opts) do
    with {:ok, conn, conn_state, meter} <- checkout(pool, meter, opts) do
      case fun.(conn, conn_state, meter, opts) do
        {:ok, result, meter} ->
          {:ok, conn, result, meter}
        error ->
          checkin(conn)
          error
      end
    end
  end

  defp checkin(conn) do
    case delete_info(conn) do
      {:ok, conn_state} ->
        %DBConnection{holder: holder, pool_ref: pool_ref} = conn
        _ = holder.checkin(pool_ref, conn_state)
        :ok
      {:error, _} = error ->
        error
    end
  end

  defp checkin(%DBConnection{} = conn, fun, meter, opts) do
    with {:ok, conn_state, meter} <- fetch_info(conn, meter) do
      return = fun.(conn, conn_state, meter, opts)
      checkin(conn)
      return
    end
  end
  defp checkin(pool, fun, meter, opts) do
    run(pool, fun, meter, opts)
  end

  defp delete_disconnect(conn, conn_state, err) do
    _ = delete_info(conn)
    %DBConnection{holder: holder, pool_ref: pool_ref} = conn
    _ = holder.disconnect(pool_ref, err, conn_state)
    :ok
  end

  defp delete_stop(conn, conn_state, kind, reason, stack) do
    _ = delete_info(conn)
    msg = "client #{inspect self()} stopped: " <>
      Exception.format(kind, reason, stack)
    exception = DBConnection.ConnectionError.exception(msg)
    %DBConnection{holder: holder, pool_ref: pool_ref} = conn
    _ = holder.stop(pool_ref, exception, conn_state)
    :ok
  end

  defp handle(conn, conn_state, fun, args, opts) do
    %DBConnection{conn_mod: conn_mod} = conn

    try do
      apply(conn_mod, fun, args ++ [opts, conn_state])
    catch
      kind, reason ->
        stack = System.stacktrace()
        delete_stop(conn, conn_state, kind, reason, stack)
        {:catch, kind, reason, stack}
    end
  end

  defp handle_common_result(return, conn, conn_state, meter) do
    case return do
      {:ok, result, conn_state} ->
        put_info(conn, conn_state)
        {:ok, result, meter}

      {:error, err, conn_state} ->
        put_info(conn, conn_state)
        {:error, err, meter}

      {:disconnect, err, conn_state} ->
        delete_disconnect(conn, conn_state, err)
        {:error, err, meter}

      {:catch, kind, reason, stack} ->
        {kind, reason, stack, meter}

      other ->
        bad_return!(other, conn, conn_state, meter)
    end
  end

  defp bad_return!(other, conn, conn_state, meter) do
    try do
      raise DBConnection.ConnectionError, "bad return value: #{inspect other}"
    catch
      :error, reason ->
        stack = System.stacktrace()
        delete_stop(conn, conn_state, :error, reason, stack)
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
    run(conn, &run_prepare_declare/6, query, params, meter, opts)
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
          run(conn, &run_declare/6, query, params, meter, opts)

        {_, _, _, _} = error ->
          error
      end

    log(result, :declare, query, params)
  end

  defp deallocate(conn, query, cursor, opts) do
    conn
    |> cleanup(&run_deallocate/6, [query, cursor], meter(opts), opts)
    |> log(:deallocate, query, cursor)
  end

  defp run_prepare(conn, conn_state, query, meter, opts) do
    with {:ok, query, meter} <- prepare(conn, conn_state, query, meter, opts) do
      describe(conn, query, meter, opts)
    end
  end

  defp prepare(conn, conn_state, query, meter, opts) do
    conn
    |> handle(conn_state, :handle_prepare, [query], opts)
    |> handle_common_result(conn, conn_state, event(meter, :prepare))
  end

  defp run_prepare_execute(conn, conn_state, query, params, meter, opts) do
    with {:ok, query, meter}
          <- run_prepare(conn, conn_state, query, meter, opts),
         {:ok, params, meter} <- encode(conn, query, params, meter, opts),
         {:ok, conn_state, meter} <- fetch_info(conn, meter),
         {:ok, result, meter}
          <- run_execute(conn, conn_state, query, params, meter, opts) do
      # run_execute maybe return {:ok, new_query, result, meter}, replacing the
      # query prepared in run_prepare but it should not happen because just
      # prepared.
      {:ok, query, result, meter}
    end
  end

  defp run_execute(conn, conn_state, query, params, meter, opts) do
    meter = event(meter, :execute)

    case handle(conn, conn_state, :handle_execute, [query, params], opts) do
      {:ok, query, result, conn_state} ->
        put_info(conn, conn_state)
        {:ok, query, result, meter}

      {:ok, _, _} = other ->
        bad_return!(other, conn, conn_state, meter)

      other ->
        handle_common_result(other, conn, conn_state, meter)
    end
  end

  defp raised_close(conn, query, meter, opts, kind, reason, stack) do
    with {status, conn_state, meter} when status in [:ok, :failed]
          <- get_info(conn, meter),
         {:ok, _, meter}
          <- run_close(conn, status, conn_state, [query], meter, opts) do
      {kind, reason, stack, meter}
    end
  end

  defp run_close(conn, status, conn_state, args, meter, opts) do
    meter = event(meter, :close)
    cleanup(conn, status, conn_state, :handle_close, args, meter, opts)
  end

  defp cleanup(%DBConnection{} = conn, fun, args, meter, opts) do
    with {status, conn_state, meter} when status in [:ok, :failed]
          <- get_info(conn, meter) do
      fun.(conn, status, conn_state, args, meter, opts)
    end
  end
  defp cleanup(pool, fun, args, meter, opts) do
    with {:ok, conn, conn_state, meter} <- checkout(pool, meter, opts) do
      try do
        fun.(conn, :ok, conn_state, args, meter, opts)
      after
        checkin(conn)
      end
    end
  end

  defp cleanup(conn, status, conn_state, fun, args, meter, opts) do
    %DBConnection{conn_mod: conn_mod} = conn
    try do
      apply(conn_mod, fun, args ++ [opts, conn_state])
    else
      {:ok, result, conn_state} ->
        put_info(conn, status, conn_state)
        {:ok, result, meter}
      {:error, err, conn_state} ->
        put_info(conn, status, conn_state)
        {:error, err, meter}
      {:disconnect, err, conn_state} ->
        delete_disconnect(conn, conn_state, err)
        {:error, err, meter}
      other ->
        try do
          raise DBConnection.ConnectionError, "bad return value: #{inspect other}"
        catch
          :error, reason ->
            stack = System.stacktrace()
            delete_stop(conn, conn_state, :error, reason, stack)
            {:error, reason, stack, meter}
        end
    catch
      kind, reason ->
        stack = System.stacktrace()
        delete_stop(conn, conn_state, kind, reason, stack)
        {kind, reason, stack, meter}
    end
  end

  defp run(%DBConnection{} = conn, fun, meter, opts) do
    with {:ok, conn_state, meter} <- fetch_info(conn, meter) do
      fun.(conn, conn_state, meter, opts)
    end
  end
  defp run(pool, fun, meter, opts) do
    with {:ok, conn, conn_state, meter} <- checkout(pool, meter, opts) do
      try do
        fun.(conn, conn_state, meter, opts)
      after
        checkin(conn)
      end
    end
  end

  defp run(%DBConnection{} = conn, fun, arg, meter, opts) do
    with {:ok, conn_state, meter} <- fetch_info(conn, meter) do
      fun.(conn, conn_state, arg, meter, opts)
    end
  end
  defp run(pool, fun, arg, meter, opts) do
    with {:ok, conn, conn_state, meter} <- checkout(pool, meter, opts) do
      try do
        fun.(conn, conn_state, arg, meter, opts)
      after
        checkin(conn)
      end
    end
  end

  defp run(%DBConnection{} = conn, fun, arg1, arg2, meter, opts) do
    with {:ok, conn_state, meter} <- fetch_info(conn, meter) do
      fun.(conn, conn_state, arg1, arg2, meter, opts)
    end
  end
  defp run(pool, fun, arg1, arg2, meter, opts) do
    with {:ok, conn, conn_state, meter} <- checkout(pool, meter, opts) do
      try do
        fun.(conn, conn_state, arg1, arg2, meter, opts)
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
    do: {log, [{event, :erlang.monotonic_time()} | events]}

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
    Logger.error(fn() ->
      "an exception was raised logging #{inspect entry}: " <> Exception.format(kind, reason, stack)
    end)
  catch
    _, _ ->
      :ok
  end

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

  defp fail(conn) do
    case get_info(conn, nil) do
      {:ok, conn_state, _meter} ->
        put_info(conn, :failed, conn_state)
      _ ->
        :ok
    end
  end

  defp conclude(%DBConnection{conn_ref: conn_ref} = conn, result) do
    case get_info(conn, nil) do
      {:ok, _conn_state, _meter} ->
        result
      _ ->
        throw({__MODULE__, conn_ref, :rollback})
    end
  end

  defp reset(conn) do
    case get_info(conn, nil) do
      {:failed, conn_state, _meter} ->
        put_info(conn, :ok, conn_state)
      _ ->
        :ok
    end
  end

  defp begin(conn, run, opts) do
    conn
    |> run.(&run_begin/4, meter(opts), opts)
    |> log(:begin, :begin, nil)
  end

  defp run_begin(conn, conn_state, meter, opts) do
    meter = event(meter, :begin)

    case handle(conn, conn_state, :handle_begin, [], opts) do
      {status, conn_state} when status in [:idle, :transaction, :error] ->
        put_info(conn, conn_state)
        status_disconnect(conn, status, meter)

      other ->
        handle_common_result(other, conn, conn_state, meter)
    end
  end

  defp rollback(conn, run, opts) do
    conn
    |> run.(&run_rollback/4, meter(opts), opts)
    |> log(:rollback, :rollback, nil)
  end

  defp run_rollback(conn, conn_state, meter, opts) do
    meter = event(meter, :rollback)

    case handle(conn, conn_state, :handle_rollback, [], opts) do
      {status, conn_state} when status in [:idle, :transaction, :error] ->
        put_info(conn, conn_state)
        status_disconnect(conn, status, meter)

      other ->
        handle_common_result(other, conn, conn_state, meter)
    end
  end

  defp commit(conn, run, opts) do
    case run.(conn, &run_commit/4, meter(opts), opts) do
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

  defp run_commit(conn, conn_state, meter, opts) do
    meter = event(meter, :commit)

    case handle(conn, conn_state, :handle_commit, [], opts) do
      {:error, conn_state} ->
        {:rollback, run_rollback(conn, conn_state, meter, opts)}

      {status, conn_state} when status in [:idle, :transaction] ->
        put_info(conn, conn_state)
        {:commit, status_disconnect(conn, status, meter)}

      other ->
        {:commit, handle_common_result(other, conn, conn_state, meter)}
    end
  end

  defp status_disconnect(conn, status, meter) do
    # conn_state must valid as just put there in previous call
    {:ok, conn_state, meter} = fetch_info(conn, meter)
    err = DBConnection.TransactionError.exception(status)
    delete_disconnect(conn, conn_state, err)
    {:error, err, meter}
  end

  defp run_status(conn, conn_state, meter, opts) do
    %DBConnection{conn_mod: conn_mod} = conn
    try do
      apply(conn_mod, :handle_status, [opts, conn_state])
    else
      {status, conn_state} when status in [:idle, :transaction, :error] ->
        put_info(conn, conn_state)
        {status, conn_state}
      {:disconnect, err, conn_state} ->
        delete_disconnect(conn, conn_state, err)
        {:error, meter}
      other ->
        try do
          raise DBConnection.ConnectionError, "bad return value: #{inspect other}"
        catch
          :error, reason ->
            stack = System.stacktrace()
            delete_stop(conn, conn_state, :error, reason, stack)
            {:error, reason, stack, meter}
        end
    catch
      kind, reason ->
        stack = System.stacktrace()
        delete_stop(conn, conn_state, kind, reason, stack)
        {kind, reason, stack, meter}
    end
  end

  defp run_prepare_declare(conn, conn_info, query, params, meter, opts) do
    with {:ok, query, meter} <- prepare(conn, conn_info, query, meter, opts),
         {:ok, query, meter} <- describe(conn, query, meter, opts),
         {:ok, params, meter} <- encode(conn, query, params, meter, opts),
         {:ok, conn_state, meter} <- fetch_info(conn, meter),
         {:ok, query, cursor, meter}
          <- run_declare(conn, conn_state, query, params, meter, opts) do
      {:ok, query, cursor, meter}
    end
  end

  defp run_declare(conn, conn_state, query, params, meter, opts) do
    meter = event(meter, :declare)

    case handle(conn, conn_state, :handle_declare, [query, params], opts) do
      {:ok, query, result, conn_state} ->
        put_info(conn, conn_state)
        {:ok, query, result, meter}

      {:ok, _, _} = other ->
        bad_return!(other, conn, conn_state, meter)

      other ->
        handle_common_result(other, conn, conn_state, meter)
    end
  end

  defp stream_fetch(conn, {:cont, query, cursor}, opts) do
    conn
    |> run(&run_stream_fetch/5, [query, cursor], meter(opts), opts)
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

  defp run_stream_fetch(conn, conn_state, args, meter, opts) do
    [query, _] = args

    with {ok, result, meter} when ok in [:cont, :halt] <-
           run_fetch(conn, conn_state, args, meter, opts),
         {:ok, result, meter} <- decode(query, result, meter, opts) do
      {ok, result, meter}
    end
  end

  defp run_fetch(conn, conn_state, args, meter, opts) do
    meter = event(meter, :fetch)

    case handle(conn, conn_state, :handle_fetch, args, opts) do
      {:cont, result, conn_state}  ->
        put_info(conn, conn_state)
        {:cont, result, meter}

      {:halt, result, conn_state}  ->
        put_info(conn, conn_state)
        {:halt, result, meter}

      other ->
        handle_common_result(other, conn, conn_state, meter)
    end
  end

  defp stream_deallocate(conn, {_status, query, cursor}, opts),
    do: deallocate(conn, query, cursor, opts)

  defp run_deallocate(conn, status, conn_state, args, meter, opts) do
    meter = event(meter, :deallocate)
    cleanup(conn, status, conn_state, :handle_deallocate, args, meter, opts)
  end

  defp resource(%DBConnection{} = conn, start, next, stop, opts) do
    start = fn() -> start.(conn, opts) end
    next = fn(state) -> next.(conn, state, opts) end
    stop = fn(state) -> stop.(conn, state, opts) end
    Stream.resource(start, next, stop)
  end

  defp put_info(conn, status \\ :ok, conn_state) do
    _ = Process.put(key(conn), {status, conn_state})
    :ok
  end

  defp fetch_info(conn, meter) do
    case Process.get(key(conn)) do
      {:ok, conn_state} ->
        {:ok, conn_state, meter}

      {:failed, _conn_state} ->
        msg = "transaction rolling back"
        {:error, DBConnection.ConnectionError.exception(msg), meter}

      nil ->

        msg = "connection is closed"
        {:error, DBConnection.ConnectionError.exception(msg), meter}
    end
  end

  defp get_info(conn, meter) do
    case Process.get(key(conn)) do
      {status, conn_state} ->
        {status, conn_state, meter}

      nil ->
        msg = "connection is closed"
        {:error, DBConnection.ConnectionError.exception(msg), meter}
    end
  end

  defp delete_info(conn) do
    case Process.delete(key(conn)) do
      {:ok, _conn_state} = ok ->
        ok

      {:failed, conn_state} ->
        {:ok, conn_state}

      nil ->
        msg = "connection is closed"
        {:error, DBConnection.ConnectionError.exception(msg)}
    end
  end

  defp key(%DBConnection{conn_ref: conn_ref}), do: {__MODULE__, conn_ref}
end
