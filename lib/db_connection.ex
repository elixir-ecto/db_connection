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

  By default the `DBConnection` provides a single connection. However
  the `:pool` option can be set to use a pool of connections. If a
  pool is used the module must be passed as an option - unless inside a
  `run/3` or `transaction/3` fun and using the run/transaction
  connection reference (`t`).
  """
  require Logger

  defstruct [:pool_mod, :pool_ref, :conn_mod, :conn_ref, :conn_mode]

  @typedoc """
  Run or transaction connection reference.
  """
  @type t :: %__MODULE__{pool_mod: module,
                         pool_ref: any,
                         conn_mod: any,
                         conn_ref: reference}
  @type conn :: GenServer.server | t
  @type query :: any
  @type params :: any
  @type result :: any
  @type cursor :: any

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

  Messages are discarded, instead of being passed to `handle_info/2`,
  when the state is checked out.

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
  Handle the beginning of a transaction. Return `{:ok, result, state}` to
  continue, `{:error, exception, state}` to abort the transaction and
  continue or `{:disconnect, exception, state}` to abort the transaction
  and disconnect.

  This callback is called in the client process.
  """
  @callback handle_begin(opts :: Keyword.t, state :: any) ::
    {:ok, result, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @doc """
  Handle committing a transaction. Return `{:ok, result, state}` on success and
  to continue, `{:error, exception, state}` to abort the transaction and
  continue or `{:disconnect, exception, state}` to abort the transaction
  and disconnect.

  This callback is called in the client process.
  """
  @callback handle_commit(opts :: Keyword.t, state :: any) ::
    {:ok, result, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @doc """
  Handle rolling back a transaction. Return `{:ok, result, state}` on success
  and to continue, `{:error, exception, state}` to abort the transaction
  and continue or `{:disconnect, exception, state}` to abort the
  transaction and disconnect.

  A transaction will be rolled back if an exception occurs or
  `rollback/2` is called.

  This callback is called in the client process.
  """
  @callback handle_rollback(opts :: Keyword.t, state :: any) ::
    {:ok, result, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

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
  `{:error, exception, state}` to return an error and continue or
  `{:disconnect, exception, state}` to return an error and disconnect.

  This callback is called in the client process.
  """
  @callback handle_execute(query, params, opts :: Keyword.t, state :: any) ::
    {:ok, result, new_state :: any} |
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
  `{:error, exception, state}` to return an error and continue or
  `{:disconnect, exception, state}` to return an error and disconnect.

  This callback is called in the client process.
  """
  @callback handle_declare(query, params, opts :: Keyword.t, state :: any) ::
    {:ok, cursor, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @doc """
  Fetch the first result from a cursor declared by `handle_declare/4`. Return
  `{:ok, result, state}` to return the result `result` and continue,
  `{:deallocate, result, state}` to return the result `result` and deallocate,
  `{:error, exception, state}` to return an error and close the cursor,
  `{:disconnect, exception, state}` to return an error and disconnect.

  This callback is called in the client process.
  """
  @callback handle_first(query, cursor, opts :: Keyword.t, state :: any) ::
    {:ok | :deallocate, result, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @doc """
  Fetch the next result from a cursor declared by `handle_declare/4`. Return
  `{:ok, result, state}` to return the result `result` and continue,
  `{:deallocate, result, state}` to return the result `result` and deallocate,
  `{:error, exception, state}` to return an error and close the cursor,
  `{:disconnect, exception, state}` to return an error and disconnect.

  This callback is called in the client process.
  """
  @callback handle_next(query, cursor, opts :: Keyword.t, state :: any) ::
    {:ok | :deallocate, result, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @doc """
  Deallocate a cursor declared by `handle_declare/4' with the database. Return
  `{:ok, result, state}` on success and to continue,
  `{:error, exception, state}` to return an error and continue, or
  `{:disconnect, exception, state}` to return an error and disconnect.

  This callback is called in the client process.
  """
  @callback handle_deallocate(query, cursor, opts :: Keyword.t, state :: any) ::
    {:ok, result, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @doc """
  Handle a message received by the connection process when checked in.
  Return `{:ok, state}` to continue or `{:disconnect, exception,
  state}` to disconnect.

  Messages received by the connection process when checked out will be
  logged and discared.

  This callback is called in the connection process.
  """
  @callback handle_info(msg :: any, state :: any) ::
    {:ok, new_state :: any} |
    {:disconnect, Exception.t, new_state :: any}

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
  Use `DBConnection` to set the behaviour and include default
  no-op implementations for `ping/1` and `handle_info/2`.
  """
  defmacro __using__(_) do
    quote location: :keep do
      @behaviour DBConnection

      def connect(_) do
        # We do this to trick dialyzer to not complain about non-local returns.
        message = "connect/1 not implemented"
        case :erlang.phash2(1, 1) do
          0 -> raise message
          1 -> {:error, RuntimeError.exception(message)}
        end
      end

      def disconnect(_, _) do
        message = "disconnect/2 not implemented"
        case :erlang.phash2(1, 1) do
          0 -> raise message
          1 -> :ok
        end
      end

      def checkout(state) do
        message = "checkout/1 not implemented"
        case :erlang.phash2(1, 1) do
          0 -> raise message
          1 -> {:disconnect, RuntimeError.exception(message), state}
        end
      end

      def checkin(state) do
        message = "checkin/1 not implemented"
        case :erlang.phash2(1, 1) do
          0 -> raise message
          1 -> {:disconnect, RuntimeError.exception(message), state}
        end
      end

      def ping(state), do: {:ok, state}

      def handle_begin(_, state) do
        message = "handle_begin/2 not implemented"
        case :erlang.phash2(1, 1) do
          0 -> raise message
          1 -> {:error, RuntimeError.exception(message), state}
        end
      end

      def handle_commit(_, state) do
        message = "handle_commit/2 not implemented"
        case :erlang.phash2(1, 1) do
          0 -> raise message
          1 -> {:error, RuntimeError.exception(message), state}
        end
      end

      def handle_rollback(_, state) do
        message = "handle_rollback/2 not implemented"
        case :erlang.phash2(1, 1) do
          0 -> raise message
          1 -> {:error, RuntimeError.exception(message), state}
        end
      end

      def handle_prepare(_, _, state) do
       message = "handle_prepare/3 not implemented"
        case :erlang.phash2(1, 1) do
          0 -> raise message
          1 -> {:error, RuntimeError.exception(message), state}
        end
      end

      def handle_execute(_, _, _, state) do
        message = "handle_execute/4 not implemented"
        case :erlang.phash2(1, 1) do
          0 -> raise message
          1 -> {:error, RuntimeError.exception(message), state}
        end
      end

      def handle_close(_, _, state) do
        message = "handle_close/3 not implemented"
        case :erlang.phash2(1, 1) do
          0 -> raise message
          1 -> {:error, RuntimeError.exception(message), state}
        end
      end

      def handle_declare(_, _, _, state) do
       message = "handle_declare/4 not implemented"
        case :erlang.phash2(1, 1) do
          0 -> raise message
          1 -> {:error, RuntimeError.exception(message), state}
        end
      end

      def handle_first(_, _, _, state) do
       message = "handle_first/4 not implemented"
        case :erlang.phash2(1, 1) do
          0 -> raise message
          1 -> {:error, RuntimeError.exception(message), state}
        end
      end

      def handle_next(_, _, _, state) do
       message = "handle_next/4 not implemented"
        case :erlang.phash2(1, 1) do
          0 -> raise message
          1 -> {:error, RuntimeError.exception(message), state}
        end
      end

      def handle_deallocate(_, _, _, state) do
        message = "handle_deallocate/4 not implemented"
        case :erlang.phash2(1, 1) do
          0 -> raise message
          1 -> {:error, RuntimeError.exception(message), state}
        end
      end

      def handle_info(_, state), do: {:ok, state}

      defoverridable [connect: 1, disconnect: 2, checkout: 1, checkin: 1,
                      ping: 1, handle_begin: 2, handle_commit: 2,
                      handle_rollback: 2, handle_prepare: 3, handle_execute: 4,
                      handle_close: 3, handle_declare: 4, handle_first: 4,
                      handle_next: 4, handle_deallocate: 4, handle_info: 2]
    end
  end

  @doc """
  Ensures the given pool applications have been started.

  ### Options

    * `:pool` - The `DBConnection.Pool` module to use, (default:
    `DBConnection.Connection`)

  """
  @spec ensure_all_started(opts :: Keyword.t, type :: atom) ::
    {:ok, [atom]} | {:error, atom}
  def ensure_all_started(opts, type \\ :temporary) do
    Keyword.get(opts, :pool, DBConnection.Connection).ensure_all_started(opts, type)
  end

  @doc """
  Start and link to a database connection process.

  ### Options

    * `:pool` - The `DBConnection.Pool` module to use, (default:
    `DBConnection.Connection`)
    * `:idle` - The idle strategy, `:passive` to avoid checkin when idle and
    `:active` to checkin when idle (default: `:passive`)
    * `:idle_timeout` - The idle timeout to ping the database (default:
    `1_000`)
    * `:backoff_min` - The minimum backoff interval (default: `1_000`)
    * `:backoff_max` - The maximum backoff interval (default: `30_000`)
    * `:backoff_type` - The backoff strategy, `:stop` for no backoff and
    to stop, `:exp` for exponential, `:rand` for random and `:rand_exp` for
    random exponential (default: `:rand_exp`)
    * `:after_connect` - A function to run on connect using `run/3`, either
    a 1-arity fun, `{module, function, args}` with `DBConnection.t` prepended
    to `args` or `nil` (default: `nil`)
    * `:name` - A name to register the started process (see the `:name` option
    in `GenServer.start_link/3`).

  ### Example

      {:ok, conn} = DBConnection.start_link(mod, [idle_timeout: 5_000])
  """
  @spec start_link(module, opts :: Keyword.t) :: GenServer.on_start
  def start_link(conn_mod, opts) do
    pool_mod = Keyword.get(opts, :pool, DBConnection.Connection)
    apply(pool_mod, :start_link, [conn_mod, opts])
  end

  @doc """
  Create a supervisor child specification for a pool of connections.

  See `Supervisor.Spec` for child options (`child_opts`).
  """
  @spec child_spec(module, opts :: Keyword.t, child_opts :: Keyword.t) ::
    Supervisor.Spec.spec
  def child_spec(conn_mod, opts, child_opts \\ []) do
    pool_mod = Keyword.get(opts, :pool, DBConnection.Connection)
    apply(pool_mod, :child_spec, [conn_mod, opts, child_opts])
  end

  @doc """
  Prepare a query with a database connection for later execution and
  returns `{:ok, query}` on success or `{:error, exception}` if there was
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

      query         = %Query{statement: "SELECT id FROM table"}
      {:ok, query}  = DBConnection.prepare(conn, query)
      {:ok, result} = DBConnection.execute(conn, query, [])
      :ok           = DBConnection.close(conn, query)

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
      with {:ok, query, meter} <- parse(query, meter(opts), opts),
           {:ok, query, result, meter}
            <- run(conn, &run_prepare_execute/6, query, params, meter, opts),
           {:ok, result, meter} <- decode(query, result, meter, opts) do
        {:ok, query, result, meter}
      end
    log(result, :prepare_execute, query, params)
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
  `{:ok, result}` on success or `{:error, exception}` if there was an
  error.

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
    {:ok, result} | {:error, Exception.t}
  def execute(conn, query, params, opts \\ []) do
    result =
      with {:ok, params, meter} <- encode(query, params, meter(opts), opts),
           {:ok, result, meter}
            <- run(conn, &run_execute/6, query, params, meter, opts) do
           decode(query, result, meter, opts)
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
      {:ok, result} -> result
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
    |> cleanup(&run_close/6, query, meter(opts), opts)
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
          fun.(conn)
        after
          checkin(conn, opts)
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
  def transaction(%DBConnection{conn_mode: :transaction} = conn, fun, _opts) do
    %DBConnection{conn_ref: conn_ref} = conn
    try do
      fun.(conn)
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
        conclude(conn, result)
    end
  end
  def transaction(conn, fun, opts) do
    case run(conn, &run_transaction/5, fun, meter(opts), opts) do
      {query, {:ok, res, {kind, reason, stack}, meter}} ->
        log(meter, :transaction, query, nil, {:ok, res})
        :erlang.raise(kind, reason, stack)
      {query, {:ok, res, return, meter}} ->
        log(meter, :transaction, query, nil, {:ok, res})
        return
      {query, {:error, err, meter}} when query != nil ->
        log(meter, :transaction, query, nil, {:error, err})
        raise err
      {query, return} ->
        log(return, :transaction, query, nil)
    end
  end

  @doc """
  Rollback a transaction, does not return.

  Aborts the current transaction fun. If inside `transaction/3` bubbles
  up to the top level.

  ### Example

      {:error, :bar} = DBConnection.transaction(conn, fn(conn) ->
        DBConnection.rollback(conn, :bar)
        IO.puts "never reaches here!"
      end)
  """
  @spec rollback(t, reason :: any) :: no_return
  def rollback(conn, reason) do
    %DBConnection{conn_ref: conn_ref, conn_mode: mode} = conn
    case get_info(conn, nil) do
      {:error, err, _meter} ->
        raise err
      {_status, _conn_state, _meter} when mode == :transaction ->
        throw({__MODULE__, conn_ref, reason})
      {_status, _conn_state, _meter} ->
        raise "not inside transaction"
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
  `handle_first/4`, `handle_next/4' and `handle_deallocate/4`.

  ### Example

      {:ok, results} = DBConnection.transaction(conn, fn(conn) ->
        query  = %Query{statement: "SELECT id FROM table"}
        stream = DBConnection.prepare_stream(conn, query, [])
        Enum.to_list(stream)
      end)
  """
  @spec prepare_stream(t, query, params, opts :: Keyword.t) ::
    DBConnection.PrepareStream.t
  def prepare_stream(%DBConnection{} = conn, query, params, opts) do
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
  are passed to `handle_declare/4`, `handle_first/4` , `handle_next/4` and
  `handle_deallocate/4`.

  ### Example

      {:ok, results} = DBConnection.transaction(conn, fn(conn) ->
        query  = %Query{statement: "SELECT id FROM table"}
        query  = DBConnection.prepare!(conn, query)
        stream = DBConnection.stream(conn, query, [])
        Enum.to_list(stream)
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
    declare = &prepare_declare(&1, query, params, &2)
    resource(conn, declare, &fetch/3, &deallocate/3, opts).(acc, fun)
  end
  def reduce(%DBConnection.Stream{} = stream, acc, fun) do
    %DBConnection.Stream{conn: conn, query: query, params: params,
                         opts: opts} = stream
    declare = &declare(&1, query, params, &2)
    resource(conn, declare, &fetch/3, &deallocate/3, opts).(acc, fun)
  end

  ## Helpers

  defp checkout(pool, meter, opts) do
    meter = event(meter, :checkout)
    pool_mod = Keyword.get(opts, :pool, DBConnection.Connection)
    try do
      apply(pool_mod, :checkout, [pool, opts])
    catch
      kind, reason ->
        stack = System.stacktrace()
        {kind, reason, stack, meter}
    else
      {:ok, pool_ref, conn_mod, conn_state} ->
        conn = %DBConnection{pool_mod: pool_mod, pool_ref: pool_ref,
          conn_mod: conn_mod, conn_ref: make_ref()}
        put_info(conn, conn_state)
        {:ok, conn, conn_state, meter}
      {:error, err} ->
        {:error, err, meter}
    end
  end

  defp checkin(conn, opts) do
    case delete_info(conn) do
      {:ok, conn_state} ->
        %DBConnection{pool_mod: pool_mod, pool_ref: pool_ref} = conn
        _ = apply(pool_mod, :checkin, [pool_ref, conn_state, opts])
        :ok
      {:error, _} = error ->
        error
    end
  end

  defp delete_disconnect(conn, conn_state, err, opts) do
    _ = delete_info(conn)
    %DBConnection{pool_mod: pool_mod, pool_ref: pool_ref} = conn
    args = [pool_ref, err, conn_state, opts]
    _ = apply(pool_mod, :disconnect, args)
    :ok
  end

  defp delete_stop(conn, conn_state, kind, reason, stack, opts) do
    _ = delete_info(conn)
    msg = "client #{inspect self()} stopped: " <>
      Exception.format(kind, reason, stack)
    exception = DBConnection.ConnectionError.exception(msg)
    %DBConnection{pool_mod: pool_mod, pool_ref: pool_ref} = conn
    args = [pool_ref, exception, conn_state, opts]
    _ = apply(pool_mod, :stop, args)
    :ok
  end

  defp handle(conn, conn_state, fun, args, meter, opts) do
    %DBConnection{conn_mod: conn_mod} = conn
    try do
      apply(conn_mod, fun, args ++ [opts, conn_state])
    else
      {:ok, result, conn_state} ->
        put_info(conn, conn_state)
        {:ok, result, meter}
      {:deallocate, result, conn_state}
          when fun in [:handle_first, :handle_next] ->
        put_info(conn, conn_state)
        {:deallocate, result, meter}
      {:error, err, conn_state} when fun == :handle_begin ->
        put_info(conn, conn_state)
        {:error, err, meter}
      {:error, err, conn_state} when fun != :handle_begin ->
        put_info(conn, conn_state)
        {:error, err, meter}
      {:disconnect, err, conn_state} ->
        delete_disconnect(conn, conn_state, err, opts)
        {:error, err, meter}
      other ->
        try do
          raise DBConnection.ConnectionError, "bad return value: #{inspect other}"
        catch
          :error, reason ->
            stack = System.stacktrace()
            delete_stop(conn, conn_state, :error, reason, stack, opts)
            {:error, reason, stack, meter}
        end
    catch
      kind, reason ->
        stack = System.stacktrace()
        delete_stop(conn, conn_state, kind, reason, stack, opts)
        {kind, reason, stack, meter}
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

  defp encode(query, params, meter, opts) do
    try do
      DBConnection.Query.encode(query, params, opts)
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

  defp run_prepare(conn, conn_state, query, meter, opts) do
    with {:ok, query, meter} <- prepare(conn, conn_state, query, meter, opts) do
      describe(conn, query, meter, opts)
    end
  end

  defp prepare(conn, conn_state, query, meter, opts) do
    meter = event(meter, :prepare)
    handle(conn, conn_state, :handle_prepare, [query], meter, opts)
  end

  defp run_prepare_execute(conn, conn_state, query, params, meter, opts) do
    with {:ok, query, meter}
          <- run_prepare(conn, conn_state, query, meter, opts),
         {:ok, params, meter} <- encode(conn, query, params, meter, opts),
         {:ok, conn_state, meter} <- fetch_info(conn, meter),
         {:ok, result, meter}
          <- run_execute(conn, conn_state, query, params, meter, opts) do
      {:ok, query, result, meter}
    end
  end

  defp run_execute(conn, conn_state, query, params, meter, opts) do
    meter = event(meter, :execute)
    handle(conn, conn_state, :handle_execute, [query, params], meter, opts)
  end

  defp raised_close(conn, query, meter, opts, kind, reason, stack) do
    with {status, conn_state, meter} when status in [:ok, :failed]
          <- get_info(conn, meter),
         {:ok, _, meter}
          <- run_close(conn, status, conn_state, query, meter, opts) do
      {kind, reason, stack, meter}
    end
  end

  defp run_close(conn, status, conn_state, query, meter, opts) do
    meter = event(meter, :close)
    cleanup(conn, status, conn_state, :handle_close, [query], meter, opts)
  end

  defp cleanup(%DBConnection{} = conn, fun, arg, meter, opts) do
    with {status, conn_state, meter} when status in [:ok, :failed]
          <- get_info(conn, meter) do
      fun.(conn, status, conn_state, arg, meter, opts)
    end
  end
  defp cleanup(pool, fun, arg, meter, opts) do
    with {:ok, conn, conn_state, meter} <- checkout(pool, meter, opts) do
      try do
        fun.(conn, :ok, conn_state, arg, meter, opts)
      after
        checkin(conn, opts)
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
        delete_disconnect(conn, conn_state, err, opts)
        {:error, err, meter}
      other ->
        try do
          raise DBConnection.ConnectionError, "bad return value: #{inspect other}"
        catch
          :error, reason ->
            stack = System.stacktrace()
            delete_stop(conn, conn_state, :error, reason, stack, opts)
            {:error, reason, stack, meter}
        end
    catch
      kind, reason ->
        stack = System.stacktrace()
        delete_stop(conn, conn_state, kind, reason, stack, opts)
        {kind, reason, stack, meter}
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
        checkin(conn, opts)
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
        checkin(conn, opts)
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
  defp log({:deallocate, res, meter}, call, query, params),
    do: log(meter, call, query, params, {:deallocate, res})
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
  defp entry_result({:deallocate, res}),
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

  defp run_transaction(conn, conn_state, fun, meter, opts) do
    case run_begin(conn, conn_state, meter, opts) do
      {:ok, res, meter} ->
        %DBConnection{conn_ref: conn_ref} = conn
        try do
          log(meter, :transaction, :begin, nil, {:ok, res})
          fun.(%DBConnection{conn | conn_mode: :transaction})
        catch
          :throw, {__MODULE__, ^conn_ref, reason} ->
            rollback(conn, {:error, reason}, opts)
          kind, reason ->
            stack = System.stacktrace()
            rollback(conn, {kind, reason, stack}, opts)
        else
          result ->
            commit(conn, result, opts)
        end
      {:error, err, meter} ->
        {:begin, {:error, err, meter}}
      {kind, reason, stack, meter} ->
        {:begin, {kind, reason, stack, meter}}
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

  defp conclude(conn, result) do
    case get_info(conn, nil) do
      {:ok, _conn_state, _meter} ->
        {:ok, result}
      _ ->
        {:error, :rollback}
    end
  end

  defp run_begin(conn, conn_state, meter, opts) do
    meter = event(meter, :begin)
    handle(conn, conn_state, :handle_begin, [], meter, opts)
  end

  defp rollback(conn, return, opts) do
    case get_info(conn, meter(opts)) do
      {status, conn_state, meter} when status in [:ok, :failed] ->
        run_rollback(conn, status, conn_state, return, meter, opts)
      {:error, _err, _meter} ->
        case return do
          {:error, reason} ->
            {nil, {:error, reason, nil}}
          {kind, stack, reason} ->
            {nil, {kind, stack, reason, nil}}
        end
    end
  end

  defp run_rollback(conn, status, conn_state, return, meter, opts) do
    meter = event(meter, :rollback)
    case cleanup(conn, status, conn_state, :handle_rollback, [], meter, opts) do
      {:ok, res, meter} ->
        {:rollback, {:ok, res, return, meter}}
      return ->
        {:rollback, return}
    end
  end

  defp commit(conn, result, opts) do
     case get_info(conn, meter(opts)) do
      {:ok, conn_state, meter} ->
        run_commit(conn, conn_state, {:ok, result}, meter, opts)
      {:failed, conn_state, meter} ->
        return = {:error, :rollback}
        run_rollback(conn, :failed, conn_state, return, meter, opts)
      {:error, _err, _meter} ->
        {nil, {:error, :rollback, nil}}
    end
  end

  defp run_commit(conn, conn_state, return, meter, opts) do
    meter = event(meter, :commit)
    case handle(conn, conn_state, :handle_commit, [], meter, opts) do
      {:ok, res, meter} ->
        {:commit, {:ok, res, return, meter}}
      return ->
        {:commit, return}
    end
  end

  defp prepare_declare(conn, query, params, opts) do
    result =
      with {:ok, query, meter} <- parse(query, meter(opts), opts) do
        run(conn, &run_prepare_declare/6, query, params, meter, opts)
      end
    case log(result, :prepare_declare, query, params) do
      {:ok, query, cursor} ->
        {:first, query, cursor}
      {:error, err} ->
        raise err
    end
  end

  defp run_prepare_declare(conn, conn_info, query, params, meter, opts) do
    with {:ok, query, meter} <- prepare(conn, conn_info, query, meter, opts),
         {:ok, query, meter} <- describe(conn, query, meter, opts),
         {:ok, params, meter} <- encode(conn, query, params, meter, opts),
         {:ok, cursor, meter} <- declare(conn, query, params, meter, opts) do
      {:ok, query, cursor, meter}
    end
  end

  defp declare(conn, query, params, opts) do
    result =
      with {:ok, params, meter} <- encode(query, params, meter(opts), opts) do
        run(conn, &run_declare/6, query, params, meter, opts)
      end
    case log(result, :declare, query, params) do
      {:ok, cursor} ->
        {:first, query, cursor}
      {:error, err} ->
        raise err
    end
  end

  defp declare(conn, query, params, meter, opts) do
    with {:ok, conn_state, meter} <- fetch_info(conn, meter) do
      run_declare(conn, conn_state, query, params, meter, opts)
    end
  end

  defp run_declare(conn, conn_state, query, params, meter, opts) do
    meter = event(meter, :declare)
    handle(conn, conn_state, :handle_declare, [query, params], meter, opts)
  end

  defp fetch(conn, {:first, query, cursor}, opts) do
    fetch(conn, :handle_first, :first, query, cursor,opts)
  end
  defp fetch(conn, {:next, query, cursor}, opts) do
    fetch(conn, :handle_next, :next, query, cursor, opts)
  end
  defp fetch(_, {:deallocate, _,  _} = state, _) do
    {:halt, state}
  end

  defp fetch(conn, fun, call, query, cursor, opts) do
    result =
      conn
      |> run(&run_fetch/6, fun, [query, cursor], meter(opts), opts)
      |> log(call, query, cursor)
    case result do
      {:ok, result} ->
        {[result], {:next, query, cursor}}
      {:deallocate, result} ->
        {[result], {:deallocate, query, cursor}}
      {:error, err} ->
        raise err
    end
  end

  defp run_fetch(conn, conn_info, fun, [query, _] = args, meter, opts) do
    meter = event(meter, :fetch)
    with {ok, result, meter} when ok in [:ok, :deallocate]
          <- handle(conn, conn_info, fun, args, meter, opts),
         {:ok, result, meter} <- decode(query, result, meter, opts) do
      {ok, result, meter}
    end
  end

  defp deallocate(conn, {_, query, cursor}, opts) do
    case get_info(conn, meter(opts)) do
      {status, conn_state, meter} when status in [:ok, :failed] ->
        deallocate(conn, status, conn_state, query, cursor, meter, opts)
      {:error, err, _meter} ->
        {:error, err}
    end
  end

  defp deallocate(conn, status, conn_state, query, cursor, meter, opts) do
    result =
      conn
      |> run_deallocate(status, conn_state, query, cursor, meter, opts)
      |> log(:deallocate, query, cursor)
    case result do
      {:ok, _} ->
        :ok
      {:error, err} ->
        raise err
    end
  end

  defp run_deallocate(conn, status, conn_state, query, cursor, meter, opts) do
    meter = event(meter, :deallocate)
    args = [query, cursor]
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
