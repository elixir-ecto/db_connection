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
  defstruct [:pool_mod, :pool_ref, :conn_mod, :conn_ref, :proxy_mod]

  @typedoc """
  Run or transaction connection reference.
  """
  @type t :: %__MODULE__{pool_mod: module,
                         pool_ref: any,
                         conn_mod: any,
                         conn_ref: reference,
                         proxy_mod: module | nil}
  @type conn :: GenSever.server | t
  @type query :: any
  @type params :: any
  @type result :: any

  @doc """
  Connect to the databases. Return `{:ok, state}` on success or
  `{:error, exception}` on failure.

  If an error is returned it will be logged and another
  connection attempt will be made after a backoff interval.

  This callback is called in the connection process.
  """
  @callback connect(opts :: Keyword.t) ::
    {:ok, state :: any} | {:error, Exception.t}

  @doc """
  Checkouts the state from the connection process. Return `{:ok, state}`
  to allow the checkout or `{:disconnect, exception} to disconnect.

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
  to allow the checkin or `{:disconnect, exception}` to disconnect.

  This callback is called when the control of the state is passed back
  to the connection process. It should reverse any changes made in
  `checkout/2`.

  This callback is called in the connection process.
  """
  @callback checkin(state :: any) ::
    {:ok, new_state :: any} | {:disconnect, Exception.t, new_state :: any}

  @doc """
  Called when the connection has been idle for a period of time. Return
  `{:ok, state}` to continue or `{:disconnect, exception}` to
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
  Handle the beginning of a transaction. Return `{:ok, state}` to
  continue, `{:error, exception, state}` to abort the transaction and
  continue or `{:disconnect, exception, state}` to abort the transaction
  and disconnect.

  This callback is called in the client process.
  """
  @callback handle_begin(opts :: Keyword.t, state :: any) ::
    {:ok, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @doc """
  Handle commiting a transaction. Return `{:ok, state}` on success and
  to continue, `{:error, exception, state}` to abort the transaction and
  continue or `{:disconnect, exception, state}` to abort the transaction
  and disconnect.

  This callback is called in the client process.
  """
  @callback handle_commit(opts :: Keyword.t, state :: any) ::
    {:ok, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @doc """
  Handle rolling back a transaction. Return `{:ok, state}` on success
  and to continue, `{:error, exception, state}` to abort the transaction
  and continue or `{:disconnect, exception, state}` to abort the
  transaction and disconnect.

  A transaction will be rolled back if an exception occurs or
  `rollback/2` is called.

  This callback is called in the client process.
  """
  @callback handle_rollback(opts :: Keyword.t, state :: any) ::
    {:ok, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @doc """
  Prepare a query with the database. Return `{:ok, query, state}` where
  `query` is a query to pass to `execute/4` or `close/3`,
  `{:error, exception, state}` to return an error and continue or
  `{:disconnect, exception, state}` to return an error and disconnect.

  This callback is intended for cases where the state of a connection is
  needed to prepare a query and/or the query can be saved in the
  database to call later.

  If the connection is not required to prepare a query, `query/4`
  should be used and the query can be prepared by the
  `DBConnection.Query` protocol.

  This callback is called in the client process.
  """
  @callback handle_prepare(query, opts :: Keyword.t, state :: any) ::
    {:ok, query, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @doc """
  Execute a query prepared by `handle_prepare/3`. Return
  `{:ok, result, state}` to return the result `result` and continue,
  `{:prepare, state}` to retry execute after preparing the query,
  `{:error, exception, state}` to return an error and continue or
  `{:disconnect, exception, state{}` to return an error and disconnect.

  This callback is called in the client process.
  """
  @callback handle_execute(query, params, opts :: Keyword.t, state :: any) ::
    {:ok, result, new_state :: any} |
    {:prepare, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @doc """
  Execute a query prepared by `handle_prepare/3` and close it. Return
  `{:ok, result, state}` to return the result `result` and continue,
  `{:prepare, state}` to retry execute after preparing the query,
  `{:error, exception, state}` to return an error and continue or
  `{:disconnect, exception, state{}` to return an error and disconnect.

  This callback should be equivalent to calling `handle_execute/4` and
  `handle_close/3`.

  This callback is called in the client process.
  """
  @callback handle_execute_close(query, params, opts :: Keyword.t,
  state :: any) ::
    {:ok, result, new_state :: any} |
    {:prepare, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @doc """
  Close a query prepared by `handle_prepare/3` with the database. Return
  `{:ok, state}` on success and to continue,
  `{:error, exception, state}` to return an error and continue, or
  `{:disconnect, exception, state}` to return an error and disconnect.

  This callback is called in the client process.
  """
  @callback handle_close(query, opts :: Keyword.t, state :: any) ::
    {:ok, new_state :: any} |
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
  cases the exception will be a `DBConnection.Error.

  This callback is called in the connection process.
  """
  @callback disconnect(err :: Exception.t, state :: any) :: :ok

  @doc """
  Use `DBConnection` to set the behaviour and include default
  implementations for `handle_prepare/3` (no-op), `handle_execute_close/4`
  (forwards to `handle_execute/4` and `handle_close/3`) and `handle_close/3`
  (no-op). `handle_info/2` is also implemented as a no-op.
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

      def checkout(_) do
        message = "checkout/1 not implemented"
        case :erlang.phash2(1, 1) do
          0 -> raise message
          1 -> {:error, RuntimeError.exception(message)}
        end
      end

      def checkin(_) do
        message = "checkin/1 not implemented"
        case :erlang.phash2(1, 1) do
          0 -> raise message
          1 -> {:error, RuntimeError.exception(message)}
        end
      end

      def ping(state), do: {:ok, state}

      def handle_begin(_, _) do
        message = "handle_begin/2 not implemented"
        case :erlang.phash2(1, 1) do
          0 -> raise message
          1 -> {:error, RuntimeError.exception(message)}
        end
      end

      def handle_commit(_, _) do
        message = "handle_commit/2 not implemented"
        case :erlang.phash2(1, 1) do
          0 -> raise message
          1 -> {:error, RuntimeError.exception(message)}
        end
      end

      def handle_rollback(_, _) do
        message = "handle_rollback/2 not implemented"
        case :erlang.phash2(1, 1) do
          0 -> raise message
          1 -> {:error, RuntimeError.exception(message)}
        end
      end

      def handle_prepare(query, _, state), do: {:ok, query, state}

      def handle_execute(_, _, _, _) do
        message = "handle_execute/4 not implemented"
        case :erlang.phash2(1, 1) do
          0 -> raise message
          1 -> {:error, RuntimeError.exception(message)}
        end
      end

      def handle_execute_close(query, params, opts, state) do
        case handle_execute(query, params, opts, state) do
          {:ok, result, state} ->
            case handle_close(query, opts, state) do
              {:ok, state} -> {:ok, result, state}
              other        -> other
            end
          {:error, err, state} ->
            case handle_close(query, opts, state) do
              {:ok, state} -> {:error, err, state}
              other        -> other
            end
          other ->
            other
        end
      end

      def handle_close(_, _, state), do: {:ok, state}

      def handle_info(_, state), do: {:ok, state}

      defoverridable [connect: 1, disconnect: 2, checkout: 1, checkin: 1,
                      ping: 1, handle_begin: 2, handle_commit: 2,
                      handle_rollback: 2, handle_prepare: 3, handle_execute: 4,
                      handle_execute_close: 4, handle_close: 3, handle_info: 2]
    end
  end

  @doc """
  Start and link to a database connection process.

  ### Options

    * `:pool` - The `DBConnection.Pool` module to use, (default:
    `DBConnection.Connection`)
    * `:idle_timeout` - The idle timeout to ping the database (default:
    `15_000`)
    * `:backoff_start` - The first backoff interval (default: `200`)
    * `:backoff_max` - The maximum backoff interval (default: `15_000`)
    * `:backoff_type` - The backoff strategy, `:stop` for no backoff and
    to stop (see `:backoff`, default: `:jitter`)
    * `:after_connect` - A function to run on connect using `run/3`, either
    a 1-arity fun, `{module, function, args}` with `DBConnection.t` prepended
    to `args` or `nil` (default: `nil`)

  ### Example

      {:ok, pid} = DBConnection.start_link(mod, [idle_timeout: 5_000])
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
  Run a query with a database connection and returns `{:ok, result}` on
  success or `{:error, exception}` if there was an error.

  ### Options

    * `:pool_timeout` - The maximum time to wait for a reply when making a
    synchronous call to the pool (default: `5_000`)
    * `:queue` - Whether to block waiting in an internal queue for the
    connection's state (boolean, default: `true`)
    * `:timeout` - The maximum time that the caller is allowed the
    to hold the connection's state (ignored when using a run/transaction
    connection, default: `15_000`)
    * `:proxy` - The `DBConnection.Proxy` module, if any, to proxy the
    connection's state (ignored when using a run/transaction connection,
    default: `nil`)

  The pool and connection module may support other options. All options
  are passed to `handle_prepare/3` and `handle_execute_close/4`.

  ### Example

      {:ok, _} = DBConnection.query(pid, "SELECT id FROM table", [], [])
  """
  @spec query(conn, query, params, opts :: Keyword.t) ::
    {:ok, result} | {:error, Exception.t}
  def query(conn, query, params, opts \\ []) do
    query = DBConnection.Query.parse(query, opts)
    case run_query(conn, query, params, opts) do
      {:ok, query, result} ->
        {:ok, DBConnection.Query.decode(query, result, opts)}
      other ->
        other
    end
  end

  @doc """
  Run a query with a database connection and return the result. An
  exception is raised on error.

  See `query/3`.
  """
  @spec query!(conn, query, params, opts :: Keyword.t) :: result
  def query!(conn, query, params, opts \\ []) do
    case query(conn, query, params, opts) do
      {:ok, result} -> result
      {:error, err} -> raise err
    end
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
    * `:proxy` - The `DBConnection.Proxy` module, if any, to proxy the
    connection's state (ignored when using a run/transaction connection,
    default: `nil`)

  The pool and connection module may support other options. All options
  are passed to `handle_prepare/3`.

  ## Example

      {ok, query}   = DBConnection.prepare(pid, "SELECT id FROM table")
      {:ok, result} = DBConnection.execute(pid, query, [])
      :ok           = DBConnection.close(pid, query)
  """
  @spec prepare(conn, query, opts :: Keyword.t) ::
    {:ok, query} | {:error, Exception.t}
  def prepare(conn, query, opts \\ []) do
    query = DBConnection.Query.parse(query, opts)
    case handle(conn, :handle_prepare, [query], opts, :result) do
      {:ok, query} ->
        {:ok, DBConnection.Query.describe(query, opts)}
      other ->
        other
    end
  end

  @doc """
  Prepare a query with a database connection and return the prepared
  query. An exception is raised on error.

  See `prepare/3`.
  """
  @spec prepare!(conn, query, opts :: Keyword.t) :: query
  def prepare!(conn, query, opts) do
    case prepare(conn, query, opts) do
      {:ok, result} -> result
      {:error, err} -> raise err
    end
  end

  @doc """
  Prepare a query and execute it with a database connection and return both the
  preprared query and the result, `{:ok, query, result}` on success or
  `{:error, exception}` if there was an error.

  This function is different to `query/4` because the query is also returned,
  whereas the `query` is closed with `query/4`.

  The returned `query` can be passed to `execute/4`, `execute_close/4`, and/or
  `close/3`

  ### Options

    * `:pool_timeout` - The maximum time to wait for a reply when making a
    synchronous call to the pool (default: `5_000`)
    * `:queue` - Whether to block waiting in an internal queue for the
    connection's state (boolean, default: `true`)
    * `:timeout` - The maximum time that the caller is allowed the
    to hold the connection's state (ignored when using a run/transaction
    connection, default: `15_000`)
    * `:proxy` - The `DBConnection.Proxy` module, if any, to proxy the
    connection's state (ignored when using a run/transaction connection,
    default: `nil`)

   ## Example

      {ok, query, result} = DBConnection.prepare_execute(pid, "SELECT id FROM table WHERE id=$1", [1])
      {:ok, result2}      = DBConnection.execute(pid, query, [2])
      :ok                 = DBConnection.close(pid, query)
   """
  @spec prepare_execute(conn, query, params, Keyword.t) ::
    {:ok, {query, result}} |
    {:error, Exception.t}
  def prepare_execute(conn, query, params, opts \\ []) do
    query = DBConnection.Query.parse(query, opts)
    case run_prepare_execute(conn, query, params, opts) do
      {:ok, query, result} ->
        {:ok, query, DBConnection.Query.decode(query, result, opts)}
      other ->
        other
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
    * `:proxy` - The `DBConnection.Proxy` module, if any, to proxy the
    connection's state (ignored when using a run/transaction connection,
    default: `nil`)

  The pool and connection module may support other options. All options
  are passed to `handle_execute/4`.

  See `prepare/3`.
  """
  @spec execute(conn, query, params, opts :: Keyword.t) ::
    {:ok, result} | {:error, Exception.t}
  def execute(conn, query, params, opts) do
    execute(conn, :handle_execute, query, params, opts)
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
  Execute a prepared query and close it with a database connection and return
  `{:ok, result}` on success or `{:error, exception}` if there was an
  error.

  All options are passed to `handle_execute_close/4`.

  See `execute/4` and `close/3`.
  """
  @spec execute_close(conn, query, params, opts :: Keyword.t) ::
    {:ok, result} | {:error, Exception.t}
  def execute_close(conn, query, params, opts \\ []) do
    execute(conn, :handle_execute_close, query, params, opts)
  end

  @doc """
  Execute a prepared query and close it with a database connection and return
  the result. Raises an exception on error.

  See `execute_close/4`
  """
  @spec execute_close!(conn, query, params, opts :: Keyword.t) :: result
  def execute_close!(conn, query, params, opts \\ []) do
    case execute_close(conn, query, params, opts) do
      {:ok, result} -> result
      {:error, err} -> raise err
    end
  end

  @doc """
  Close a prepared query on a database connection and return `:ok` on
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
    * `:proxy` - The `DBConnection.Proxy` module, if any, to proxy the
    connection's state (ignored when using a run/transaction connection,
    default: `nil`)

  The pool and connection module may support other options. All options
  are passed to `handle_close/3`.

  See `prepare/3`.
  """
  @spec close(conn, query, opts :: Keyword.t) ::
    :ok | {:error, Exception.t}
  def close(conn, query, opts \\ []) do
    handle(conn, :handle_close, [query], opts, :no_result)
  end

  @doc """
  Close a prepared query on a database connection and return `:ok`.
  Raises an exception on error.

  See `close/3`.
  """
  @spec close!(conn, query, opts :: Keyword.t) :: :ok
  def close!(conn, query, opts \\ []) do
    case close(conn, query, opts) do
      :ok -> :ok
      {:error, err} -> raise err
    end
  end

  @doc """
  Acquire a lock on a connection and run a series of requests on it. The
  result of the fun is return inside an `:ok` tuple: `{:ok result}`.

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
    * `:proxy` - The `DBConnection.Proxy` module, if any, to proxy the
    connection's state (ignored when using a run/transaction connection,
    default: `nil`)

  The pool may support other options.

  ### Example

      {:ok, res} = DBConnection.run(pid, fn(conn) ->
        res = DBConnection.query!(conn, "SELECT id FROM table", [])
        res
      end)
  """
  @spec run(conn, (t -> result), opts :: Keyword.t) :: result when result: var
  def run(%DBConnection{} = conn, fun, _) do
    _ = fetch_info(conn)
    fun.(conn)
  end
  def run(pool, fun, opts) do
    case checkout(pool, opts) do
      {conn, conn_state} ->
        put_info(conn, :idle, conn_state)
        run_begin(conn, fun, opts)
      {conn, conn_state, proxy_state} ->
        put_info(conn, :idle, conn_state, proxy_state)
        run_begin(conn, fun, opts)
    end
  end

  @doc """
  Acquire a lock on a connection and run a series of requests inside a
  tranction. The result of the transaction fun is return inside an `:ok`
  tuple: `{:ok result}`.

  To use the locked connection call the request with the connection
  reference passed as the single argument to the `fun`. If the
  connection disconnects all future calls using that connection
  reference will fail.

  `run/3` and `transaction/3` can be nested multiple times. If a transaction is
  rolled back or a nested transaction `fun` raises the transaction is marked as
  failed. Any calls inside a failed transaction (except `rollback/2`) will raise
  until the outer transaction call returns. All running `transaction/3` calls
  will return `{:error, :rollback}` if the transaction failed and `rollback/2`
  is not called for that `transaction/3`.

  ### Options

    * `:pool_timeout` - The maximum time to wait for a reply when making a
    synchronous call to the pool (default: `5_000`)
    * `:queue` - Whether to block waiting in an internal queue for the
    connection's state (boolean, default: `true`)
    * `:timeout` - The maximum time that the caller is allowed the
    to hold the connection's state (default: `15_000`)
    * `:proxy` - The `DBConnection.Proxy` module, if any, to proxy the
    connection's state (ignored when using a run/transaction connection,
    default: `nil`)

  The pool and connection module may support other options. All options
  are passed to `handle_begin/2`, `handle_commit/2` and
  `handle_rollback/2`.

  ### Example

      {:ok, res} = DBConnection.transaction(pid, fn(conn) ->
        res = DBConnection.query!(conn, "SELECT id FROM table", [])
        res
      end)
  """
  @spec transaction(conn, (conn -> result), opts :: Keyword.t) ::
    {:ok, result} | {:error, reason :: any} when result: var
  def transaction(%DBConnection{} = conn, fun, opts) do
    case fetch_info(conn) do
      {:transaction, _} ->
        transaction_nested(conn, fun)
      {:transaction, _, _} ->
        transaction_nested(conn, fun)
      {:idle, conn_state} ->
        transaction_begin(conn, conn_state, fun, opts)
      {:idle, conn_state, proxy_state} ->
        transaction_begin(conn, conn_state, proxy_state, fun, opts)
    end
  end
  def transaction(pool, fun, opts) do
     run(pool, &transaction(&1, fun, opts), opts)
  end

  @doc """
  Rollback a transaction, does not return.

  Aborts the current transaction fun. If inside `transaction/3` bubbles
  up to the top level.

  ### Example

      {:error, :bar} = DBConnection.transaction(pid, fn(conn) ->
        DBConnection.rollback(conn, :bar)
        IO.puts "never reaches here!"
      end)
  """
  @spec rollback(t, reason :: any) :: no_return
  def rollback(%DBConnection{conn_ref: conn_ref} = conn, err) do
    case get_info(conn) do
      {transaction, _} when transaction in [:transaction, :failed] ->
        throw({:rollback, conn_ref, err})
      {transaction, _, _} when transaction in [:transaction, :failed] ->
        throw({:rollback, conn_ref, err})
      {:idle, _} ->
        raise "not inside transaction"
      {:idle, _, _} ->
        raise "not inside transaction"
      :closed ->
        raise DBConnection.Error, "connection is closed"
    end
  end

  ## Helpers

  defp checkout(pool, opts) do
    pool_mod = Keyword.get(opts, :pool, DBConnection.Connection)
    {proxy_mod, proxy_state} = proxy_mod(opts)
    case apply(pool_mod, :checkout, [pool, opts]) do
      {:ok, pool_ref, conn_mod, conn_state} when is_nil(proxy_mod) ->
        conn = %DBConnection{pool_mod: pool_mod, pool_ref: pool_ref,
          conn_mod: conn_mod, conn_ref: make_ref()}
        {conn, conn_state}
      {:ok, pool_ref, conn_mod, conn_state} ->
        conn = %DBConnection{pool_mod: pool_mod, pool_ref: pool_ref,
          conn_mod: conn_mod, conn_ref: make_ref(), proxy_mod: proxy_mod}
        checkout(conn, proxy_mod, proxy_state, conn_mod, opts, conn_state)
      :error ->
        raise DBConnection.Error, "connection not available"
    end
  end

  defp proxy_mod(opts) do
    case Keyword.get(opts, :proxy) do
      nil       -> {nil, nil}
      proxy_mod -> proxy_init(proxy_mod, opts)
    end
  end

  def proxy_init(proxy_mod, opts) do
    case apply(proxy_mod, :init, [opts]) do
      {:ok, proxy_state} -> {proxy_mod, proxy_state}
      :ignore            -> {nil, nil}
      {:error, err}      -> raise err
    end
  end

  defp checkout(conn, proxy_mod, proxy_state, conn_mod, opts, conn_state) do
    try do
      apply(proxy_mod, :checkout, [conn_mod, opts, conn_state, proxy_state])
    else
      {:ok, conn_state, proxy_state} ->
        {conn, conn_state, proxy_state}
      {:error, err, conn_state, proxy_state} ->
        checkin(conn, conn_state, proxy_state, opts)
        raise err
     {:disconnect, err, conn_state, proxy_state} ->
        delete_disconnect(conn, conn_state, proxy_state, err, opts)
        raise err
      other ->
        reason = {:bad_return_value, other}
        delete_stop(conn, conn_state, proxy_state, reason, opts)
        raise DBConnection.Error, "bad return value: #{inspect other}"
    catch
      kind, reason ->
        stack = System.stacktrace()
        stop_reason = reason(kind, reason, stack)
        delete_stop(conn, conn_state, proxy_state, stop_reason, opts)
        :erlang.raise(kind, reason, stack)
    end
  end

  defp checkin(conn, conn_state, opts) do
    %DBConnection{pool_mod: pool_mod, pool_ref: pool_ref} = conn
    _ = apply(pool_mod, :checkin, [pool_ref, conn_state, opts])
    :ok
  end

  defp checkin(conn, conn_state, proxy_state, opts) do
    %DBConnection{pool_mod: pool_mod, pool_ref: pool_ref,
      proxy_mod: proxy_mod, conn_mod: conn_mod} = conn
    try do
      apply(proxy_mod, :checkin, [conn_mod, opts, conn_state, proxy_state])
    else
      {:ok, conn_state, proxy_state} ->
        _ = apply(pool_mod, :checkin, [pool_ref, conn_state, opts])
        proxy_terminate(proxy_mod, proxy_state, :normal, opts)
     {:error, err, conn_state, proxy_state} ->
        _ = apply(pool_mod, :checkin, [pool_ref, conn_state, opts])
        proxy_terminate(proxy_mod, proxy_state, :normal, opts)
        raise err
     {:disconnect, err, conn_state, proxy_state} ->
        delete_disconnect(conn, conn_state, proxy_state, err, opts)
        raise err
      other ->
        reason = {:bad_return_value, other}
        delete_stop(conn, conn_state, proxy_state, reason, opts)
        raise DBConnection.Error, "bad return value: #{inspect other}"
    catch
      kind, reason ->
        stack = System.stacktrace()
        stop_reason = reason(kind, reason, stack)
        delete_stop(conn, conn_state, proxy_state, stop_reason, opts)
        :erlang.raise(kind, reason, stack)
    end
  end

  defp delete_disconnect(conn, conn_state, err, opts) do
    _ = delete_info(conn)
    %DBConnection{pool_mod: pool_mod, pool_ref: pool_ref} = conn
    args = [pool_ref, err, conn_state, opts]
    _ = apply(pool_mod, :disconnect, args)
    :ok
  end

  defp delete_disconnect(conn, conn_state, proxy_state, err, opts) do
    delete_disconnect(conn, conn_state, err, opts)
    %DBConnection{proxy_mod: proxy_mod} = conn
    proxy_terminate(proxy_mod, proxy_state, {:disconnect, err}, opts)
  end

  defp reason(:exit, reason, _), do: reason
  defp reason(:error, err, stack), do: {err, stack}
  defp reason(:throw, value, stack), do: {{:nocatch, value}, stack}

  defp delete_stop(conn, conn_state, reason, opts) do
    _ = delete_info(conn)
    %DBConnection{pool_mod: pool_mod, pool_ref: pool_ref} = conn
    args = [pool_ref, reason, conn_state, opts]
    _ = apply(pool_mod, :stop, args)
    :ok
  end

  defp delete_stop(conn, conn_state, proxy_state, reason, opts) do
    delete_stop(conn, conn_state, reason, opts)
    %DBConnection{proxy_mod: proxy_mod} = conn
    proxy_terminate(proxy_mod, proxy_state, {:stop, reason}, opts)
  end

  defp proxy_terminate(proxy_mod, proxy_state, reason, opts) do
    _ = apply(proxy_mod, :terminate, [reason, opts, proxy_state])
    :ok
  end

  defp handle(%DBConnection{proxy_mod: nil} = conn, fun, args, opts, return) do
    %DBConnection{conn_mod: conn_mod} = conn
    {status, conn_state} = fetch_info(conn)
    try do
      apply(conn_mod, fun, args ++ [opts, conn_state])
    else
      {:ok, result, conn_state} when return in [:result, :execute] ->
        put_info(conn, status, conn_state)
        {:ok, result}
      {:ok, conn_state} when return == :no_result ->
        put_info(conn, status, conn_state)
        :ok
      {:prepare, conn_state} when return == :execute ->
        put_info(conn, status, conn_state)
        :prepare
      {:error, _, conn_state} = error ->
        put_info(conn, status, conn_state)
        Tuple.delete_at(error, 2)
      {:disconnect, err, conn_state} ->
        delete_disconnect(conn, conn_state, err, opts)
        {:error, err}
      other ->
        delete_stop(conn, conn_state, {:bad_return_value, other}, opts)
        raise DBConnection.Error, "bad return value: #{inspect other}"
    catch
      kind, reason ->
        stack = System.stacktrace()
        delete_stop(conn, conn_state, reason(kind, reason, stack), opts)
        :erlang.raise(kind, reason, stack)
    end
  end
  defp handle(%DBConnection{} = conn, fun, args, opts, return) do
    %DBConnection{proxy_mod: proxy_mod, conn_mod: conn_mod} = conn
    {status, conn_state, proxy_state} = fetch_info(conn)
    args = [conn_mod | args] ++ [opts, conn_state, proxy_state]
    try do
      apply(proxy_mod, fun, args)
    else
      {:ok, result, conn_state, proxy_state}
      when return in [:result, :execute] ->
        put_info(conn, status, conn_state, proxy_state)
        {:ok, result}
      {:ok, conn_state, proxy_state} when return == :no_result ->
        put_info(conn, status, conn_state, proxy_state)
        :ok
      {:prepare, conn_state, proxy_state} when return == :execute ->
        put_info(conn, status, conn_state, proxy_state)
        :prepare
      {:error, err, conn_state, proxy_state} ->
        put_info(conn, status, conn_state, proxy_state)
        {:error, err}
      {:disconnect, err, conn_state, proxy_state} ->
        delete_disconnect(conn, conn_state, proxy_state, err, opts)
        {:error, err}
      other ->
        reason = {:bad_return_value, other}
        delete_stop(conn, conn_state, proxy_state, reason, opts)
        raise DBConnection.Error, "bad return value: #{inspect other}"
    catch
      kind, reason ->
        stack = System.stacktrace()
        stop_reason = reason(kind, reason, stack)
        delete_stop(conn, conn_state, proxy_state, stop_reason, opts)
        :erlang.raise(kind, reason, stack)
    end
  end
  defp handle(pool, fun, args, opts, return) do
    run(pool, &handle(&1, fun, args, opts, return), opts)
  end

  defp run_query(conn, query, params, opts) do
    run(conn, fn(conn2) ->
      case handle(conn2, :handle_prepare, [query], opts, :result) do
        {:ok, query} ->
          describe_execute(conn2, :handle_execute_close, query, params, opts)
        other ->
          other
      end
    end, opts)
  end

  defp describe_execute(conn, fun, query, params, opts) do
    query = DBConnection.Query.describe(query, opts)
    params = DBConnection.Query.encode(query, params, opts)
    prepared_execute(conn, fun, query, params, opts)
  end

  defp run_prepare_execute(conn, query, params, opts) do
    run(conn, fn(conn2) ->
      case handle(conn2, :handle_prepare, [query], opts, :result) do
        {:ok, query} ->
          describe_execute(conn2, :handle_execute, query, params, opts)
        other ->
          other
      end
    end, opts)
  end

  defp execute(conn, callback, query, params, opts) do
    params = DBConnection.Query.encode(query, params, opts)
    case run_execute(conn, callback, query, params, opts)  do
      {:ok, query, result} ->
       {:ok, DBConnection.Query.decode(query, result, opts)}
      other ->
        other
    end
  end

  defp run_execute(conn, callback, query, params, opts) do
    run(conn, fn(conn2) ->
      case handle(conn2, callback, [query, params], opts, :execute) do
        :prepare ->
          execute_prepare(conn2, callback, query, params, opts)
        {:ok, result} ->
          {:ok, query, result}
        other ->
          other
      end
    end, opts)
  end

  defp execute_prepare(conn, callback, query, params, opts) do
    case handle(conn, :handle_prepare, [query], opts, :result) do
      {:ok, query} ->
        prepared_execute(conn, callback, query, params, opts)
      other ->
        other
    end
  end

  defp prepared_execute(conn, callback, query, params, opts) do
    case handle(conn, callback, [query, params], opts, :execute) do
      :prepare ->
        raise DBConnection.Error, "connection did not prepare query"
      {:ok, result} ->
        {:ok, query, result}
      other ->
        other
    end
  end

  defp run_begin(conn, fun, opts) do
    try do
      fun.(conn)
    after
      run_end(conn, opts)
    end
  end

  defp run_end(conn, opts) do
    case delete_info(conn) do
      {:idle, conn_state} ->
        checkin(conn, conn_state, opts)
      {:idle, conn_state, proxy_state} ->
        checkin(conn, conn_state, proxy_state, opts)
      {:transaction, conn_state} ->
        delete_stop(conn, conn_state, :bad_run, opts)
        raise "connection run ended in transaction"
      {:transaction, conn_state, proxy_state} ->
        delete_stop(conn, conn_state, proxy_state, :bad_run, opts)
        raise "connection run ended in transaction"
      :closed ->
        :ok
    end
  end

  defp transaction_begin(conn, conn_state, fun, opts) do
    %DBConnection{conn_mod: conn_mod} = conn
    try do
      apply(conn_mod, :handle_begin, [opts, conn_state])
    else
      {:ok, conn_state} ->
        put_info(conn, :transaction, conn_state)
        transaction_run(conn, fun, opts)
      {:error, err, conn_state} ->
        put_info(conn, :idle, conn_state)
        raise err
      {:disconnect, err, conn_state} ->
        delete_disconnect(conn, conn_state, err, opts)
        raise err
       other ->
        delete_stop(conn, conn_state, {:bad_return_value, other}, opts)
        raise DBConnection.Error, "bad return value: #{inspect other}"
    catch
      kind, reason ->
        stack = System.stacktrace()
        delete_stop(conn, conn_state, reason(kind, reason, stack), opts)
        :erlang.raise(kind, reason, stack)
    end
  end

  defp transaction_begin(conn, conn_state, proxy_state, fun, opts) do
    %DBConnection{conn_mod: conn_mod, proxy_mod: proxy_mod} = conn
    try do
      apply(proxy_mod, :handle_begin, [conn_mod, opts, conn_state, proxy_state])
    else
      {:ok, conn_state, proxy_state} ->
        put_info(conn, :transaction, conn_state, proxy_state)
        transaction_run(conn, fun, opts)
      {:error, err, conn_state, proxy_state} ->
        put_info(conn, :idle, conn_state, proxy_state)
        raise err
      {:disconnect, err, conn_state, proxy_state} ->
        delete_disconnect(conn, conn_state, proxy_state, err, opts)
        raise err
       other ->
        reason = {:bad_return_value, other}
        delete_stop(conn, conn_state, proxy_state, reason, opts)
        raise DBConnection.Error, "bad return value: #{inspect other}"
    catch
      kind, reason ->
        stack = System.stacktrace()
        stop_reason = reason(kind, reason, stack)
        delete_stop(conn, conn_state, proxy_state, stop_reason, opts)
        :erlang.raise(kind, reason, stack)
    end
  end

  defp transaction_run(conn, fun, opts) do
    %DBConnection{conn_ref: conn_ref} = conn
    try do
      fun.(conn)
    else
      result ->
        result = {:ok, result}
        transaction_end(conn, :handle_commit, opts, result)
    catch
      :throw, {:rollback, ^conn_ref, reason} ->
        result = {:error, reason}
        transaction_end(conn, :handle_rollback, opts, result)
      kind, reason ->
        stack = System.stacktrace()
        transaction_end(conn, :handle_rollback, opts, :raise)
        :erlang.raise(kind, reason, stack)
    end
  end

  defp transaction_end(conn, fun, opts, result) do
    case get_info(conn) do
      {:transaction, conn_state} ->
        transaction_end(conn, conn_state, fun, opts, result)
      {:transaction, conn_state, proxy_state} ->
        transaction_end(conn, conn_state, proxy_state, fun, opts, result)
      {:failed, conn_state} ->
        result = failed_result(result)
        transaction_end(conn, conn_state, :handle_rollback, opts, result)
      {:failed, conn_state, proxy_state} ->
        fun = :handle_rollback
        result = failed_result(result)
        transaction_end(conn, conn_state, proxy_state, fun, opts, result)
      {:idle, conn_state} ->
        delete_stop(conn, conn_state, :bad_transaction, opts)
        raise "not in transaction"
      {:idle, conn_state, proxy_state} ->
        delete_stop(conn, conn_state, proxy_state, :bad_transaction, opts)
        raise "not in transaction"
     :closed ->
        result
    end
  end

  defp failed_result({:ok, _}), do: {:error, :rollback}
  defp failed_result(other),    do: other

  defp transaction_end(conn, conn_state, fun, opts, result) do
    %DBConnection{conn_mod: conn_mod} = conn
    try do
      apply(conn_mod, fun, [opts, conn_state])
    else
      {:ok, conn_state} ->
        put_info(conn, :idle, conn_state)
        result
      {:error, err, conn_state} ->
        put_info(conn, :idle, conn_state)
        raise err
      {:disconnect, err, conn_state} ->
        delete_disconnect(conn, conn_state, err, opts)
        raise err
       other ->
        delete_stop(conn, conn_state, {:bad_return_value, other}, opts)
        raise DBConnection.Error, "bad return value: #{inspect other}"
    catch
      kind, reason ->
        stack = System.stacktrace()
        delete_stop(conn, conn_state, reason(kind, reason, stack), opts)
        :erlang.raise(kind, reason, stack)
    end
  end

  defp transaction_end(conn, conn_state, proxy_state, fun, opts, result) do
    %DBConnection{proxy_mod: proxy_mod, conn_mod: conn_mod} = conn
    try do
      apply(proxy_mod, fun, [conn_mod, opts, conn_state, proxy_state])
    else
      {:ok, conn_state, proxy_state} ->
        put_info(conn, :idle, conn_state, proxy_state)
        result
      {:error, err, conn_state, proxy_state} ->
        put_info(conn, :idle, conn_state, proxy_state)
        raise err
      {:disconnect, err, conn_state, proxy_state} ->
        delete_disconnect(conn, conn_state, proxy_state, err, opts)
        raise err
       other ->
        reason = {:bad_return_value, other}
        delete_stop(conn, conn_state, proxy_state, reason, opts)
        raise DBConnection.Error, "bad return value: #{inspect other}"
    catch
      kind, reason ->
        stack = System.stacktrace()
        stop_reason = reason(kind, reason, stack)
        delete_stop(conn, conn_state, proxy_state, stop_reason, opts)
        :erlang.raise(kind, reason, stack)
    end
  end

  defp transaction_nested(conn, fun) do
    %DBConnection{conn_ref: conn_ref} = conn
    try do
      fun.(conn)
    else
      result ->
        transaction_ok(conn, {:ok, result})
    catch
      :throw, {:rollback, ^conn_ref, reason} ->
        transaction_failed(conn)
        {:error, reason}
      kind, reason ->
        stack = System.stacktrace()
        transaction_failed(conn)
        :erlang.raise(kind, reason, stack)
    end
  end

  defp transaction_ok(conn, result) do
    case get_info(conn) do
      {:failed, _} ->
        {:error, :rollback}
      {:failed, _, _} ->
        {:error, :rollback}
      _ ->
        result
    end
  end

  defp transaction_failed(conn) do
    case get_info(conn) do
      {:transaction, conn_state} ->
        put_info(conn, :failed, conn_state)
      {:transaction, conn_state, proxy_state} ->
        put_info(conn, :failed, conn_state, proxy_state)
      _ ->
        :ok
    end
  end

  defp put_info(conn, status, conn_state) do
    _ = Process.put(key(conn), {status, conn_state})
    :ok
  end

  defp put_info(conn, status, conn_state, proxy_state) do
    _ = Process.put(key(conn), {status, conn_state, proxy_state})
    :ok
  end

  defp fetch_info(conn) do
    case get_info(conn) do
      {:failed, _}     -> raise DBConnection.Error, "transaction rolling back"
      {:failed, _, _}  -> raise DBConnection.Error, "transaction rolling back"
      {_, _} = info    -> info
      {_, _, _} = info -> info
      :closed          -> raise DBConnection.Error, "connection is closed"
    end
  end

  defp get_info(conn), do: Process.get(key(conn), :closed)

  defp delete_info(conn) do
    Process.delete(key(conn)) || :closed
  end

  defp key(%DBConnection{conn_ref: conn_ref}), do: {__MODULE__, conn_ref}
end
