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

  The `DBConnection.Query` and `DBConnection.Result` protocols provide
  utility functions so that queries can be prepared or encoded and
  results decoding without blocking the connection or pool. Unless set
  to `:manual` these protocols will be called automatically at the
  appropriate moment.

  By default the `DBConnection` provides a single connection. However
  the `:pool_mod` option can be set to use a pool of connections. If a
  pool is used the module must be passed as an option - unless inside a
  `run/3` or `transaction/3` fun and using the run/transaction
  connection reference (`t`).
  """
  defstruct [:pool_mod, :pool_ref, :conn_mod, :conn_ref]

  @typedoc """
  Run or transaction connection reference.
  """
  @type t :: %__MODULE__{pool_mod: module,
                         pool_ref: any,
                         conn_mod: any,
                         conn_ref: reference}
  @type conn :: GenSever.server | t
  @type query :: any
  @type result :: any

  @doc """
  Connect to the databases. Return `{:ok, state}` on success or
  `{:error, exception}` on failure.

  If an error is returned it will be logged and another
  connection attempt will be made after a backoff interval.
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
  """
  @callback checkout(state :: any) ::
    {:ok, new_state :: any} | {:disconnect, Exception.t, new_state :: any}

  @doc """
  Checks in the state to the connection process. Return `{:ok, state}`
  to allow the checkin or `{:disconnect, exception}` to disconnect.

  This callback is called when the control of the state is passed back
  to the connection process. It should reverse any changes made in
  `checkout/2`.
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
  """
  @callback ping(state :: any) ::
    {:ok, new_state :: any} | {:disconnect, Exception.t, new_state :: any}

  @doc """
  Handle a query. Return `{:ok, result, state}` to return the result
  `result` and to continue, `{:error, exception, state}` to return an
  error and to continue and `{:disconnect, exception, state}` to return
  an error and to disconnect.
  """
  @callback handle_query(query, opts :: Keyword.t, state :: any) ::
    {:ok, result, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @doc """
  Handle the beginning of a transaction. Return `{:ok, state}` to
  continue, `{:error, exception, state}` to abort the transaction and
  continue or `{:disconnect, exception, state}` to abort the transaction
  and disconnect.
  """
  @callback handle_begin(opts :: Keyword.t, state :: any) ::
    {:ok, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @doc """
  Handle commiting a transaction. Return `{:ok, state}` on success and
  to continue, `{:error, exception, state}` to abort the transaction and
  continue or `{:disconnect, exception, state}` to abort the transaction
  and disconnect.
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
  """
  @callback handle_rollback(opts :: Keyword.t, state :: any) ::
    {:ok, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @doc """
  Prepare a query with the database. Return `{:ok, query, state}` where
  `query` is a query to pass to `execute/3` or `close/3`,
  `{:error, exception, state}` to return an error and continue or
  `{:disconnect, exception, state}` to return an error and disconnect.

  This callback is intended for cases where the state of a connection is
  needed to prepare a query and/or the query can be saved in the
  database to call later.

  If the connection is not required to prepare a query, `query/3`
  should be used and the query can be prepared by the
  `DBConnection.Query` protocol.
  """
  @callback handle_prepare(query, opts :: Keyword.t, state :: any) ::
    {:ok, query, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @doc """
  Execute a query prepared by `handle_prepare/3`. Return
  `{:ok, result, state}` to return the result `result` and continue,
  `{:error, exception, state}` to return an error and continue or
  `{:disconnect, exception, state{}` to return an error and disconnect.
  """
  @callback handle_execute(query, opts :: Keyword.t, state :: any) ::
    {:ok, result, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @doc """
  Close a query prepared by `handle_prepare/3` with the database. Return
  `{:ok, state}` on success and to continue,
  `{:error, exception, state}` to return an error and continue, or
  `{:disconnect, exception, state}` to return an error and disconnect.
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
  """
  @callback disconnect(err :: Exception.t, state :: any) :: :ok

  @doc """
  Use `DBConnection` to set the behaviour and include default
  implementations for `handle_prepare/3` (no-op), `handle_execute/3`
  (forwards to `handle_query`) and `handle_close/3` (no-op) for
  connections that don't handle the prepare/execute/close pattern.
  `handle_info/2` is also implemented as a no-op.
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

      def handle_query(_, _, _) do
        message = "handle_query/3 not implemented"
        case :erlang.phash2(1, 1) do
          0 -> raise message
          1 -> {:error, RuntimeError.exception(message)}
        end
      end

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

      def handle_execute(query, opts, state) do
        handle_query(query, opts, state)
      end

      def handle_close(_, _, state), do: {:ok, state}

      def handle_info(_, state), do: {:ok, state}

      defoverridable [connect: 1, disconnect: 2, checkout: 1, checkin: 1,
                      ping: 1, handle_query: 3, handle_begin: 2,
                      handle_commit: 2, handle_rollback: 2, handle_prepare: 3,
                      handle_execute: 3, handle_close: 3, handle_info: 2]
    end
  end

  @doc """
  Start and link to a database connection process.

  ### Options

    * `:pool_mod` - The `DBConnection.Pool` module to use, (default:
    `DBConnection.Connection`)
    * `:idle_timeout` - The idle timeout to ping the database (default:
    `15_000`)
    * `:backoff_start` - The first backoff interval (default: `200`)
    * `:backoff_max` - The maximum backoff interval (default: `15_000`)
    * `:backoff_type` - The backoff strategy, `:stop` for no backoff and
    to stop (see `:backoff`, default: `:jitter`)

  ### Example

      {:ok, pid} = DBConnection.start_link(mod, [idle_timeout: 5_000])
  """
  @spec start_link(module, opts :: Keyword.t) :: GenServer.on_start
  def start_link(conn_mod, opts) do
    pool_mod = Keyword.get(opts, :pool_mod, DBConnection.Connection)
    apply(pool_mod, :start_link, [conn_mod, opts])
  end

  @doc """
  Create a supervisor child specification for a pool of connections.

  See `Supervisor.Spec` for child options (`child_opts`).
  """
  @spec child_spec(module, opts :: Keyword.t, child_opts :: Keyword.t) ::
    Supervisor.Spec.spec
  def child_spec(conn_mod, opts, child_opts \\ []) do
    pool_mod = Keyword.get(opts, :pool_mod, DBConnection.Connection)
    apply(pool_mod, :child_spec, [conn_mod, opts, child_opts])
  end

  @doc """
  Run a query with a database connection and returns `{:ok, result}` on
  success or `{:error, exception}` if there was an error.

  ### Options

    * `:queue_timeout` - The time to wait for control of the
    connection's state, if the pool allows setting the timeout per
    request (default: `5_000`)
    * `:queue` - Whether to block waiting in an internal queue for the
    connection's state (boolean, default: `true`)
    * `:timeout` - The maximum time that the caller is allowed the
    to hold the connection's state (ignored when using a run/transaction
    connection, default: `15_000`)
    * `:prepare` - Query prepare method: `:auto` uses the
    `DBConnection.Query` protocol to prepare the query and `:manual`
    does not (default: `:auto`)
    * `:decode` - Result decode method: `:auto` uses the
    `DBConnection.Result` protocol to decode the query and `:manual`
    does not (default: `:auto`)

  The pool and connection module may support other options. All options
  are passed to `handle_query/3`.

  ### Example

      {:ok, _} = DBConnection.query(pid, "SELECT id FROM table", [decode: :manual])
  """
  @spec query(conn, query, opts :: opts :: Keyword.t) ::
    {:ok, result} | {:error, Exception.t}
  def query(conn, query, opts \\ []) do
    query = prepare_query(query, opts)
    decode = Keyword.get(opts, :decode, :auto)
    case handle(conn, :handle_query, query, opts, :result) do
      {:ok, result} when decode == :auto ->
        {:ok, DBConnection.Result.decode(result, opts)}
      other ->
        other
    end
  end

  @doc """
  Run a query with a database connection and return the result. An
  exception is raised on error.

  See `query/3`.
  """
  @spec query!(conn, query, opts :: Keyword.t) :: result
  def query!(conn, query, opts \\ []) do
    case query(conn, query, opts) do
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

    * `:queue_timeout` - The time to wait for control of the connection's
    state, the pool allows setting the timeout per request
    (default: `5_000`)
    * `:queue` - Whether to block waiting in an internal queue for the
    connection's state (boolean, default: `true`)
    * `:timeout` - The maximum time that the caller is allowed the
    to hold the connection's state (ignored when using a run/transaction
    connection, default: `15_000`)
    * `:prepare` - Query prepare method: `:auto` uses the
    `DBConnection.Query` protocol to prepare the query and `:manual`
    does not (default: `:auto`)

  The pool and connection module may support other options. All options
  are passed to `handle_prepare/3`.

  ## Example

      {ok, query}  = DBConnection.prepare(pid, "SELECT id FROM table")
      {:ok, [_|_]} = DBConnection.execute(pid, query)
      :ok          = DBConnection.close(pid, query)
  """
  @spec prepare(conn, query, opts :: Keyword.t) ::
    {:ok, query} | {:error, Exception.t}
  def prepare(conn, query, opts \\ []) do
    query = prepare_query(query, opts)
    handle(conn, :handle_prepare, query, opts, :result)
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
  Execute a prepared query with a database connection and return
  `{:ok, result}` on success or `{:error, exception}` if there was an
  error.

  ### Options

    * `:queue_timeout` - The time to wait for control of the
    connection's state, if the pool allows setting the timeout per
    request (default: `5_000`)
    * `:queue` - Whether to block waiting in an internal queue for the
    connection's state (boolean, default: `true`)
    * `:timeout` - The maximum time that the caller is allowed the
    to hold the connection's state (ignored when using a run/transaction
    connection, default: `15_000`)
    * `:encode` - Query encode method: `:auto` uses the
    `DBConnection.Query` protocol to encode any query parameters and
    `:manual` does not (default: `:auto`)
    * `:decode` - Result decode method: `:auto` uses the
    `DBConnection.Result` protocol to decode the query and `:manual`
    does not (default: `:auto`)

  The pool and connection module may support other options. All options
  are passed to `handle_execute/3`.

  See `prepare/3`.
  """
  @spec execute(conn, query, opts :: Keyword.t) ::
    {:ok, result} | {:error, Exception.t}
  def execute(conn, query, opts) do
    query = encode_query(query, opts)
    decode = Keyword.get(opts, :decode, :auto)
    case handle(conn, :handle_execute, query, opts, :result) do
      {:ok, result} when decode == :auto ->
        {:ok, DBConnection.Result.decode(result, opts)}
      other ->
        other
    end
  end

  @doc """
  Execute a prepared query with a database connection and return the
  result. Raises an exception on error.

  See `execute/3`
  """
  @spec execute!(conn, query, opts :: Keyword.t) :: result
  def execute!(conn, query, opts \\ []) do
    case execute(conn, query, opts) do
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

    * `:queue_timeout` - The time to wait for control of the
    connection's state, if the pool allows setting the timeout per
    request (default: `5_000`)
    * `:queue` - Whether to block waiting in an internal queue for the
    connection's state (boolean, default: `true`)
    * `:timeout` - The maximum time that the caller is allowed the
    to hold the connection's state (ignored when using a run/transaction
    connection, default: `15_000`)

  The pool and connection module may support other options. All options
  are passed to `handle_close/3`.

  See `prepare/3`.
  """
  @spec close(conn, query, opts :: Keyword.t) ::
    :ok | {:error, Exception.t}
  def close(conn, query, opts \\ []) do
    handle(conn, :handle_close, query, opts, :no_result)
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

    * `:queue_timeout` - The time to wait for control of the
    connection's state, if the pool allows setting the timeout per
    request (default: `5_000`)
    * `:queue` - Whether to block waiting in an internal queue for the
    connection's state (boolean, default: `true`)
    * `:timeout` - The maximum time that the caller is allowed the
    to hold the connection's state (default: `15_000`)

  The pool may support other options.

  ### Example

      {:ok, res} = DBConnection.run(pid, fn(conn) ->
        res = DBConnection.query!(conn, "SELECT id FROM table")
        res
      end)
  """
  @spec run(conn, (t -> result), opts :: Keyword.t) ::
    {:ok, result} when result: var
  def run(%DBConnection{} = conn, fun, _) do
    _ = fetch_info(conn)
    {:ok, fun.(conn)}
  end
  def run(pool, fun, opts) do
    {conn, conn_state} = checkout(pool, opts)
    put_info(conn, :idle, conn_state)
    try do
      {:ok, fun.(conn)}
    after
      run_end(conn, opts)
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

  `run/3` and `transaction/3` can be nested multiple times but a
  `transaction/3` call inside another `transaction/3` will be treated
  the same as `run/3`.

  ### Options

    * `:queue_timeout` - The time to wait for control of the
    connection's state, if the pool allows setting the timeout per
    request (default: `5_000`)
    * `:queue` - Whether to block waiting in an internal queue for the
    connection's state (boolean, default: `true`)
    * `:timeout` - The maximum time that the caller is allowed the
    to hold the connection's state (default: `15_000`)

  The pool and connection module may support other options. All options
  are passed to `handle_begin/2`, `handle_commit/2` and
  `handle_rollback/2`.

  ### Example

      {:ok, res} = DBConnection.transaction(pid, fn(conn) ->
        res = DBConnection.query!(conn, "SELECT id FROM table")
        res
      end)
  """
  @spec transaction(conn, (conn -> result), opts :: Keyword.t) ::
    {:ok, result} | {:error, reason :: any} when result: var
  def transaction(%DBConnection{} = conn, fun, opts) do
    case fetch_info(conn) do
      {:transaction, _} ->
        {:ok, fun.(conn)}
      {:idle, conn_state} ->
        transaction_begin(conn, conn_state, fun, opts, :idle)
    end
  end
  def transaction(pool, fun, opts) do
    {conn, conn_state} = checkout(pool, opts)
    transaction_begin(conn, conn_state, fun, opts, :closed)
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
    case fetch_info(conn) do
      {:transaction, _} ->
        throw({:rollback, conn_ref, err})
      {:idle, _} ->
        raise "not inside transaction"
    end
  end

  ## Helpers

  defp checkout(pool, opts) do
    pool_mod = Keyword.get(opts, :pool_mod, DBConnection.Connection)
    case apply(pool_mod, :checkout, [pool, opts]) do
      {:ok, pool_ref, conn_mod, conn_state} ->
        conn = %DBConnection{pool_mod: pool_mod, pool_ref: pool_ref,
          conn_mod: conn_mod, conn_ref: make_ref()}
        {conn, conn_state}
      :error ->
        raise DBConnection.Error, "connection not available"
    end
  end

  defp checkin(conn, conn_state, opts) do
    %DBConnection{pool_mod: pool_mod, pool_ref: pool_ref} = conn
    _ = apply(pool_mod, :checkin, [pool_ref, conn_state, opts])
    :ok
  end

  defp delete_disconnect(conn, conn_state, err, opts) do
    _ = delete_info(conn)
    %DBConnection{pool_mod: pool_mod, pool_ref: pool_ref} = conn
    args = [pool_ref, err, conn_state, opts]
    _ = apply(pool_mod, :disconnect, args)
    :ok
  end

  defp delete_stop(conn, conn_state, :exit, exit, _, opts) do
    delete_stop(conn, conn_state, exit, opts)
  end
  defp delete_stop(conn, conn_state, :error, err, stack, opts) do
    delete_stop(conn, conn_state, {err, stack}, opts)
  end
  defp delete_stop(conn, conn_state, :throw, value, stack, opts) do
    delete_stop(conn, conn_state, {{:nocatch, value}, stack}, opts)
  end

  defp delete_stop(conn, conn_state, reason, opts) do
    _ = delete_info(conn)
    %DBConnection{pool_mod: pool_mod, pool_ref: pool_ref} = conn
    args = [pool_ref, reason, conn_state, opts]
    _ = apply(pool_mod, :stop, args)
    :ok
  end

  defp prepare_query(query, opts) do
    case Keyword.get(opts, :prepare, :auto) do
      :auto   -> DBConnection.Query.prepare(query, opts)
      :manual -> query
    end
  end

  defp encode_query(query, opts) do
    case Keyword.get(opts, :encode, :auto) do
      :auto   -> DBConnection.Query.encode(query, opts)
      :manual -> query
    end
  end

  defp handle(%DBConnection{} = conn, callback, query, opts, return) do
    %DBConnection{conn_mod: conn_mod} = conn
    {status, conn_state} = fetch_info(conn)
    try do
      apply(conn_mod, callback, [query, opts, conn_state])
    else
      {:ok, result, conn_state} when return == :result ->
        put_info(conn, status, conn_state)
        {:ok, result}
      {:ok, conn_state} when return == :no_result ->
        put_info(conn, status, conn_state)
        :ok
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
        delete_stop(conn, conn_state, kind, reason, stack, opts)
        :erlang.raise(kind, reason, stack)
    end
  end

  defp handle(pool, callback, query, opts, return) do
    {conn, conn_state} = checkout(pool, opts)
    %DBConnection{conn_mod: conn_mod} = conn
    try do
      apply(conn_mod, callback, [query, opts, conn_state])
    else
      {:ok, result, conn_state} when return == :result ->
        checkin(conn, conn_state, opts)
        {:ok, result}
      {:ok, conn_state} when return == :no_result ->
        checkin(conn, conn_state, opts)
        :ok
      {:error, _, conn_state} = error ->
        checkin(conn, conn_state, opts)
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
        delete_stop(conn, conn_state, kind, reason, stack, opts)
        :erlang.raise(kind, reason, stack)
    end
  end

  defp run_end(conn, opts) do
    case delete_info(conn) do
      {:idle, conn_state} ->
        checkin(conn, conn_state, opts)
      {:transaction, conn_state} ->
        delete_stop(conn, conn_state, :bad_run, opts)
        raise "connection run ended in transaction"
      :closed ->
        :ok
    end
  end

  defp transaction_begin(conn, conn_state, fun, opts, status) do
    %DBConnection{conn_mod: conn_mod} = conn
    try do
      apply(conn_mod, :handle_begin, [opts, conn_state])
    else
      {:ok, conn_state} ->
        put_info(conn, :transaction, conn_state)
        transaction_run(conn, fun, opts, status)
      {:error, err, conn_state} ->
        checkin(conn, conn_state, opts)
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
        delete_stop(conn, conn_state, kind, reason, stack, opts)
        :erlang.raise(kind, reason, stack)
    end
  end

  defp transaction_run(conn, fun, opts, status) do
    %DBConnection{conn_ref: conn_ref} = conn
    try do
      fun.(conn)
    else
      result ->
        result = {:ok, result}
        transaction_end(conn, :handle_commit, opts, result, status)
    catch
      :throw, {:rollback, ^conn_ref, reason} ->
        result = {:error, reason}
        transaction_end(conn, :handle_rollback, opts, result, status)
      kind, reason ->
        stack = System.stacktrace()
        transaction_end(conn, :handle_rollback, opts, :raise, status)
        :erlang.raise(kind, reason, stack)
    end
  end

  defp transaction_end(conn, callback, opts, result, :closed) do
    case delete_info(conn) do
      {:transaction, conn_state} ->
        transaction_closed(conn, conn_state, callback, opts, result)
      {:idle, conn_state} ->
        delete_stop(conn, conn_state, :bad_transaction, opts)
        raise "not in transaction"
      :closed ->
        result
    end
  end
  defp transaction_end(conn, callback, opts, result, :idle) do
    case get_info(conn) do
      {:transaction, conn_state} ->
        transaction_idle(conn, conn_state, callback, opts, result)
      {:idle, conn_state} ->
        delete_stop(conn, conn_state, :bad_transaction, opts)
        raise "not in transaction"
      :closed ->
        result
    end
  end

  defp transaction_closed(conn, conn_state, callback, opts, result) do
    %DBConnection{conn_mod: conn_mod} = conn
    try do
      apply(conn_mod, callback, [opts, conn_state])
    else
      {:ok, conn_state} ->
        checkin(conn, conn_state, opts)
        result
      {:error, err, conn_state} ->
        checkin(conn, conn_state, opts)
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
        delete_stop(conn, conn_state, kind, reason, stack, opts)
        :erlang.raise(kind, reason, stack)
    end
  end

  defp transaction_idle(conn, conn_state, callback, opts, result) do
    %DBConnection{conn_mod: conn_mod} = conn
    try do
      apply(conn_mod, callback, [opts, conn_state])
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
        delete_stop(conn, conn_state, kind, reason, stack, opts)
        :erlang.raise(kind, reason, stack)
    end
  end

  defp put_info(conn, status, conn_state) do
    _ = Process.put(key(conn), {status, conn_state})
    :ok
  end

  defp fetch_info(conn) do
    case get_info(conn) do
      {_, _} = info -> info
      :closed       -> raise DBConnection.Error, "connection is closed"
    end
  end

  defp get_info(conn), do: Process.get(key(conn), :closed)

  defp delete_info(conn) do
    Process.delete(key(conn)) || :closed
  end

  defp key(%DBConnection{conn_ref: conn_ref}), do: {__MODULE__, conn_ref}
end
