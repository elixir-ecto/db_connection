defmodule DBConnection.Proxy do
  @moduledoc """
  A behaviour module for implementing a proxy module during the check out of a
  connection.

  `DBConnection.Proxy` callback modules can wrap a `DBConnection` callback
  module and state while it is outside the pool.
  """
  @doc """
  Checks out the connection state and creates the proxy state. Return
  `{:ok, conn, state}` to allow the checkout and continue,
  `{:error, exception, conn}` to disallow the checkout and to raise an exception
  or `{:disconnect, exception, conn}` to disconnect the connection and raise an
  exception.

  This callback is called after the connections `checkout/1` callback and should
  setup the state for the proxy module.
  """
  @callback checkout(module, Keyword.t, conn :: any) ::
    {:ok, new_conn :: any, state :: any} |
    {:error | :disconnect, Exception.t, new_conn :: any}

  @doc """
  Checks in the connection state so it can be checked into the pool. Return
  `{:ok, conn}` to allow the checkin and continue, `{:error, exception, conn}`
  to allow the checkin but raise an exception or
  `{:disconnect, exception, conn}` to disconnect the connection and raise an
  exception.

  This callback is called before the connections `checkin/1`  and should undo
  any changes made to the connection in `checkout/3`.
  """
  @callback checkin(module, Keyword.t, conn :: any, state :: any) ::
    {:ok, new_conn :: any} |
    {:error | :disconnect, Exception.t, new_conn :: any}

  @doc """
  Handle the beginning of a transaction. Return `{:ok, conn, state}` to
  continue, `{:error, exception, conn, state}` to abort the transaction and
  continue or `{:disconnect, exception, conn}` to abort the transaction
  and disconnect the connection.
  """
  @callback handle_begin(module, opts :: Keyword.t, conn :: any ,
  state :: any) ::
    {:ok, new_conn :: any, new_state :: any} |
    {:error, Exception.t, new_conn :: any, new_state :: any} |
    {:disconnect, Exception.t, new_conn :: any}

  @doc """
  Handle commiting a transaction. Return `{:ok, conn, state}` on success and
  to continue, `{:error, exception, conn, state}` to abort the transaction and
  continue or `{:disconnect, exception, conn}` to abort the transaction
  and disconnect the connection.
  """
  @callback handle_commit(module, opts :: Keyword.t, conn :: any ,
  state :: any) ::
    {:ok, new_conn :: any, new_state :: any} |
    {:error, Exception.t, new_conn :: any, new_state :: any} |
    {:disconnect, Exception.t, new_conn :: any}

  @doc """
  Handle rolling back a transaction. Return `{:ok, conn, state}` on success
  and to continue, `{:error, exception, conn, state}` to abort the transaction
  and continue or `{:disconnect, exception, conn}` to abort the
  transaction and disconnect.
  """
  @callback handle_rollback(module, opts :: Keyword.t, conn :: any ,
  state :: any) ::
    {:ok, new_conn :: any, new_state :: any} |
    {:error, Exception.t, new_conn :: any, new_state :: any} |
    {:disconnect, Exception.t, new_conn :: any}

  @doc """
  Prepare a query with the database. Return `{:ok, query, conn, state}` where
  `query` is a query to pass to `execute/4` or `close/3`,
  `{:error, exception, conn, state}` to return an error and continue or
  `{:disconnect, exception, conn}` to return an error and disconnect the
  connection.
  """
  @callback handle_prepare(module, DBConnection.query, opts :: Keyword.t,
  conn :: any, state :: any) ::
    {:ok, DBConnection.query, new_conn :: any, new_state :: any} |
    {:error, Exception.t, new_conn :: any, new_state :: any} |
    {:disconnect, Exception.t, new_conn :: any}

  @doc """
  Execute a query. Return `{:ok, result, conn, state}` to return the result
  `result` and continue, `{:prepare, conn, state}` to retry execute after
  preparing the query, `{:error, exception, conn, state}` to return an error and
  continue or `{:disconnect, exception, conn}` to return an error and
  disconnect the connection.
  """
  @callback handle_execute(module, DBConneciton.query, DBConnection.params,
  opts :: Keyword.t, conn :: any, state :: any) ::
    {:ok, DBConnection.result, new_conn :: any, new_state :: any} |
    {:prepare, new_conn :: any, new_state :: any} |
    {:error, Exception.t, new_conn :: any, new_state :: any} |
    {:disconnect, Exception.t, new_conn :: any}

  @doc """
  Execute a query and close it. See `handle_execute/6`.
  """
  @callback handle_execute_close(module, DBConneciton.query,
  DBConnection.params, opts :: Keyword.t, conn :: any, state :: any) ::
    {:ok, DBConnection.result, new_conn :: any, new_state :: any} |
    {:prepare, new_conn :: any, new_state :: any} |
    {:error, Exception.t, new_conn :: any, new_state :: any} |
    {:disconnect, Exception.t, new_conn :: any}

  @doc """
  Close a query. Return `{:ok, conn, state}` on success and to continue,
  `{:error, exception, conn, state}` to return an error and continue, or
  `{:disconnect, exception, conn}` to return an error and disconnect.
  """
  @callback handle_close(module, DBConnection.query, opts :: Keyword.t,
  conn :: any, state :: any) ::
    {:ok, new_conn :: any, new_state :: any} |
    {:error, Exception.t, new_conn :: any, new_state :: any} |
    {:disconnect, Exception.t, new_conn :: any}

  @doc """
  Use `DBConnection.Proxy` to set the behaviour and include default
  implementations. The default implementation of `checkout/3` stores
  the checkout options as the proxy's state, `checkin/4` acts a no-op
  and the remaining callbacks call the internal connection module with
  the given arguments and state.
  """
  defmacro __using__(_) do
    quote location: :keep do
      @behaviour DBConnection.Proxy

      def checkout(_, opts, conn), do: {:ok, conn, opts}

      def checkin(_, _, conn, _), do: {:ok, conn}

      def handle_begin(mod, opts, conn, state) do
        case apply(mod, :handle_begin, [opts, conn]) do
          {:ok, _} = ok ->
            Tuple.append(ok, state)
          {:error, _, _} = error ->
            Tuple.append(error, state)
          {:disconnect, _, _} = discon ->
            discon
          other ->
            raise DBConnection.Error, "bad return value: #{inspect other}"
        end
      end

      def handle_commit(mod, opts, conn, state) do
        case apply(mod, :handle_commit, [opts, conn]) do
          {:ok, _} = ok ->
            Tuple.append(ok, state)
          {:error, _, _} = error ->
            Tuple.append(error, state)
          {:disconnect, _, _} = discon ->
            discon
          other ->
            raise DBConnection.Error, "bad return value: #{inspect other}"
        end
      end

      def handle_rollback(mod, opts, conn, state) do
        case apply(mod, :handle_rollback, [opts, conn]) do
          {:ok, _} = ok ->
            Tuple.append(ok, state)
          {:error, _, _} = error ->
            Tuple.append(error, state)
          {:disconnect, _, _} = discon ->
            discon
          other ->
            raise DBConnection.Error, "bad return value: #{inspect other}"
        end
      end

      def handle_prepare(mod, query, opts, conn, state) do
        case apply(mod, :handle_prepare, [query, opts, conn]) do
          {:ok, _, _} = ok ->
            Tuple.append(ok, state)
          {:error, _, _} = error ->
            Tuple.append(error, state)
          {:disconnect, _, _} = discon ->
            discon
          other ->
            raise DBConnection.Error, "bad return value: #{inspect other}"
        end
      end

      def handle_execute(mod, query, params, opts, conn, state) do
        case apply(mod, :handle_execute, [query, params, opts, conn]) do
          {:ok, _, _} = ok ->
            Tuple.append(ok, state)
          {:prepare, _} = prepare ->
            Tuple.append(prepare, state)
          {:error, _, _} = error ->
            Tuple.append(error, state)
          {:disconnect, _, _} = discon ->
            discon
          other ->
            raise DBConnection.Error, "bad return value: #{inspect other}"
        end
      end

      def handle_execute_close(mod, query, params, opts, conn, state) do
        case apply(mod, :handle_execute_close, [query, params, opts, conn]) do
          {:ok, _, _} = ok ->
            Tuple.append(ok, state)
          {:prepaere, _} = prepare ->
            Tuple.append(prepare, state)
          {:error, _, _} = error ->
            Tuple.append(error, state)
          {:disconnect, _, _} = discon ->
            discon
          other ->
            raise DBConnection.Error, "bad return value: #{inspect other}"
        end
      end

      def handle_close(mod, query, opts, conn, state) do
        case apply(mod, :handle_close, [query, opts, conn]) do
          {:ok, _} = ok ->
            Tuple.append(ok, state)
          {:error, _, _} = error ->
            Tuple.append(error, state)
          {:disconnect, _, _} = discon ->
            discon
          other ->
            raise DBConnection.Error, "bad return value: #{inspect other}"
        end
      end

      defoverridable [checkout: 3, checkin: 4, handle_begin: 4,
                      handle_commit: 4, handle_rollback: 4, handle_prepare: 5,
                      handle_execute: 6, handle_execute_close: 6,
                      handle_close: 5]
    end
  end
end
