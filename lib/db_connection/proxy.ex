defmodule DBConnection.Proxy do
  @moduledoc """
  A behaviour module for implementing a proxy module during the check out of a
  connection.

  `DBConnection.Proxy` callback modules can wrap a `DBConnection` callback
  module and state while it is outside the pool.
  """

  @doc """
  Setup the initial state of the proxy. Return `{:ok, state}` to continue,
  `:ignore` not to use the proxy or `{:error, exception}` to raise an exception.

  This callback is called before checking out a connection from the pool.
  """
  @callback init(Keyword.t) ::
    {:ok, state :: any} | :ignore | {:error, Exception.t}

  @doc """
  Checks out the connection state to the proxy module. Return
  `{:ok, conn, state}` to allow the checkout and continue,
  `{:error, exception, conn, state}` to disallow the checkout and to raise an
  exception or `{:disconnect, exception, conn, state}` to disconnect the
  connection and raise an exception.

  This callback is called after the connections `checkout/1` callback and should
  setup the connection state for use by the proxy module.
  """
  @callback checkout(module, Keyword.t, conn :: any, state :: any) ::
    {:ok, new_conn :: any, new_state :: any} |
    {:error | :disconnect, Exception.t, new_conn :: any, new_state :: any}

  @doc """
  Checks in the connection state so it can be checked into the pool. Return
  `{:ok, conn}` to allow the checkin and continue,
  `{:error, exception, conn, state}` to allow the checkin but raise an
  exception or `{:disconnect, exception, conn, state}` to disconnect the
  connection and raise an exception.

  This callback is called before the connections `checkin/1` and should undo
  any changes made to the connection in `checkout/3`.
  """
  @callback checkin(module, Keyword.t, conn :: any, state :: any) ::
    {:ok, new_conn :: any, new_state :: any} |
    {:error | :disconnect, Exception.t, new_conn :: any, new_state :: any}

  @doc """
  Handle the beginning of a transaction. Return `{:ok, conn, state}` to
  continue, `{:error, exception, conn, state}` to abort the transaction and
  continue or `{:disconnect, exception, conn, state}` to abort the transaction
  and disconnect the connection.
  """
  @callback handle_begin(module, opts :: Keyword.t, conn :: any,
  state :: any) ::
    {:ok, new_conn :: any, new_state :: any} |
    {:error | :disconnect, Exception.t, new_conn :: any, new_state :: any}

  @doc """
  Handle commiting a transaction. Return `{:ok, conn, state}` on success and
  to continue, `{:error, exception, conn, state}` to abort the transaction and
  continue or `{:disconnect, exception, conn, state}` to abort the transaction
  and disconnect the connection.
  """
  @callback handle_commit(module, opts :: Keyword.t, conn :: any,
  state :: any) ::
    {:ok, new_conn :: any, new_state :: any} |
    {:error | :disconnect, Exception.t, new_conn :: any, new_state :: any}

  @doc """
  Handle rolling back a transaction. Return `{:ok, conn, state}` on success
  and to continue, `{:error, exception, conn, state}` to abort the transaction
  and continue or `{:disconnect, exception, conn, state}` to abort the
  transaction and disconnect.
  """
  @callback handle_rollback(module, opts :: Keyword.t, conn :: any,
  state :: any) ::
    {:ok, new_conn :: any, new_state :: any} |
    {:error | :disconnect, Exception.t, new_conn :: any, new_state :: any}

  @doc """
  Prepare a query with the database. Return `{:ok, query, conn, state}` where
  `query` is a query to pass to `execute/4` or `close/3`,
  `{:error, exception, conn, state}` to return an error and continue or
  `{:disconnect, exception, conn, state}` to return an error and disconnect the
  connection.
  """
  @callback handle_prepare(module, DBConnection.query, opts :: Keyword.t,
  conn :: any, state :: any) ::
    {:ok, DBConnection.query, new_conn :: any, new_state :: any} |
    {:error | :disconnect, Exception.t, new_conn :: any, new_state :: any}

  @doc """
  Execute a query. Return `{:ok, result, conn, state}` to return the result
  `result` and continue, `{:prepare, conn, state}` to retry execute after
  preparing the query, `{:error, exception, conn, state}` to return an error and
  continue or `{:disconnect, exception, conn, state}` to return an error and
  disconnect the connection.
  """
  @callback handle_execute(module, DBConneciton.query, DBConnection.params,
  opts :: Keyword.t, conn :: any, state :: any) ::
    {:ok, DBConnection.result, new_conn :: any, new_state :: any} |
    {:prepare, new_conn :: any, new_state :: any} |
    {:error | :disconnect, Exception.t, new_conn :: any, new_state :: any}

  @doc """
  Execute a query and close it. See `handle_execute/6`.
  """
  @callback handle_execute_close(module, DBConneciton.query,
  DBConnection.params, opts :: Keyword.t, conn :: any, state :: any) ::
    {:ok, DBConnection.result, new_conn :: any, new_state :: any} |
    {:prepare, new_conn :: any, new_state :: any} |
    {:error | :disconnect, Exception.t, new_conn :: any, new_state :: any}

  @doc """
  Close a query. Return `{:ok, conn, state}` on success and to continue,
  `{:error, exception, conn, state}` to return an error and continue, or
  `{:disconnect, exception, conn, state}` to return an error and disconnect.
  """
  @callback handle_close(module, DBConnection.query, opts :: Keyword.t,
  conn :: any, state :: any) ::
    {:ok, new_conn :: any, new_state :: any} |
    {:error | :disconnect, Exception.t, new_conn :: any, new_state :: any}

  @doc """
  Terminate the proxy. Should cleanup any side effects as process may not exit.

  This callback is called after checking in a connection to the pool.
  """
  @callback terminate(:normal | {:disconnect, Exception.t} | {:stop, any},
  opts :: Keyword.t, state :: any) :: any

  @doc """
  Use `DBConnection.Proxy` to set the behaviour and include default
  implementations. The default implementation of `init/1` stores
  the checkout options as the proxy's state. `checkout/4`, `checkin/4` and
  `terminate/3` act as no-ops. The remaining callbacks call the internal
  connection module with the given arguments and state.
  """
  defmacro __using__(_) do
    quote location: :keep do
      @behaviour DBConnection.Proxy

      def init(opts), do: {:ok, opts}

      def checkout(_, opts, conn, state), do: {:ok, conn, state}

      def checkin(_, _, conn, _), do: {:ok, conn}

      def handle_begin(mod, opts, conn, state) do
        case apply(mod, :handle_begin, [opts, conn]) do
          {:ok, _} = ok ->
            :erlang.append_element(ok, state)
          {tag, _, _} = error when tag in [:error, :disconnect] ->
            :erlang.append_element(error, state)
          other ->
            raise DBConnection.Error, "bad return value: #{inspect other}"
        end
      end

      def handle_commit(mod, opts, conn, state) do
        case apply(mod, :handle_commit, [opts, conn]) do
          {:ok, _} = ok ->
            :erlang.append_element(ok, state)
          {tag, _, _} = error when tag in [:error, :disconnect] ->
            :erlang.append_element(error, state)
          other ->
            raise DBConnection.Error, "bad return value: #{inspect other}"
        end
      end

      def handle_rollback(mod, opts, conn, state) do
        case apply(mod, :handle_rollback, [opts, conn]) do
          {:ok, _} = ok ->
            :erlang.append_element(ok, state)
          {tag, _, _} = error when tag in [:error, :disconnect] ->
            :erlang.append_element(error, state)
          other ->
            raise DBConnection.Error, "bad return value: #{inspect other}"
        end
      end

      def handle_prepare(mod, query, opts, conn, state) do
        case apply(mod, :handle_prepare, [query, opts, conn]) do
          {:ok, _, _} = ok ->
            :erlang.append_element(ok, state)
          {tag, _, _} = error when tag in [:error, :disconnect] ->
            :erlang.append_element(error, state)
          other ->
            raise DBConnection.Error, "bad return value: #{inspect other}"
        end
      end

      def handle_execute(mod, query, params, opts, conn, state) do
        case apply(mod, :handle_execute, [query, params, opts, conn]) do
          {:ok, _, _} = ok ->
            :erlang.append_element(ok, state)
          {:prepare, _} = prepare ->
            :erlang.append_element(prepare, state)
          {tag, _, _} = error when tag in [:error, :disconnect] ->
            :erlang.append_element(error, state)
          other ->
            raise DBConnection.Error, "bad return value: #{inspect other}"
        end
      end

      def handle_execute_close(mod, query, params, opts, conn, state) do
        case apply(mod, :handle_execute_close, [query, params, opts, conn]) do
          {:ok, _, _} = ok ->
            :erlang.append_element(ok, state)
          {:prepaere, _} = prepare ->
            :erlang.append_element(prepare, state)
          {tag, _, _} = error when tag in [:error, :disconnect] ->
            :erlang.append_element(error, state)
          other ->
            raise DBConnection.Error, "bad return value: #{inspect other}"
        end
      end

      def handle_close(mod, query, opts, conn, state) do
        case apply(mod, :handle_close, [query, opts, conn]) do
          {:ok, _} = ok ->
            :erlang.append_element(ok, state)
          {tag, _, _} = error when tag in [:error, :disconnect] ->
            :erlang.append_element(error, state)
          other ->
            raise DBConnection.Error, "bad return value: #{inspect other}"
        end
      end

      def terminate(_, _, _), do: :ok

      defoverridable [init: 1, checkout: 4, checkin: 4, handle_begin: 4,
                      handle_commit: 4, handle_rollback: 4, handle_prepare: 5,
                      handle_execute: 6, handle_execute_close: 6,
                      handle_close: 5, terminate: 3]
    end
  end
end
