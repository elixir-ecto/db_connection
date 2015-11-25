defmodule DBConnection do

  defstruct [:pool_mod, :pool_ref, :conn_mod, :conn_ref]

  @type t :: %__MODULE__{pool_mod: module,
                         pool_ref: any,
                         conn_mod: any,
                         conn_ref: reference}
  @type conn :: GenSever.server | t
  @type query :: any
  @type result :: any

  @callback connect(Keyword.t) :: {:ok, state :: any} | {:error, Exception.t}

  @callback checkout(state :: any) ::
    {:ok, new_state :: any} | {:disconnect, Exception.t, new_state :: any}

  @callback checkin(state :: any) ::
    {:ok, new_state :: any} | {:disconnect, Exception.t, new_state :: any}

  @callback ping(state :: any) ::
    {:ok, new_state :: any} | {:disconnect, Exception.t, new_state :: any}

  @callback handle_query(query, Keyword.t, state :: any) ::
    {:ok, result, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @callback handle_begin(Keyword.t, state :: any) ::
    {:ok, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @callback handle_commit(Keyword.t, state :: any) ::
    {:ok, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @callback handle_rollback(Keyword.t, state :: any) ::
    {:ok, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @callback handle_prepare(query, Keyword.t, state :: any) ::
    {:ok, query, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @callback handle_execute(query, Keyword.t, state :: any) ::
    {:ok, result, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @callback handle_close(query, Keyword.t, state :: any) ::
    {:ok, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @callback disconnect(state :: any) :: :ok

  @spec start_link(module, Keyword.t) :: GenServer.on_start
  def start_link(conn_mod, opts) do
    pool_mod = Keyword.get(opts, :pool_mod, DBConnection.Connection)
    apply(pool_mod, :start_link, [conn_mod, opts])
  end

  @spec child_spec(module, Keyword.t, Keyword.t) :: Supervisor.Spec.spec
  def child_spec(conn_mod, opts, child_opts \\ []) do
    pool_mod = Keyword.get(opts, :pool_mod, DBConnection.Connection)
    apply(pool_mod, :child_spec, [conn_mod, opts, child_opts])
  end

  @spec query(conn, query, Keyword.t) ::
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

  @spec query!(conn, query, Keyword.t) :: result
  def query!(conn, query, opts \\ []) do
    case query(conn, query, opts) do
      {:ok, result} -> result
      {:error, err} -> raise err
    end
  end

  @spec prepare(conn, query, Keyword.t) ::
    {:ok, query} | {:error, Exception.t}
  def prepare(conn, query, opts \\ []) do
    query = prepare_query(query, opts)
    handle(conn, :handle_prepare, query, opts, :result)
  end

  @spec prepare!(conn, query, Keyword.t) :: query
  def prepare!(conn, query, opts) do
    case prepare(conn, query, opts) do
      {:ok, result} -> result
      {:error, err} -> raise err
    end
  end

  @spec execute(conn, query, Keyword.t) ::
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

  @spec execute!(conn, query, Keyword.t) :: result
  def execute!(conn, query, opts \\ []) do
    case execute(conn, query, opts) do
      {:ok, result} -> result
      {:error, err} -> raise err
    end
  end

  @spec close(conn, query, Keyword.t) ::
    :ok | {:error, Exception.t}
  def close(conn, query, opts \\ []) do
    handle(conn, :handle_close, query, opts, :no_result)
  end

  @spec close!(conn, query, Keyword.t) :: :ok
  def close!(conn, query, opts \\ []) do
    case close(conn, query, opts) do
      :ok -> :ok
      {:error, err} -> raise err
    end
  end

  @spec run(conn, (t -> result), Keyword.t) ::
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

  @spec transaction(conn, (conn -> result), Keyword.t) ::
    {:ok, result} | {:error, reason :: any} when result: var
  def transaction(%DBConnection{} = conn, fun, opts) do
    case fetch_info(conn) do
      {:transaction, _} ->
        fun.(conn)
      {:idle, conn_state} ->
        transaction_begin(conn, conn_state, fun, opts, :idle)
    end
  end
  def transaction(pool, fun, opts) do
    {conn, conn_state} = checkout(pool, opts)
    transaction_begin(conn, conn_state, fun, opts, :closed)
  end

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
    pool_mod = Keyword.get(opts, :pool_mod, DBConnection)
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
        stack = Systen.stacktrace()
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
      {disconnect, err, conn_state} when disconnect in [:error, :disconnect] ->
        delete_disconnect(conn, conn_state, err, opts)
        raise err
       other ->
        delete_stop(conn, conn_state, {:bad_return_value, other}, opts)
        raise DBConnection.Error, "bad return value: #{inspect other}"
    catch
      kind, reason ->
        stack = System.stackrace()
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
      {disconnect, err, conn_state} when disconnect in [:error, :disconnect] ->
        delete_disconnect(conn, conn_state, err, opts)
        raise err
       other ->
        delete_stop(conn, conn_state, {:bad_return_value, other}, opts)
        raise DBConnection.Error, "bad return value: #{inspect other}"
    catch
      kind, reason ->
        stack = System.stackrace()
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
      {disconnect, err, conn_state} when disconnect in [:error, :disconnect] ->
        delete_disconnect(conn, conn_state, err, opts)
        raise err
       other ->
         delete_stop(conn, conn_state, {:bad_return_value, other}, opts)
         raise DBConnection.Error, "bad return value: #{inspect other}"
    catch
      kind, reason ->
        stack = System.stackrace()
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
    Process.delete(conn) || :closed
  end

  defp key(%DBConnection{conn_ref: conn_ref}), do: {__MODULE__, conn_ref}
end
