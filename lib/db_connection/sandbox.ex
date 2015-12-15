defmodule DBConnection.Sandbox do
  @moduledoc """
  A behaviour module for implementing a sandbox over a `DBConnection`
  connection.

  To sandbox a connection start `DBConnection` with module
  `DBConnection.Sandbox` and add the option `sandbox_mod: module`, where
  `module` is the module to sandbox. The module must implement both
  the `DBConnection` and `DBConnection.Sandbox` behaviours.

  `DBConnection.Sandbox` callbacks will be used in place of `DBConnection`
  transaction callbacks when sandbox mode is active.
  """

  @behaviour DBConnection

  defstruct [:request]

  @doc """
  Start the sandbox on the connection. Return `{:ok, state}` on sucess to
  continue, `{:error, exception, state}` to abort sandbox mode and continue or
  `{:disconnect, exception, state}` to abort sandbox mode and disconnect.

  This callback is called in the client process.
  """
  @callback sandbox_start(opts :: Keyword.t, state :: any) ::
    {:ok, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @doc """
  Restart the sandbox on the connection. Return `{:ok, state}` on sucess to
  continue, `{:error, exception, state}` to continue the sandbox mode or
  `{:disconnect, exception, state}` to abort sandbox mode and disconnect.

  This callback is called in the client process.
  """
  @callback sandbox_restart(opts :: Keyword.t, state :: any) ::
    {:ok, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @doc """
  Restart the sandbox on the connection. Return `{:ok, state}` on sucess to
  continue, `{:error, exception, state}` to continue the sandbox mode or
  `{:disconnect, exception, state}` to abort sandbox mode and disconnect.

  This callback is called in the client process.
  """
  @callback sandbox_stop(opts :: Keyword.t, state :: any) ::
    {:ok, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @doc """
  Begin a transaction in sandbox mode. Return `{:ok, state}` to continue,
  `{:error, exception, state}` to abort the transaction and continue or
  `{:disconnect, exception, state}` to abort the transaction and disconnect.

  This callback is called in the client process.
  """
  @callback sandbox_begin(opts :: Keyword.t, state :: any) ::
    {:ok, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @doc """
  Pseudo-commit a transaction in sandbox mode. Return `{:ok, state}` on success
  and to continue, `{:error, exception, state}` to abort the transaction and
  continue or `{:disconnect, exception, state}` to abort the transaction and
  disconnect.

  This callback is called in the client process.
  """
  @callback sandbox_commit(opts :: Keyword.t, state :: any) ::
    {:ok, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @doc """
  Rollback a transaction in sandbox mode. Return `{:ok, state}` on success
  and to continue, `{:error, exception, state}` to abort the transaction
  and continue or `{:disconnect, exception, state}` to abort the
  transaction and disconnect.

  A transaction will be rolled back if an exception occurs or
  `rollback/2` is called.

  This callback is called in the client process.
  """
  @callback sandbox_rollback(opts :: Keyword.t, state :: any) ::
    {:ok, new_state :: any} |
    {:error | :disconnect, Exception.t, new_state :: any}

  @doc """
  Start sandbox mode on a connection.

  Will fail if sandbox mode is active or if inside a transaction.
  """
  @spec start_sandbox(DBConnection.conn, Keyword.t) :: :ok
  def start_sandbox(pool, opts \\ []), do: request(pool, :start, opts)

  @doc """
  Restart sandbox mode on a connection.

  Will fail if sandbox mode is not active or if inside a transaction.
  """
  @spec restart_sandbox(DBConnection.conn, Keyword.t) :: :ok
  def restart_sandbox(pool, opts \\ []), do: request(pool, :restart, opts)

  @doc """
  Stop sandbox mode on a connection.

  Will fail inside a transaction when sandbox mode is active.
  """
  @spec stop_sandbox(DBConnection.conn, Keyword.t) :: :ok
  def stop_sandbox(pool, opts \\ []), do: request(pool, :stop, opts)

  @doc false
  def connect(opts) do
    mod = Keyword.fetch!(opts, :sandbox_mod)
    case apply(mod, :connect, [opts]) do
      {:ok, mod_state}    -> {:ok, {mod, :run, mod_state}}
      {:error, _} = error -> error
    end
  end

  @doc false
  def checkout(state), do: handle(state, :checkout, [], :no_result)

  @doc false
  def checkin(state), do: handle(state, :checkin, [], :no_result)

  @doc false
  def ping(state), do: handle(state, :ping, [], :no_result)

  @doc false
  def handle_begin(opts, {_, :run, _} = state) do
    transaction_handle(state, :handle_begin, opts, :transaction)
  end
  def handle_begin(opts, {_, :sandbox, _} = state) do
    transaction_handle(state, :sandbox_begin, opts, :sandbox_transaction)
  end

  @doc false
  def handle_commit(opts, {_, :transaction, _} = state) do
    transaction_handle(state, :handle_commit, opts, :run)
  end
  def handle_commit(opts, {_, :sandbox_transaction, _} = state) do
    transaction_handle(state, :sandbox_commit, opts, :sandbox)
  end

  @doc false
  def handle_rollback(opts, {_, :transaction, _} = state) do
    transaction_handle(state, :handle_rollback, opts, :run)
  end
  def handle_rollback(opts, {_, :sandbox_transaction, _} = state) do
    transaction_handle(state, :sandbox_rollback, opts, :sandbox)
  end

  @doc false
  def handle_prepare(query, opts, state) do
    handle(state, :handle_prepare, [query, opts], :result)
  end

  @doc false
  def handle_execute(query, params, opts, state) do
    handle(state, :handle_execute, [query, params, opts], :execute)
  end

  @doc false
  def handle_execute_close(query, params, opts, state) do
    handle(state, :handle_execute_close, [query, params, opts], :execute)
  end

  @doc false
  def handle_close(%__MODULE__{request: request}, opts, state) do
    handle_request(request, opts, state)
  end
  def handle_close(query, opts, state) do
    handle(state, :handle_close, [query, opts], :no_result)
  end

  @doc false
  def handle_info(msg, state) do
    handle(state, :handle_info, [msg], :no_result)
  end

  @doc false
  def disconnect(err, {mod, status, state}) do
    :ok = apply(mod, :disconnect, [err, state])
    if status in [:sandbox, :sandbox_transaction] do
      raise err
    else
      :ok
    end
  end

  ## Helpers

  defp request(pool, request, opts) do
    DBConnection.close!(pool, %__MODULE__{request: request}, opts)
  end

  defp handle({mod, status, state}, callback, args, return) do
    case apply(mod, callback, args ++ [state]) do
      {:ok, state} when return == :no_result ->
        {:ok, {mod, status, state}}
      {:ok, result, state} when return in [:result, :execute] ->
        {:ok, result, {mod, status, state}}
      {:prepare, state} when return == :execute ->
        {:prepare, {mod, status, state}}
      {error, err, state} when error in [:disconnect, :error] ->
        {error, err, {mod, status, state}}
      other ->
        other
    end
  end

  defp transaction_handle({mod, status, state}, callback, opts, new_status) do
    case apply(mod, callback, [opts, state]) do
      {:ok, state} ->
        {:ok, {mod, new_status, state}}
      {:error, err, state} when status in [:run, :transaction] ->
        {:error, err, {mod, :run, state}}
      {:error, err, state} when status in [:sandbox, :sandbox_transaction] ->
        {:error, err, {mod, :sandbox, state}}
      {:disconnect, err, state} ->
        {:disconnect, err, {mod, status, state}}
      other ->
        other
    end
  end

  defp handle_request(:start, opts, {_, :run, _} = state) do
    sandbox_handle(state, :sandbox_start, opts, :sandbox)
  end
  defp handle_request(:start, _, {_, :transaction, _} = state) do
    err = RuntimeError.exception("can not start sandbox inside transaction")
    {:error, err, state}
  end
  defp handle_request(:start, _, state) do
    err = RuntimeError.exception("sandbox already started")
    {:error, err, state}
  end
  defp handle_request(:restart, opts, {_, :sandbox, _} = state) do
    sandbox_handle(state, :sandbox_restart, opts, :sandbox)
  end
  defp handle_request(:restart, _, {_, :sandbox_transaction, _} = state) do
    err = RuntimeError.exception("can not restart sandbox inside transaction")
    {:error, err, state}
  end
  defp handle_request(:restart, _, state) do
    err = RuntimeError.exception("sandbox not started")
    {:error, err, state}
  end
  defp handle_request(:stop, opts, {_, :sandbox, _} = state) do
    sandbox_handle(state, :sandbox_stop, opts, :run)
  end
  defp handle_request(:stop, _, {_, :sandbox_transaction, _} = state) do
    err = RuntimeError.exception("can not stop sandbox inside transaction")
    {:error, err, state}
  end
  defp handle_request(:stop, _, state) do
    {:ok, state}
  end

  defp sandbox_handle({mod, status, state}, callback, opts, new_status) do
    case apply(mod, callback, [opts, state]) do
      {:ok, state} ->
        {:ok, {mod, new_status, state}}
      {error, err, state} when error in [:disconnect, :error] ->
        {:disconnect, err, {mod, status, state}}
      other ->
        other
    end
  end
end
