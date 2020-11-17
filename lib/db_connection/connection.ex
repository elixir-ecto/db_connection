defmodule DBConnection.ConnectionError do
  defexception [:message, severity: :error, reason: :error]

  @moduledoc """
  The raised exception might include the reason which would be useful
  to programatically determine what was causing the error.
  """

  @doc false
  def exception(message, reason) do
    message
    |> exception()
    |> Map.replace!(:reason, reason)
  end
end

defmodule DBConnection.Connection do
  @moduledoc false

  use Connection
  require Logger
  alias DBConnection.Backoff
  alias DBConnection.Holder

  @timeout 15_000

  @doc false
  def start_link(mod, opts, pool, tag) do
    start_opts = Keyword.take(opts, [:debug, :spawn_opt])
    Connection.start_link(__MODULE__, {mod, opts, pool, tag}, start_opts)
  end

  @doc false
  def child_spec(mod, opts, pool, tag, child_opts) do
    Supervisor.child_spec(
      %{id: __MODULE__, start: {__MODULE__, :start_link, [mod, opts, pool, tag]}},
      child_opts
    )
  end

  @doc false
  def disconnect({pid, ref}, err, state) do
    Connection.cast(pid, {:disconnect, ref, err, state})
  end

  @doc false
  def stop({pid, ref}, err, state) do
    Connection.cast(pid, {:stop, ref, err, state})
  end

  @doc false
  def ping({pid, ref}, state) do
    Connection.cast(pid, {:ping, ref, state})
  end

  ## Connection API

  @doc false
  def init({mod, opts, pool, tag}) do
    s = %{
      mod: mod,
      opts: opts,
      state: nil,
      client: :closed,
      pool: pool,
      tag: tag,
      timer: nil,
      backoff: Backoff.new(opts),
      connection_listeners: Keyword.get(opts, :connection_listeners, []),
      after_connect: Keyword.get(opts, :after_connect),
      after_connect_timeout: Keyword.get(opts, :after_connect_timeout, @timeout)
    }

    {:connect, :init, s}
  end

  @doc false
  def connect(_, s) do
    %{mod: mod, opts: opts, backoff: backoff, after_connect: after_connect} = s

    try do
      apply(mod, :connect, [connect_opts(opts)])
    rescue
      e ->
        {e, stack} = maybe_sanitize_exception(e, __STACKTRACE__, opts)
        reraise e, stack
    else
      {:ok, state} when after_connect != nil ->
        ref = make_ref()
        Connection.cast(self(), {:after_connect, ref})
        {:ok, %{s | state: state, client: {ref, :connect}}}

      {:ok, state} ->
        backoff = backoff && Backoff.reset(backoff)
        ref = make_ref()
        Connection.cast(self(), {:connected, ref})
        {:ok, %{s | state: state, client: {ref, :connect}, backoff: backoff}}

      {:error, err} when is_nil(backoff) ->
        raise err

      {:error, err} ->
        Logger.error(fn() ->
          [inspect(mod), ?\s, ?(, inspect(self()), ") failed to connect: " |
            Exception.format_banner(:error, err, [])]
        end, crash_reason: {err, []})
        {timeout, backoff} = Backoff.backoff(backoff)
        {:backoff, timeout, %{s | backoff: backoff}}
    end
  end

  defp maybe_sanitize_exception(e, stack, opts) do
    if Keyword.get(opts, :show_sensitive_data_on_connection_error, false) do
      {e, stack}
    else
      message =
        "connect raised #{inspect(e.__struct__)} exception#{sanitized_message(e)}. " <>
          "The exception details are hidden, as they may contain sensitive data such as " <>
          "database credentials. You may set :show_sensitive_data_on_connection_error " <>
          "to true when starting your connection if you wish to see all of the details"

      {RuntimeError.exception(message), cleanup_stacktrace(stack)}
    end
  end

  defp sanitized_message(%KeyError{} = e), do: ": #{Exception.message(%{e | term: nil})}"
  defp sanitized_message(_), do: ""

  @doc false
  def disconnect({log, err}, %{mod: mod} = s) do
    if log == :log do
      severity =
        case err do
          %DBConnection.ConnectionError{severity: severity} -> severity
          _ -> :error
        end

      Logger.log(severity, fn() ->
        [inspect(mod), ?\s, ?(, inspect(self()),
          ") disconnected: " | Exception.format_banner(:error, err, [])]
      end)

      :ok
    end

    %{state: state, client: client, timer: timer, backoff: backoff} = s
    demonitor(client)
    cancel_timer(timer)
    :ok = apply(mod, :disconnect, [err, state])
    s = %{s | state: nil, client: :closed, timer: nil}

    notify_connection_listeners({:disconnected, self()}, s)

    case client do
      _ when backoff == :nil ->
        {:stop, {:shutdown, err}, s}

      {_, :after_connect} ->
        {timeout, backoff} = Backoff.backoff(backoff)
        {:backoff, timeout, %{s | backoff: backoff}}

      _ ->
        {:connect, :disconnect, s}
    end
  end

  @doc false
  def handle_cast({:ping, ref, state}, %{client: {ref, :pool}, mod: mod} = s) do
    case apply(mod, :ping, [state]) do
      {:ok, state} ->
        pool_update(state, s)

      {:disconnect, err, state} ->
        {:disconnect, {:log, err}, %{s | state: state}}
    end
  end

  def handle_cast({:disconnect, ref, err, state}, %{client: {ref, _}} = s) do
    {:disconnect, {:log, err}, %{s | state: state}}
  end

  def handle_cast({:stop, ref, err, state}, %{client: {ref, _}} = s) do
    {_, stack} = :erlang.process_info(self(), :current_stacktrace)
    {:stop, {err, stack}, %{s | state: state}}
  end

  def handle_cast({tag, _, _, _}, s) when tag in [:disconnect, :stop] do
    handle_timeout(s)
  end

  def handle_cast({:after_connect, ref}, %{client: {ref, :connect}} = s) do
    %{mod: mod, state: state, after_connect: after_connect,
      after_connect_timeout: timeout, opts: opts} = s

    notify_connection_listeners({:connected, self()}, s)

    case apply(mod, :checkout, [state]) do
      {:ok, state} ->
        opts = [timeout: timeout] ++ opts
        {pid, ref} = DBConnection.Task.run_child(mod, state, after_connect, opts)
        timer = start_timer(pid, timeout)
        s = %{s | client: {ref, :after_connect}, timer: timer, state: state}
        {:noreply, s}
      {:disconnect, err, state} ->
        {:disconnect, {:log, err}, %{s | state: state}}
    end
  end

  def handle_cast({:after_connect, _}, s) do
    {:noreply, s}
  end

  def handle_cast({:connected, ref}, %{client: {ref, :connect}} = s) do
    %{mod: mod, state: state} = s

    notify_connection_listeners({:connected, self()}, s)

    case apply(mod, :checkout, [state]) do
      {:ok, state} ->
        pool_update(state, s)
      {:disconnect, err, state} ->
        {:disconnect, {:log, err}, %{s | state: state}}
    end
  end

  def handle_cast({:connected, _}, s) do
    {:noreply, s}
  end

  @doc false
  def handle_info({:DOWN, ref, _, pid, reason}, %{client: {ref, :after_connect}} = s) do
    message = "client #{inspect pid} exited: " <> Exception.format_exit(reason)
    err = DBConnection.ConnectionError.exception(message)
    {:disconnect, {down_log(reason), err}, %{s | client: {nil, :after_connect}}}
  end
  def handle_info({:DOWN, mon, _, pid, reason}, %{client: {ref, mon}} = s) do
    message = "client #{inspect pid} exited: " <> Exception.format_exit(reason)
    err = DBConnection.ConnectionError.exception(message)
    {:disconnect, {down_log(reason), err}, %{s | client: {ref, nil}}}
  end

  def handle_info({:timeout, timer, {__MODULE__, pid, timeout}}, %{timer: timer} = s)
      when is_reference(timer) do
    message =
      "client #{inspect pid} timed out because it checked out " <>
        "the connection for longer than #{timeout}ms"

    exc = case Process.info(pid, :current_stacktrace) do
            {:current_stacktrace, stacktrace} ->
              message <> "\n\n#{inspect pid} was at location:\n\n" <>
                Exception.format_stacktrace(stacktrace)
            _ ->
              message
          end
          |> DBConnection.ConnectionError.exception()

    {:disconnect, {:log, exc}, %{s | timer: nil}}
  end

  def handle_info(:timeout, %{client: nil} = s) do
    %{mod: mod, state: state} = s
    case apply(mod, :ping, [state]) do
      {:ok, state} ->
        handle_timeout(%{s | state: state})
      {:disconnect, err, state} ->
        {:disconnect, {:log, err}, %{s | state: state}}
    end
  end

  def handle_info(
        {:"ETS-TRANSFER", holder, _pid, {msg, ref, extra}},
        %{client: {ref, :after_connect}, timer: timer} = s
      ) do
    {_, state} = Holder.delete(holder)
    cancel_timer(timer)
    s = %{s | timer: nil}

    case msg do
      :checkin -> handle_checkin(state, s)
      :disconnect -> handle_cast({:disconnect, ref, extra, state}, s)
      :stop -> handle_cast({:stop, ref, extra, state}, s)
    end
  end

  def handle_info(msg, %{mod: mod} = s) do
    Logger.info(fn() ->
      [inspect(mod), ?\s, ?(, inspect(self()), ") missed message: " | inspect(msg)]
    end)

    handle_timeout(s)
  end

  @doc false
  def format_status(info, [_, %{client: :closed, mod: mod}]) do
    case info do
      :normal    -> [{:data, [{'Module', mod}]}]
      :terminate -> mod
    end
  end
  def format_status(info, [pdict, %{mod: mod, state: state}]) do
    case function_exported?(mod, :format_status, 2) do
      true when info == :normal ->
        normal_status(mod, pdict, state)

      false when info == :normal ->
        normal_status_default(mod, state)

      true when info == :terminate ->
        {mod, terminate_status(mod, pdict, state)}

      false when info == :terminate ->
        {mod, state}
    end
  end

  ## Helpers

  defp connect_opts(opts) do
    case Keyword.get(opts, :configure) do
      {mod, fun, args} ->
        apply(mod, fun, [opts | args])
      fun when is_function(fun, 1) ->
        fun.(opts)
      nil ->
        opts
    end
  end

  defp down_log(:normal), do: :nolog
  defp down_log(:shutdown), do: :nolog
  defp down_log({:shutdown, _}), do: :nolog
  defp down_log(_), do: :log

  defp handle_timeout(s), do: {:noreply, s}

  defp demonitor({_, mon}) when is_reference(mon) do
    Process.demonitor(mon, [:flush])
  end
  defp demonitor({mon, :after_connect}) when is_reference(mon) do
    Process.demonitor(mon, [:flush])
  end
  defp demonitor({_, _}), do: true
  defp demonitor(nil), do: true

  defp start_timer(_, :infinity), do: nil
  defp start_timer(pid, timeout) do
    :erlang.start_timer(timeout, self(), {__MODULE__, pid, timeout})
  end

  defp cancel_timer(nil), do: :ok
  defp cancel_timer(timer) do
    case :erlang.cancel_timer(timer) do
      false -> flush_timer(timer)
      _     -> :ok
    end
  end

  defp flush_timer(timer) do
    receive do
      {:timeout, ^timer, {__MODULE__, _, _}} ->
        :ok
    after
      0 ->
        raise ArgumentError, "timer #{inspect(timer)} does not exist"
    end
  end

  defp handle_checkin(state, s) do
    %{backoff: backoff, client: client} = s
    backoff = backoff && Backoff.reset(backoff)
    demonitor(client)
    pool_update(state, %{s | client: nil, backoff: backoff})
  end

  defp pool_update(state, %{pool: pool, tag: tag, mod: mod} = s) do
    case Holder.update(pool, tag, mod, state) do
      {:ok, ref} ->
        {:noreply, %{s | client: {ref, :pool}, state: state}, :hibernate}

      :error ->
        {:stop, {:shutdown, :no_more_pool}, s}
    end
  end

  defp normal_status(mod, pdict, state) do
    try do
      mod.format_status(:normal, [pdict, state])
    catch
      _, _ ->
        normal_status_default(mod, state)
    else
      status ->
        status
    end
  end

  defp normal_status_default(mod, state) do
    [{:data, [{'Module', mod}, {'State', state}]}]
  end

  defp terminate_status(mod, pdict, state) do
    try do
      mod.format_status(:terminate, [pdict, state])
    catch
      _, _ ->
        state
    else
      status ->
        status
    end
  end

  defp cleanup_stacktrace(stack) do
    case stack do
      [{_, _, arity, _} | _rest] = stacktrace when is_integer(arity) ->
        stacktrace

      [{mod, fun, args, info} | rest] when is_list(args) ->
        [{mod, fun, length(args), info} | rest]
    end
  end

  defp notify_connection_listeners(message, %{} = state) do
    %{connection_listeners: connection_listeners} = state

    Enum.each(connection_listeners, &send(&1, message))
  end
end
