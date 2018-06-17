defmodule DBConnection.ConnectionError do
  defexception [:message]

  def exception(message), do: %DBConnection.ConnectionError{message: message}
end

defmodule DBConnection.Connection do
  @moduledoc """
  A `DBConnection.Pool` with a single connection, the default pool.

  ### Options

    * `:sync_connect` - Whether to block the caller of `start_link` to
    carry out an initial connection attempt (boolean, default: `false`)
  """
  @behaviour DBConnection.Pool
  use Connection
  require Logger
  alias DBConnection.Backoff
  alias DBConnection.ConnectionPool

  @pool_timeout  5_000
  @timeout       15_000
  @idle_timeout  1_000

  ## DBConnection.Pool API

  @doc false
  def ensure_all_started(_opts, _type) do
    {:ok, []}
  end

  @doc false
  def start_link(mod, opts) do
    start_link(mod, opts, :connection)
  end

  @doc false
  def child_spec(mod, opts, child_opts \\ []) do
    child_spec(mod, opts, :connection, child_opts)
  end

  @doc false
  def checkout(pool, opts) do
    pool_timeout = opts[:pool_timeout] || @pool_timeout
    queue?        = Keyword.get(opts, :queue, true)
    timeout       = opts[:timeout] || @timeout

    ref = make_ref()
    try do
      Connection.call(pool, {:checkout, ref, queue?, timeout}, pool_timeout)
    catch
      :exit, {_, {_, :call, [pool | _]}} = reason ->
        cancel(pool, ref)
        exit(reason)
    end
  end

  @doc false
  def checkin({pid, ref}, state, _) do
    Connection.cast(pid, {:checkin, ref, state})
  end

  @doc false
  def disconnect({pid, ref}, err, state, _) do
    Connection.cast(pid, {:disconnect, ref, err, state})
  end

  @doc false
  def stop({pid, ref}, err, state, _) do
    Connection.cast(pid, {:stop, ref, err, state})
  end

  @doc false
  def sync_stop({pid, ref}, err, state, opts) do
    timeout = Keyword.get(opts, :pool_timeout, @pool_timeout)
    {_, mref} = spawn_monitor(fn() ->
      sync_stop(pid, ref, err, state, timeout)
    end)
    # The reason is not important as long as the process exited
    # before trying to checkin
    receive do
      {:DOWN, ^mref, _, _, _} -> :ok
    end
  end

  @doc false
  def ping({pid, ref}, state) do
    Connection.cast(pid, {:ping, ref, state})
  end

  ## Internal API

  @doc false
  def start_link(mod, opts, mode, info \\ nil) do
    start_opts = start_opts(mode, opts)
    Connection.start_link(__MODULE__, {mod, opts, mode, info}, start_opts)
  end

  @doc false
  def child_spec(mod, opts, mode, child_opts) do
    Supervisor.Spec.worker(__MODULE__, [mod, opts, mode], child_opts)
  end

  @doc false
  def child_spec(mod, opts, mode, info, child_opts) do
    Supervisor.Spec.worker(__MODULE__, [mod, opts, mode, info], child_opts)
  end

  ## Connection API

  @doc false
  def init({mod, opts, mode, info}) do
    queue         = if mode in [:connection, :poolboy], do: :queue.new(), else: mode
    idle          = if mode in [:connection, :poolboy], do: get_idle(opts), else: :passive
    after_timeout = if mode == :poolboy, do: :stop, else: :backoff
    pool          = if mode == :connection_pool, do: elem(info, 0)
    tag           = if mode == :connection_pool, do: elem(info, 1)

    s = %{mod: mod, opts: opts, state: nil, client: :closed,
          pool: pool, tag: tag, queue: queue,
          timer: nil, backoff: Backoff.new(opts),
          after_connect: Keyword.get(opts, :after_connect),
          after_connect_timeout: Keyword.get(opts, :after_connect_timeout,
                                             @timeout), idle: idle,
          idle_timeout: Keyword.get(opts, :idle_timeout, @idle_timeout),
          idle_time: 0,  after_timeout: after_timeout}
    if mode == :connection and Keyword.get(opts, :sync_connect, false) do
      connect(:init, s)
    else
      {:connect, :init, s}
    end
  end

  @doc false
  def connect(_, s) do
    %{mod: mod, opts: opts, backoff: backoff, after_connect: after_connect,
      idle: idle, idle_timeout: idle_timeout} = s
    try do
      apply(mod, :connect, [connect_opts(opts)])
    rescue
      e in KeyError ->
        stack = cleanup_stacktrace(System.stacktrace())
        error = Exception.format_banner(:error, %{e | term: nil}, stack)
        msg = """
        Connect raised a KeyError error:

          #{error}

        Some exception details are hidden, as they may contain sensitive data such \
        as database credentials.
        """

        reraise RuntimeError.exception(msg), stack
      e ->
        stack = cleanup_stacktrace(System.stacktrace())
        msg = """
        Connect raised a #{inspect e.__struct__} error. The exception details are hidden, as \
        they may contain sensitive data such as database credentials.
        """

      reraise RuntimeError.exception(msg), stack
    else
      {:ok, state} when after_connect != nil ->
        ref = make_ref()
        Connection.cast(self(), {:after_connect, ref})
        {:ok, %{s | state: state, client: {ref, :connect}}}
      {:ok, state} when idle == :passive ->
        backoff = backoff && Backoff.reset(backoff)
        ref = make_ref()
        Connection.cast(self(), {:connected, ref})
        {:ok, %{s | state: state, client: {ref, :connect}, backoff: backoff}}
      {:ok, state} when idle == :active ->
        backoff = backoff && Backoff.reset(backoff)
        {:ok, %{s | state: state, client: nil, backoff: backoff}, idle_timeout}
      {:error, err} when is_nil(backoff) ->
        raise err
      {:error, err} ->
        Logger.error(fn() ->
          [inspect(mod), ?\s, ?(, inspect(self()), ") failed to connect: " |
            Exception.format_banner(:error, err, [])]
        end)
        {timeout, backoff} = Backoff.backoff(backoff)
        {:backoff, timeout, %{s | backoff: backoff}}
    end
  end

  @doc false
  def disconnect({log, err}, %{mod: mod} = s) do
    case log do
      :nolog ->
        :ok
      :log ->
        _ = Logger.error(fn() ->
          [inspect(mod), ?\s, ?(, inspect(self()),
            ") disconnected: " | Exception.format_banner(:error, err, [])]
        end)
        :ok
    end
    %{state: state, client: client, timer: timer,
      queue: queue, backoff: backoff} = s
    demonitor(client)
    cancel_timer(timer)
    queue = clear_queue(queue)
    :ok = apply(mod, :disconnect, [err, state])
    s = %{s | state: nil, client: :closed, timer: nil, queue: queue}
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
  def handle_call({:checkout, ref, queue?, timeout}, {pid, _} = from, s) do
    case s do
      %{queue: mode} when is_atom(mode) ->
        exit(:bad_checkout)
      %{client: nil, idle: :passive, mod: mod, state: state} ->
        Connection.reply(from, {:ok, {self(), ref}, mod, state})
        client = {ref, Process.monitor(pid)}
        timer = start_timer(pid, timeout)
        {:noreply,  %{s | client: client, timer: timer}}
      %{client: {_, :connect}, after_connect: nil, idle: :passive,
      state: state} ->
        mon = Process.monitor(pid)
        handle_checkout({ref, mon}, timeout, from, state, s)
      %{client: nil, idle: :active, state: state} ->
        mon = Process.monitor(pid)
        handle_checkout({ref, mon}, timeout, from, state, s)
      %{client: :closed} ->
        message = "connection not available because of disconnection"
        err = DBConnection.ConnectionError.exception(message)
        {:reply, {:error, err}, s}
      %{queue: queue} when queue? == true ->
        client = {ref, Process.monitor(pid)}
        queue = :queue.in({client, timeout, from}, queue)
        {:noreply, %{s | queue: queue}}
      _ when queue? == false ->
        message = "connection not available and queuing is disabled"
        err = DBConnection.ConnectionError.exception(message)
        {:reply, {:error, err}, s}
    end
  end

  def handle_call({:stop, ref, _, _} = stop, from, %{client: {ref, _}} = s) do
    Connection.reply(from, :ok)
    handle_cast(stop, s)
  end
  def handle_call({:stop, _, _, _}, _, s) do
    {:reply, :error, s}
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

  def handle_cast({:checkin, ref, state}, %{client: {ref, _}} = s) do
    handle_next(state, s)
  end

  def handle_cast({:disconnect, ref, err, state}, %{client: {ref, _}} = s) do
    {:disconnect, {:log, err}, %{s | state: state}}
  end

  def handle_cast({:stop, ref, err, state}, %{client: {ref, _}} = s) do
    ## Terrible hack so the stacktrace points here and we get the new
    ## state in logs
    {_, stack} = :erlang.process_info(self(), :current_stacktrace)
    {:stop, {err, stack}, %{s | state: state}}
  end

  def handle_cast({:cancel, _}, %{queue: mode}) when is_atom(mode) do
    exit(:bad_cancel)
  end
  def handle_cast({:cancel, ref}, %{client: {ref, _}, state: state} = s) do
    handle_next(state, s)
  end
  def handle_cast({:cancel, ref}, %{queue: queue} = s) do
    cancel =
      fn({{ref2, mon}, _, _}) ->
        if ref === ref2 do
          Process.demonitor(mon, [:flush])
          false
        else
          true
        end
      end
    handle_timeout(%{s | queue: :queue.filter(cancel, queue)})
  end

  def handle_cast({:checkin, _, _}, s) do
    handle_timeout(s)
  end
  def handle_cast({tag, _, _, _}, s) when tag in [:disconnect, :stop] do
    handle_timeout(s)
  end

  def handle_cast({:after_connect, ref}, %{client: {ref, :connect}} = s) do
    %{mod: mod, state: state, after_connect: after_connect,
      after_connect_timeout: timeout, opts: opts} = s
    case apply(mod, :checkout, [state]) do
      {:ok, state} ->
        opts = [timeout: timeout] ++ opts
        {pid, ref} =
          DBConnection.Task.run_child(mod, after_connect, state, opts)
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
    %{mod: mod, state: state, queue: queue} = s
    case apply(mod, :checkout, [state]) do
      {:ok, state} when queue == :connection_pool ->
        pool_update(state, s)
      {:ok, state} ->
        handle_next(state, %{s | client: nil})
      {:disconnect, err, state} ->
        {:disconnect, {:log, err}, %{s | state: state}}
    end
  end

  def handle_cast({:connected, _}, %{idle: :passive} = s) do
    {:noreply, s}
  end

  @doc false
  def handle_info({:DOWN, ref, _, pid, reason},
  %{client: {ref, :after_connect}} = s) do
    message = "client #{inspect pid} exited: " <> Exception.format_exit(reason)
    err = DBConnection.ConnectionError.exception(message)
    {:disconnect, {down_log(reason), err}, %{s | client: {nil, :after_connect}}}
  end
  def handle_info({:DOWN, mon, _, pid, reason}, %{client: {ref, mon}} = s) do
    message = "client #{inspect pid} exited: " <> Exception.format_exit(reason)
    err = DBConnection.ConnectionError.exception(message)
    {:disconnect, {down_log(reason), err}, %{s | client: {ref, nil}}}
  end
  def handle_info({:DOWN, _, :process, _, _} = msg, %{queue: mode} = s)
      when is_atom(mode) do
    do_handle_info(msg, s)
  end
  def handle_info({:DOWN, ref, :process, _, _} = msg, %{queue: queue} = s) do
    len = :queue.len(queue)
    down = fn({{_, mon}, _, _}) -> ref != mon end
    queue = :queue.filter(down, queue)
    case :queue.len(queue) do
      ^len ->
        do_handle_info(msg, s)
      _ ->
        {:noreply, %{s | queue: queue}}
    end
  end

  def handle_info({:timeout, timer, {__MODULE__, pid, timeout}},
  %{timer: timer} = s) when is_reference(timer) do
    message = "client #{inspect pid} timed out because " <>
    "it checked out the connection for longer than #{timeout}ms"
    exception = DBConnection.ConnectionError.exception(message)
    case s do
      # Client timed out and using poolboy. Disable backoff to cause an exit so
      # that poolboy starts a new process immediately. Otherwise this worker
      # doesn't get used until the client checks in. This is equivalent to the
      # other pools because because poolboy does unlimited restarts and no
      # backoff required as connection is active.
      %{after_timeout: :stop, client: {_, mon}} when is_reference(mon) ->
        {:disconnect, {:log, exception}, %{s | timer: nil, backoff: nil}}
      _ ->
        {:disconnect, {:log, exception}, %{s | timer: nil}}
    end
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

  def handle_info(msg, %{client: nil} = s) do
    do_handle_info(msg, s)
  end
  def handle_info(msg, %{client: {_, :connect}} = s) do
    do_handle_info(msg, s)
  end
  def handle_info(msg, %{mod: mod} = s) do
    Logger.info(fn() ->
      [inspect(mod), ?\s, ?(, inspect(self()), ") missed message: " |
        inspect(msg)]
    end)
    {:noreply, s}
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

  defp start_opts(:connection, opts) do
    Keyword.take(opts, [:debug, :name, :timeout, :spawn_opt])
  end
  defp start_opts(mode, opts) when mode in [:poolboy, :connection_pool] do
    Keyword.take(opts, [:debug, :spawn_opt])
  end

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

  defp cancel(pool, ref) do
    try do
      Connection.cast(pool, {:cancel, ref})
    rescue
      ArgumentError ->
        :ok
    end
  end

  defp sync_stop(pid, ref, err, state, timeout) do
    mref = Process.monitor(pid)
    case Connection.call(pid, {:stop, ref, err, state}, timeout) do
      :ok ->
        # The reason is not important as long as the process exited
        # before trying to checkin
        receive do: ({:DOWN, ^mref, _, _, _} -> :ok)
      :error ->
        exit(:normal)
    end
  end

  defp get_idle(opts) do
    case Keyword.get(opts, :idle, :passive) do
      :passive -> :passive
      :active  -> :active
    end
  end

  defp handle_checkout({ref, _} = client, timeout, {pid, _} = from, state, s) do
    %{mod: mod} = s
    case apply(mod, :checkout, [state]) do
      {:ok, state} ->
        Connection.reply(from, {:ok, {self(), ref}, mod, state})
        timer = start_timer(pid, timeout)
        {:noreply,  %{s | client: client, timer: timer, state: state}}
      {:disconnect, err, state} ->
        {:disconnect, {:log, err}, %{s | state: state}}
    end
  end

  defp handle_next(state, %{client: {_, :after_connect} = client} = s) do
    %{backoff: backoff} = s
    backoff = backoff && Backoff.reset(backoff)
    demonitor(client)
    handle_next(state, %{s | client: nil, backoff: backoff})
  end
  defp handle_next(state, %{queue: :connection_pool} = s) do
    pool_update(state, s)
  end
  defp handle_next(state, s) do
    %{client: client, timer: timer, queue: queue, idle: idle} = s
    demonitor(client)
    cancel_timer(timer)
    {item, queue} = :queue.out(queue)
    s = %{s | client: nil, timer: nil, queue: queue}
    case item do
      {:value, {{ref, pid} = new_client, timeout, from}} ->
        %{mod: mod} = s
        Connection.reply(from, {:ok, {self(), ref}, mod, state})
        timer = start_timer(pid, timeout)
        {:noreply,  %{s | client: new_client, timer: timer, state: state}}
      :empty when idle == :passive ->
        handle_timeout(%{s | state: state})
      :empty when idle == :active ->
        handle_checkin(state, s)
    end
  end

  defp handle_checkin(state, %{mod: mod} = s) do
    case apply(mod, :checkin, [state]) do
      {:ok, state} ->
        handle_timeout(%{s | state: state})
      {:disconnect, err, state} ->
        {:disconnect, {:log, err}, %{s | state: state}}
    end
  end

  defp down_log(:normal), do: :nolog
  defp down_log(:shutdown), do: :nolog
  defp down_log({:shutdown, _}), do: :nolog
  defp down_log(_), do: :log

  defp do_handle_info(msg, %{mod: mod, state: state} = s) do
    if function_exported?(mod, :handle_info, 2) do
      case apply(mod, :handle_info, [msg, state]) do
        {:ok, state} ->
          handle_timeout(%{s | state: state})

        {:disconnect, err, state} ->
          {:disconnect, {:log, err}, %{s | state: state}}
      end
    else
      handle_timeout(s)
    end
  end

  defp handle_timeout(%{client: nil, idle_timeout: idle_timeout} = s) do
    {:noreply, s, idle_timeout}
  end
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

  defp clear_queue(queue) when is_atom(queue) do
    queue
  end
  defp clear_queue(queue) do
    clear =
      fn({{_, mon}, _, from}) ->
          Process.demonitor(mon, [:flush])
          message = "connection not available because of disconnection"
          err = DBConnection.ConnectionError.exception(message)
          Connection.reply(from, {:error, err})
          false
      end
    :queue.filter(clear, queue)
  end

  defp pool_update(state, %{pool: pool, tag: tag, mod: mod} = s) do
    ref = ConnectionPool.update(pool, tag, mod, state)
    {:noreply, %{s | client: {ref, :pool}, state: state}, :hibernate}
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
      [{_, _, arity, _} | _rest] = stacktrace when is_integer(arity) -> stacktrace
      [{mod, fun, args, info} | rest] when is_list(args) ->
        [{mod, fun, length(args), info} | rest]
    end
  end
end
