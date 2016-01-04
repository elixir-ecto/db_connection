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

  @pool_timeout  5_000
  @timeout       15_000
  @idle_timeout  15_000
  @backoff_start 200
  @backoff_max   15_000
  @backoff_type  :jitter

  ## DBConnection.Pool API

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
  def stop({pid, ref}, reason, state, _) do
    Connection.cast(pid, {:stop, ref, reason, state})
  end

  @doc false
  def sync_stop({pid, ref}, reason, state, opts) do
    timeout = Keyword.get(opts, :pool_timeout, @pool_timeout)
    {_, mref} = spawn_monitor(fn() ->
      sync_stop(pid, ref, reason, state, timeout)
    end)
    # The reason is not important as long as the process exited
    # before trying to checkin
    receive do
      {:DOWN, ^mref, _, _, _} -> :ok
    end
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

  ## Connection API

  @doc false
  def init({mod, opts, mode, info}) do
    queue  = if mode == :sojourn, do: :broker, else: :queue.new()
    broker = if mode == :sojourn, do: info
    s = %{mod: mod, opts: opts, state: nil, client: :closed, broker: broker,
          queue: queue, timer: nil, backoff: backoff_init(opts),
          after_connect: Keyword.get(opts, :after_connect),
          after_connect_timeout: Keyword.get(opts, :after_connect_timeout,
                                             @timeout),
          idle_timeout: Keyword.get(opts, :idle_timeout, @idle_timeout)}
    if mode == :connection and Keyword.get(opts, :sync_connect, false) do
      connect(:init, s)
    else
      {:connect, :init, s}
    end
  end

  @doc false
  def connect(_, s) do
    %{mod: mod, opts: opts, backoff: backoff, after_connect: after_connect,
      queue: queue, idle_timeout: idle_timeout} = s
    case apply(mod, :connect, [opts]) do
      {:ok, state} when after_connect != nil ->
        ref = make_ref()
        Connection.cast(self(), {:after_connect, ref})
        {:ok, %{s | state: state, client: {ref, :connect}}}
      {:ok, state} when queue == :broker ->
        backoff = backoff_succeed(backoff)
        ref = make_ref()
        Connection.cast(self(), {:connected, ref})
        {:ok, %{s | state: state, client: {ref, :connect}, backoff: backoff}}
      {:ok, state} ->
        backoff = backoff_succeed(backoff)
        {:ok, %{s | state: state, client: nil, backoff: backoff}, idle_timeout}
      {:error, err} when is_nil(backoff) ->
        raise err
      {:error, err} ->
        Logger.error(fn() ->
          [inspect(mod), ?\s, ?(, inspect(self()), ") failed to connect: " |
            Exception.format_banner(:error, err, [])]
        end)
        timeout = :backoff.get(backoff)
        {_, backoff} = :backoff.fail(backoff)
        {:backoff, timeout, %{s | backoff: backoff}}
    end
  end

  @doc false
  def disconnect(err, %{backoff: nil}) do
    raise err
  end
  def disconnect(err, %{mod: mod} = s) do
    Logger.error(fn() ->
      [inspect(mod), ?\s, ?(, inspect(self()),
        ") disconnected: " | Exception.format_banner(:error, err, [])]
    end)
    %{state: state, client: client, timer: timer, queue: queue,
      backoff: backoff} = s
    demonitor(client)
    cancel_timer(timer)
    queue = clear_queue(queue)
    :ok = apply(mod, :disconnect, [err, state])
    s = %{s | state: nil, client: :closed, timer: nil, queue: queue}
    case client do
      {_, :after_connect} ->
        timeout = :backoff.get(backoff)
        {_, backoff} = :backoff.fail(backoff)
        {:backoff, timeout, %{s | backoff: backoff}}
      _ ->
        {:connect, :disconnect, s}
    end
  end

  @doc false
  def handle_call({:checkout, ref, queue?, timeout}, {pid, _} = from, s) do
    case s do
      %{queue: :broker} ->
        exit(:bad_checkout)
      %{client: nil, state: state} ->
        mon = Process.monitor(pid)
        handle_checkout({ref, mon}, timeout, from, state, s)
      %{client: :closed} ->
        {:reply, :error, s}
      %{queue: queue} when queue? == true ->
        client = {ref, Process.monitor(pid)}
        queue = :queue.in({client, timeout, from}, queue)
        {:noreply, %{s | queue: queue}}
      _ when queue? == false ->
        {:reply, :error, s}
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
  def handle_cast({:checkin, ref, state}, %{client: {ref, _}} = s) do
    handle_next(state, s)
  end

  def handle_cast({:disconnect, ref, err, state}, %{client: {ref, _}} = s) do
    {:disconnect, err, %{s | state: state}}
  end

  def handle_cast({:stop, ref, reason, state}, %{client: {ref, _}} = s) do
    message = "client stopped: " <> Exception.format_exit(reason)
    exception = DBConnection.Error.exception(message)
    ## Terrible hack so the stacktrace points here and we get the new
    ## state in logs
    {_, stack} = :erlang.process_info(self(), :current_stacktrace)
    {:stop, {exception, stack}, %{s | state: state}}
  end

  def handle_cast({:cancel, _}, %{queue: :broker}) do
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
        ref = DBConnection.Task.run_child(mod, after_connect, state, opts)
        timer = start_timer(timeout)
        s = %{s | client: {ref, :after_connect}, timer: timer, state: state}
        {:noreply, s}
      {:disconnect, err, state} ->
        {:disconnect, err, %{s | state: state}}
    end
  end

  def handle_cast({:after_connect, _}, s) do
    {:noreply, s}
  end

  def handle_cast({:connected, ref}, %{client: {ref, :connect}} = s) do
    %{mod: mod, state: state, broker: broker} = s
    case apply(mod, :checkout, [state]) do
      {:ok, state} ->
        info = {self(), mod, state}
        {:await, ^ref, _} = :sbroker.async_ask_r(broker, info, ref)
        {:noreply,  %{s | client: {ref, :broker}, state: state}}
      {:disconnect, err, state} ->
        {:disconnect, err, %{s | state: state}}
    end
  end

  def handle_cast({:connected, _}, %{queue: :broker} = s) do
    {:noreply, s}
  end

  @doc false
  def handle_info({:DOWN, ref, _, _, _}, %{client: {ref, :after_connect}} = s) do
    exception = DBConnection.Error.exception("client down")
    {:disconnect, exception, %{s | client: {nil, :after_connect}}}
  end
  def handle_info({:DOWN, mon, :process, _, _}, %{client: {ref, mon}} = s) do
    exception = DBConnection.Error.exception("client down")
    {:disconnect, exception, %{s | client: {ref, nil}}}
  end
  def handle_info({:DOWN, _, :process, _, _} = msg, %{queue: :broker} = s) do
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

  def handle_info({:timeout, timer, __MODULE__}, %{timer: timer} = s)
  when is_reference(timer) do
    exception = DBConnection.Error.exception("client timeout")
    {:disconnect, exception, %{s | timer: nil}}
  end

  def handle_info(:timeout, %{client: nil, broker: nil} = s) do
    %{mod: mod, state: state} = s
    case apply(mod, :ping, [state]) do
      {:ok, state} ->
        handle_timeout(%{s | state: state})
      {:disconnect, err, state} ->
        {:disconnect, err, %{s | state: state}}
    end
  end

  def handle_info({ref, msg}, %{client: {ref, :broker}} = s) do
    handle_broker(msg, s)
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
  def format_status(info, [pdict, %{mod: mod, state: state}]) do
    case function_exported?(mod, :format_status, 2) do
      true when info == :normal ->
        normal_status(mod, pdict, state)
      false when info == :normal ->
        normal_status_default(mod, state)
      true when info == :terminate ->
        terminate_status(mod, pdict, state)
      false when info == :terminate ->
        state
    end
  end

  ## Helpers

  defp start_opts(:connection, opts) do
    Keyword.take(opts, [:debug, :name, :timeout, :spawn_opt])
  end
  defp start_opts(mode, opts) when mode in [:poolboy, :sojourn] do
    Keyword.take(opts, [:debug, :timeout, :spawn_opt])
  end

  defp cancel(pool, ref) do
    try do
      Connection.cast(pool, {:cancel, ref})
    rescue
      ArgumentError ->
        :ok
    end
  end

  defp sync_stop(pid, ref, reason, state, timeout) do
    mref = Process.monitor(pid)
    case Connection.call(pid, {:stop, ref, reason, state}, timeout) do
      :ok ->
        # The reason is not important as long as the process exited
        # before trying to checkin
        receive do: ({:DOWN, ^mref, _, _, _} -> :ok)
      :error ->
        exit(:normal)
    end
  end

  defp backoff_init(opts) do
    case Keyword.get(opts, :backoff_type, @backoff_type) do
      :stop ->
        nil
      type ->
        {start, max} = backoff_args(opts)
        backoff = :backoff.init(start, max)
        :backoff.type(backoff, type)
    end
  end

  defp backoff_args(opts) do
    case {opts[:backoff_start], opts[:backoff]} do
      {nil, nil}   -> {@backoff_start, @backoff_max}
      {nil, max}   -> {min(@backoff_start, max), max}
      {start, nil} -> {start, max(start, @backoff_max)}
      {start, max} -> {start, max}
    end
  end

  defp backoff_succeed(nil), do: nil
  defp backoff_succeed(backoff) do
    {_, backoff} = :backoff.succeed(backoff)
    backoff
  end

  defp handle_checkout({ref, _} = client, timeout, from, state, s) do
    %{mod: mod} = s
    case apply(mod, :checkout, [state]) do
      {:ok, state} ->
        Connection.reply(from, {:ok, {self(), ref}, mod, state})
        timer = start_timer(timeout)
        {:noreply,  %{s | client: client, timer: timer, state: state}}
      {:disconnect, err, state} ->
        {:disconnect, err, %{s | state: state}}
    end
  end

  defp handle_next(state, %{client: {_, :after_connect} = client} = s) do
    %{backoff: backoff} = s
    backoff = backoff_succeed(backoff)
    demonitor(client)
    handle_next(state, %{s | client: nil, backoff: backoff})
  end
  defp handle_next(state, %{queue: :broker} = s) do
    %{client: client, timer: timer, mod: mod, broker: broker} = s
    demonitor(client)
    cancel_timer(timer)
    info = {self(), mod, state}
    {:await, ref, _} = :sbroker.async_ask_r(broker, info, make_ref())
    {:noreply,  %{s | state: state, client: {ref, :broker}, timer: nil}}
  end
  defp handle_next(state, s) do
    %{client: client, timer: timer, queue: queue} = s
    demonitor(client)
    cancel_timer(timer)
    {item, queue} = :queue.out(queue)
    s = %{s | client: nil, timer: nil, queue: queue}
    case item do
      {:value, {{ref, _} = new_client, timeout, from}} ->
        %{mod: mod} = s
        Connection.reply(from, {:ok, {self(), ref}, mod, state})
        timer = start_timer(timeout)
        {:noreply,  %{s | client: new_client, timer: timer, state: state}}
      :empty ->
        handle_checkin(state, s)
    end
  end

  defp handle_checkin(state, %{mod: mod} = s) do
    case apply(mod, :checkin, [state]) do
      {:ok, state} ->
        handle_timeout(%{s | state: state})
      {:disconnect, err, state} ->
        {:disconnect, err, %{s | state: state}}
    end
  end

  defp handle_broker({:go, ref, {pid, timeout}, _, _}, s) do
    mon = Process.monitor(pid)
    timer = start_timer(timeout)
    {:noreply, %{s | client: {ref, mon}, timer: timer}}
  end

  defp handle_broker({:drop, _}, s) do
    %{mod: mod, state: state, broker: broker, client: {ref, :broker}} = s
    case apply(mod, :ping, [state]) do
      {:ok, state} ->
        info = {self(), mod, state}
        {:await, ^ref, _} = :sbroker.async_ask_r(broker, info, ref)
        {:noreply,  %{s | state: state}}
      {:disconnect, err, state} ->
        {:disconnect, err, %{s | state: state}}
    end
  end

  defp do_handle_info(msg, %{mod: mod, state: state} = s) do
    case apply(mod, :handle_info, [msg, state]) do
      {:ok, state} ->
        handle_timeout(%{s | state: state})
      {:disconnect, err, state} ->
        {:disconnect, err, %{s | state: state}}
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

  defp start_timer(:infinity), do: nil
  defp start_timer(timeout), do: :erlang.start_timer(timeout, self, __MODULE__)

  defp cancel_timer(nil), do: :ok
  defp cancel_timer(timer) do
    case :erlang.cancel_timer(timer) do
      false -> flush_timer(timer)
      _     -> :ok
    end
  end

  defp flush_timer(timer) do
    receive do
      {:timeout, ^timer, __MODULE__} ->
        :ok
    after
      0 ->
        raise ArgumentError, "timer #{inspect(timer)} does not exist"
    end
  end

  defp clear_queue(:broker), do: :broker
  defp clear_queue(queue) do
    clear =
      fn({{_, mon}, _, from}) ->
          Process.demonitor(mon, [:flush])
          Connection.reply(from, :error)
          false
      end
    :queue.filter(clear, queue)
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
end
