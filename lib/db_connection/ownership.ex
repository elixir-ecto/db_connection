defmodule DBConnection.Ownership do
  @moduledoc """
  A `DBConnection.Pool` that requires explicit checkout and checking
  as a mechanism to coordinate between processes.

  ### Options

    * `:ownership_pool` - The actual pool to use to power the ownership
      mechanism
    * `:ownership_timeout` - The timeout for ownership operations
  """

  @behaviour DBConnection.Pool
  @strategy :one_for_all

  defmodule Owner do
    @moduledoc false

    use GenServer
    @timeout 15_000

    def start(from, pool, pool_mod, pool_opts) do
      GenServer.start(__MODULE__, {from, pool, pool_mod, pool_opts}, [])
    end

    def checkin(owner, fun, args, opts) do
      timeout = Keyword.get(opts, :owner_timeout, @timeout)
      GenServer.call(owner, {:checkin, fun, args}, timeout)
    end

    # Callbacks

    def init(args) do
      send self(), :init
      {:ok, args}
    end

    def handle_info(:init, {from, pool, pool_mod, pool_opts}) do
      state = %{owner: nil, pool: pool, pool_mod: pool_mod,
                pool_opts: pool_opts, pool_ref: nil, pool_state: nil}

      try do
        pool_mod.checkout(pool, pool_opts)
      catch
        kind, reason ->
          GenServer.reply(from, {kind, reason, System.stacktrace})
          {:stop, {:shutdown, :nocheckout}, state}
      else
        {:ok, pool_ref, pool_module, pool_state} ->
          {caller, _} = from
          ref = Process.monitor(caller)
          reply = {:ok, {self(), pool_ref}, pool_module, pool_state}
          GenServer.reply(from, reply)
          {:noreply, %{state | owner: ref, pool_ref: pool_ref, pool_state: pool_state}}
        :error ->
          GenServer.reply(from, :error)
          {:stop, {:shutdown, :nocheckout}, state}
      end
    end

    def handle_info({:DOWN, ref, _, _, _}, %{owner: ref} = state) do
      %{pool_mod: pool_mod, pool_ref: pool_ref,
        pool_state: pool_state, pool_opts: pool_opts} = state
      error = DBConnection.Error.exception("client down")
      pool_mod.disconnect(pool_ref, error, pool_state, pool_opts)
      {:stop, {:shutdown, :client_down}, state}
    end

    def handle_info(_msg, state) do
      {:noreply, state}
    end

    def handle_call({:checkin, fun, args}, from, %{pool_mod: pool_mod} = state) do
      GenServer.reply(from, apply(pool_mod, fun, args))
      {:stop, {:shutdown, :checkin}, state}
    end
  end

  defmodule Manager do
    @moduledoc false

    use GenServer
    @timeout 15_000

    def start_link(module, opts) do
      pool_mod = Keyword.fetch!(opts, :ownership_pool)
      {owner_opts, pool_opts} = Keyword.split(opts, [:name])
      GenServer.start_link(__MODULE__, {module, pool_mod, pool_opts}, owner_opts)
    end

    def init({module, pool_mod, pool_opts}) do
      {:ok, pool} = pool_mod.start_link(module, pool_opts)
      {:ok, %{pool: pool, pool_mod: pool_mod}}
    end

    def checkout(manager, opts) do
      timeout = Keyword.get(opts, :owner_timeout, @timeout)
      GenServer.call(manager, {:checkout, opts}, timeout)
    end

    def handle_call({:checkout, opts}, from, state) do
      %{pool_mod: pool_mod, pool: pool} = state
      # TODO: Move this to a supervisor
      # TODO: Cache using ETS
      Owner.start(from, pool, pool_mod, opts)
      {:noreply, state}
    end
  end

  def start_link(module, opts) do
    Manager.start_link(module, opts)
  end

  def child_spec(module, opts, child_opts) do
    Supervisor.Spec.worker(Manager, [module, opts], child_opts)
  end

  def checkout(owner, opts) do
    case Manager.checkout(owner, opts) do
      {:ok, _, _, _} = ok -> ok
      :error -> :error
      {kind, reason, stack} -> :erlang.raise(kind, reason, stack)
    end
  end

  def checkin({owner, pool_ref}, state, opts) do
    Owner.checkin(owner, :checkin, [pool_ref, state, opts], opts)
  end

  def disconnect({owner, pool_ref}, exception, state, opts) do
    Owner.checkin(owner, :disconnect, [pool_ref, exception, state, opts], opts)
  end

  def stop({owner, pool_ref}, reason, state, opts) do
    Owner.checkin(owner, :stop, [pool_ref, reason, state, opts], opts)
  end
end
