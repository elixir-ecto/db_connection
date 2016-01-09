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
    @timeout 15_000

    use GenServer

    def start_link(module, opts) do
      pool_mod = Keyword.fetch!(opts, :ownership_pool)
      {owner_opts, pool_opts} = Keyword.split(opts, [:name])
      GenServer.start_link(__MODULE__, {module, pool_mod, pool_opts}, owner_opts)
    end

    def checkout(owner, opts) do
      timeout = Keyword.get(opts, :owner_timeout, @timeout)
      GenServer.call(owner, {:checkout, opts}, timeout)
    end

    def checkin(owner, fun, args, opts) do
      timeout = Keyword.get(opts, :owner_timeout, @timeout)
      GenServer.call(owner, {:checkin, fun, args}, timeout)
    end

    # Callbacks

    def init({module, pool_mod, pool_opts}) do
      {:ok, pool} = pool_mod.start_link(module, pool_opts)
      {:ok, %{pool: pool, pool_mod: pool_mod, checkout: nil}}
    end

    def handle_call({:checkout, opts}, {caller, _ref}, state) do
      %{pool_mod: pool_mod, pool: pool} = state

      try do
        pool_mod.checkout(pool, opts)
      catch
        kind, reason ->
          {:reply, {kind, reason, System.stacktrace}, state}
      else
        {:ok, pool_ref, pool_module, pool_state} ->
          ref = Process.monitor(caller)
          reply = {:ok, {self(), pool_ref}, pool_module, pool_state}
          checkout = {Process.monitor(caller), pool_ref, pool_state, opts}
          {:reply, reply, %{state | checkout: checkout}}
        :error ->
          {:reply, :error, state}
      end
    end

    def handle_call({:checkin, fun, args}, _from, state) do
      %{pool_mod: pool_mod} = state
      {:reply, apply(pool_mod, fun, args), state}
    end

    def handle_info({:DOWN, ref, _, _, _},
                    %{checkout: {ref, pool_ref, pool_state, pool_opts}} = state) do
      %{pool_mod: pool_mod} = state
      # TODO: The pool_state is certainly not the one being hold by
      # the client. Is this an issue? Is stop the best option here?
      err = DBConnection.Error.exception("client down")
      pool_mod.disconnect(pool_ref, err, pool_state, pool_opts)
      {:noreply, %{state | checkout: nil}}
    end

    def handle_info(_msg, state) do
      {:noreply, state}
    end
  end

  def start_link(module, opts) do
    Owner.start_link(module, opts)
  end

  def child_spec(module, opts, child_opts) do
    Supervisor.Spec.worker(Owner, [module, opts], child_opts)
  end

  def checkout(owner, opts) do
    case Owner.checkout(owner, opts) do
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
