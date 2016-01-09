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
      GenServer.call(owner, :checkout, timeout)
    end

    def checkin(owner, fun, args, opts) do
      timeout = Keyword.get(opts, :owner_timeout, @timeout)
      GenServer.call(owner, {:checkin, fun, args}, timeout)
    end

    # Callbacks

    def init({module, pool_mod, pool_opts}) do
      {:ok, pid} = pool_mod.start_link(module, pool_opts)
      {:ok, %{pool: pid, pool_mod: pool_mod}}
    end

    def handle_call(:checkout, _from, state) do
      %{pool_mod: pool_mod, pool: pool} = state
      {:reply, {pool_mod, pool}, state}
    end

    def handle_call({:checkin, fun, args}, _from, state) do
      %{pool_mod: pool_mod} = state
      {:reply, apply(pool_mod, fun, args), state}
    end
  end

  def start_link(module, opts) do
    Owner.start_link(module, opts)
  end

  def child_spec(module, opts, child_opts) do
    Supervisor.Spec.worker(Owner, [module, opts], child_opts)
  end

  def checkout(owner, opts) do
    {pool_mod, pool} = Owner.checkout(owner, opts)
    case pool_mod.checkout(pool, opts) do
      {:ok, pool_ref, module, state} ->
        {:ok, {owner, pool_ref}, module, state}
      :error ->
        :error
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
