defmodule DBConnection.Ownership do
  @moduledoc """
  A `DBConnection.Pool` that requires explicit checkout and checking
  as a mechanism to coordinate between processes.

  ### Options

    * `:ownership_pool` - The actual pool to use to power the ownership
      mechanism. The pool is started when the ownership pool is started,
      although this option may also be given on `ownership_checkout/2`
      allowing developers to customize the pool checkout/checkin
    * `:ownership_timeout` - The timeout for ownership operations
    * `:ownership_mode` - When mode is `:manual`, all connections must
      be explicitly checked out before by using `ownership_checkout/2`.
      Otherwise, mode is `:auto` and connections are checked out
      implicitly. In both cases, checkins are implicit via
      `ownership_checkin/2`. Defaults to `:auto`.
  """

  @behaviour DBConnection.Pool

  alias DBConnection.Ownership.Manager
  alias DBConnection.Ownership.Owner

  ## Ownership API

  @spec ownership_checkout(GenServer.server, Keyword.t) ::
    :ok | {:already, :owner | :allowed} | :error | no_return
  def ownership_checkout(manager, opts) do
     case Manager.checkout(manager, opts) do
      {:ok, _} -> :ok
      {:already, _} = already -> already
      :error -> :error
      {kind, reason, stack} -> :erlang.raise(kind, reason, stack)
    end
  end

  defdelegate ownership_mode(manager, mode, opts), to: Manager, as: :mode

  defdelegate ownership_checkin(manager, opts), to: Manager, as: :checkin

  defdelegate ownership_allow(manager, parent, allow, opts), to: Manager, as: :allow

  ## Pool callbacks

  @doc false
  def start_link(module, opts) do
    Manager.start_link(module, opts)
  end

  @doc false
  def child_spec(module, opts, child_opts) do
    Supervisor.Spec.worker(Manager, [module, opts], child_opts)
  end

  @doc false
  def checkout(manager, opts) do
    case Manager.lookup(manager, opts) do
      {:ok, owner} ->
        Owner.checkout(owner, opts)
      :not_found ->
        raise "cannot find ownership process for #{inspect self()}. " <>
              "This may happen if you have not explicitly checked out or " <>
              "the checked out process crashed"
      :error ->
        :error
      {kind, reason, stack} ->
        :erlang.raise(kind, reason, stack)
    end
  end

  @doc false
  def checkin(owner, state, opts) do
    Owner.checkin(owner, state, opts)
  end

  @doc false
  def disconnect(owner, exception, state, opts) do
    Owner.disconnect(owner, exception, state, opts)
  end

  @doc false
  def stop(owner, reason, state, opts) do
    Owner.stop(owner, reason, state, opts)
  end
end
