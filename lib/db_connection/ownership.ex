defmodule DBConnection.OwnershipError do
  defexception [:message]

  def exception(message), do: %DBConnection.OwnershipError{message: message}
end

defmodule DBConnection.Ownership do
  @moduledoc """
  A `DBConnection.Pool` that requires explicit checkout and checkin
  as a mechanism to coordinate between processes.

  ### Options

    * `:ownership_mode` - When mode is `:manual`, all connections must
      be explicitly checked out before by using `ownership_checkout/2`.
      Otherwise, mode is `:auto` and connections are checked out
      implicitly. `{:shared, owner}` mode is also supported so
      processes are allowed on demand. On all cases, checkins are
      explicit via `ownership_checkin/2`. Defaults to `:auto`.
    * `:ownership_timeout` - The maximum time that a process is allowed to own
      a connection, default `60_000`. This timeout exists mostly for sanity
      checking purposes and can be increased at will, since DBConnection
      automatically checks in connections whenever there is a mode change.
    * `:ownership_log` - The `Logger.level` to log ownership changes, or `nil`
      not to log, default `nil`.

  Finally, if the `:caller` option is given on checkout with a pid and no
  pool is assigned to the current process, a connection will be allowed
  from the given pid and used on checkout with `:pool_timeout` of `:infinity`.
  This is useful when multiple tasks need to collaborate on the same
  connection (hence the `:infinity` timeout).
  """

  @behaviour DBConnection.Pool

  alias DBConnection.Ownership.Manager
  alias DBConnection.Holder

  ## Ownership API

  @doc """
  Explicitly checks a connection out from the ownership manager.

  It may return `:ok` if the connection is checked out.
  `{:already, :owner | :allowed}` if the caller process already
  has a connection, `:error` if it could be not checked out or
  raise if there was an error.
  """
  @spec ownership_checkout(GenServer.server, Keyword.t) ::
    :ok | {:already, :owner | :allowed}
  def ownership_checkout(manager, opts) do
    with {:ok, pid} <- Manager.checkout(manager, opts) do
      case Holder.checkout(pid, opts) do
        {:ok, pool_ref, _module, state} ->
          Holder.checkin(pool_ref, state, opts)
        {:error, err} ->
          raise err
      end
    end
  end

  @doc """
  Changes the ownership mode.

  `mode` may be `:auto`, `:manual` or `{:shared, owner}`.

  The operation will always succeed when setting the mode to
  `:auto` or `:manual`. It may fail with reason `:not_owner`
  or `:not_found` when setting `{:shared, pid}` and the
  given pid does not own any connection. May return
  `:already_shared` if another process set the ownership
  mode to `{:shared, _}` and is still alive.
  """
  @spec ownership_mode(GenServer.server, :auto | :manual | {:shared, pid}, Keyword.t) ::
    :ok | :already_shared | :not_owner | :not_found
  defdelegate ownership_mode(manager, mode, opts), to: Manager, as: :mode

  @doc """
  Checks a connection back in.

  A connection can only be checked back in by its owner.
  """
  @spec ownership_checkin(GenServer.server, Keyword.t) ::
    :ok | :not_owner | :not_found
  defdelegate ownership_checkin(manager, opts), to: Manager, as: :checkin

  @doc """
  Allows the process given by `allow` to use the connection checked out
  by `owner_or_allowed`.

  It may return `:ok` if the connection is checked out.
  `{:already, :owner | :allowed}` if the `allow` process already
  has a connection. `owner_or_allowed` may either be the owner or any
  other allowed process. Returns `:not_found` if the given process
  does not have any connection checked out.
  """
  @spec ownership_allow(GenServer.server, owner_or_allowed :: pid, allow :: pid, Keyword.t) ::
    :ok | {:already, :owner | :allowed} | :not_found
  defdelegate ownership_allow(manager, owner, allow, opts), to: Manager, as: :allow

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
  defdelegate checkout(manager, opts), to: Holder

  @doc false
  defdelegate checkin(proxy, state, opts), to: Holder

  @doc false
  defdelegate disconnect(proxy, err, state, opts), to: Holder

  @doc false
  defdelegate stop(proxy, err, state, opts), to: Holder
end
