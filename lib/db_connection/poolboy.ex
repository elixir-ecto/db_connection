defmodule DBConnection.Poolboy do

  @behaviour DBConnection.Pool

  @timeout 5000

  def start_link(mod, opts) do
    {pool_opts, worker_opts} = pool_args(mod, opts)
    :poolboy.start_link(pool_opts, worker_opts)
  end

  def child_spec(mod, opts, child_opts) do
    {pool_opts, worker_args} = pool_args(mod, opts)
    id = Keyword.get(child_opts, :id, __MODULE__)
    :poolboy.child_spec(id, pool_opts, worker_args)
  end

  def checkout(pool, opts) do
    queue_timeout = Keyword.get(opts, :queue_timeout, @timeout)
    queue?        = Keyword.get(opts, :queue, true)

    worker = :poolboy.checkout(pool, queue?, queue_timeout)
    try do
      DBConnection.Connection.checkout(worker, opts)
    else
      {:ok, worker_ref, mod, state} ->
        {:ok, {pool, worker, worker_ref}, mod, state}
      :error ->
        :poolboy.checkin(pool, worker)
        :error
    catch
      :exit, reason ->
        :poolboy.checkin(pool, worker)
        exit(reason)
    end
  end

  def checkin({pool, worker, worker_ref}, state, opts) do
    DBConnection.Connection.checkin(worker_ref, state, opts)
    :poolboy.checkin(pool, worker)
  end

  def disconnect({pool, worker, worker_ref}, err, state, opts) do
    DBConnection.Connection.disconnect(worker_ref, err, state, opts)
    :poolboy.checkin(pool, worker)
  end

  def stop({_, _, worker_ref}, reason, state, opts) do
    # Don't check worker back into poolboy as it's going to exit, hopefully
    # this prevents poolboy giving this worker (that's about to exit) to
    # another process.
    DBConnection.Connection.stop(worker_ref, reason, state, opts)
  end

  ## Helpers

  defp pool_args(mod, opts) do
    # Strategy must be fifo - even though poolboy optimises for lifo -
    # otherwise a disconnected worker at the front of the queue can be checked
    # out, reply with an error and then return to the front of the queue. This
    # can continue unless load is high enough to or backoff triggers a
    # connection attempt.
    pool_opts = [strategy: :fifo,
                 size: Keyword.get(opts, :pool_size, 10),
                 max_overflow: Keyword.get(opts, :pool_overflow, 0),
                 worker_module: DBConnection.Poolboy.Worker]
    {name_opts(opts) ++ pool_opts, {mod, opts}}
  end

  defp name_opts(opts) do
    case Keyword.get(opts, :name) do
      nil                     -> []
      name when is_atom(name) -> {:local, name}
      name                    -> name
    end
  end
end
