defmodule DBConnection.Poolboy do
  @moduledoc """
  A `DBConnection.Pool` using poolboy.

  ### Options

    * `:pool_size` - The number of connections (default: `10`)
    * `:pool_overflow` - The maximum number of overflow connections to
    start if all connections are checked out (default: `0`)
    * `:queue_out` - Either `:out` for a FIFO queue or `:out_r` for a
    LIFO queue (default: `:out`)
  """

  @behaviour DBConnection.Pool

  @timeout 5000

  @doc false
  def start_link(mod, opts) do
    {pool_opts, worker_opts} = pool_args(mod, opts)
    :poolboy.start_link(pool_opts, worker_opts)
  end

  @doc false
  def child_spec(mod, opts, child_opts) do
    {pool_opts, worker_args} = pool_args(mod, opts)
    id = Keyword.get(child_opts, :id, __MODULE__)
    :poolboy.child_spec(id, pool_opts, worker_args)
  end

  @doc false
  def checkout(pool, opts) do
    queue_timeout = Keyword.get(opts, :queue_timeout, @timeout)
    queue?        = Keyword.get(opts, :queue, true)

    case :poolboy.checkout(pool, queue?, queue_timeout) do
      :full  -> :error
      worker -> checkout(pool, worker, opts)
    end
  end

  @doc false
  def checkin({pool, worker, worker_ref}, state, opts) do
    DBConnection.Connection.checkin(worker_ref, state, opts)
    :poolboy.checkin(pool, worker)
  end

  @doc false
  def disconnect({pool, worker, worker_ref}, err, state, opts) do
    DBConnection.Connection.disconnect(worker_ref, err, state, opts)
    :poolboy.checkin(pool, worker)
  end

  @doc false
  def stop({_, _, worker_ref}, reason, state, opts) do
    # Don't check worker back into poolboy as it's going to exit, hopefully
    # this prevents poolboy giving this worker (that's about to exit) to
    # another process.
    DBConnection.Connection.stop(worker_ref, reason, state, opts)
  end

  ## Helpers

  defp pool_args(mod, opts) do
    pool_opts = [strategy: strategy(opts),
                 size: Keyword.get(opts, :pool_size, 10),
                 max_overflow: Keyword.get(opts, :pool_overflow, 0),
                 worker_module: DBConnection.Poolboy.Worker]
    {name_opts(opts) ++ pool_opts, {mod, opts}}
  end

  defp strategy(opts) do
    case Keyword.get(opts, :queue_out, :out) do
      :out   -> :fifo
      :out_r -> :lifo
    end
  end

  defp name_opts(opts) do
    case Keyword.get(opts, :name) do
      nil                     -> []
      name when is_atom(name) -> {:local, name}
      name                    -> name
    end
  end

  defp checkout(pool, worker, opts) do
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
end
