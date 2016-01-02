defmodule DBConnection.Sojourn do
  @moduledoc """
  A `DBConnection.Pool` using sbroker.

  ### Options

    * `:pool_size` - The number of connections (default: `10`)
    * `:broker` - The sbroker callback module (see `:sbroker`,
    default: `DBConnection.Sojourn.Timeout`)
    * `:broker_start_opts` - Start options for the broker (see
    `:sbroker`, default: `[]`)
    * `:max_restarts` - the maximum amount of connection restarts allowed in a
    time frame (default `3`)
    * `:max_seconds` - the time frame in which `:max_restarts` applies (default
    `5`)
    * `:shutdown` - the shutdown strategy for connections (default `5_000`)

  All options are passed as the argument to the sbroker callback module.
  """

  @behaviour DBConnection.Pool

  @broker    DBConnection.Sojourn.Timeout
  @time_unit :micro_seconds

  import Supervisor.Spec

  @doc false
  def start_link(mod, opts) do
    Supervisor.start_link(children(mod, opts), [strategy: :rest_for_one])
  end

  @doc false
  def child_spec(mod, opts, child_opts \\ []) do
    args = [children(mod, opts), [strategy: :rest_for_one]]
    supervisor(Supervisor, args, child_opts)
  end

  @doc false
  def checkout(broker, opts) do
    case ask(broker, opts) do
      {:go, ref, {pid, mod, state}, _, _}    -> {:ok, {pid, ref}, mod, state}
      {drop, _} when drop in [:drop, :retry] -> :error
    end
  end

  @doc false
  defdelegate checkin(ref, state, opts), to: DBConnection.Connection

  @doc false
  defdelegate disconnect(ref, err, state, opts), to: DBConnection.Connection

  @doc false
  defdelegate stop(ref, reason, state, opts), to: DBConnection.Connection

  ## Helpers

  defp children(mod, opts) do
    [broker(opts), conn_sup(mod, opts), starter(opts)]
  end

  defp broker(opts) do
    case Keyword.get(opts, :name, nil) do
      nil ->
        worker(:sbroker, broker_args(opts))
      name when is_atom(name) ->
        worker(:sbroker, [{:local, name} | broker_args(opts)])
      name ->
        worker(:sbroker, [name | broker_args(opts)])
    end
  end

  defp broker_args(opts) do
    mod        = Keyword.get(opts, :broker, @broker)
    start_opts = Keyword.get(opts, :broker_start_opt, [time_unit: @time_unit])
    [mod, opts, start_opts]
  end

  defp conn_sup(mod, opts) do
    child_opts = Keyword.take(opts, [:shutdown])
    conn = DBConnection.Connection.child_spec(mod, opts, :sojourn, child_opts)
    sup_opts = Keyword.take(opts, [:max_restarts, :max_seconds])
    sup_opts = [strategy: :simple_one_for_one] ++ sup_opts
    supervisor(Supervisor, [[conn], sup_opts])
  end

  defp starter(opts) do
    worker(DBConnection.Sojourn.Starter, [opts], [restart: :transient])
  end

  defp ask(broker, opts) do
    timeout = Keyword.get(opts, :timeout, 5_000)
    info = {self(), timeout}
    case Keyword.get(opts, :queue, true) do
      true  -> :sbroker.ask(broker, info)
      false -> :sbroker.nb_ask(broker, info)
    end
  end
end
