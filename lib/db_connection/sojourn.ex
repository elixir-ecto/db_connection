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
    apply(:sbroker, :start_link, broker_args(mod, opts))
  end

  @doc false
  def child_spec(mod, opts, child_opts \\ []) do
    worker(:sbroker, broker_args(mod, opts), child_opts)
  end

  @doc false
  def checkout(broker, opts) do
    case ask(broker, opts) do
      {:go, ref, {pid, mod, state}, _, _} ->
        {:ok, {pid, ref}, mod, state}
      {drop, _} when drop in [:drop, :retry] ->
        {:error, DBConnection.Error.exception("connection not available")}
    end
  end

  @doc false
  defdelegate checkin(ref, state, opts), to: DBConnection.Connection

  @doc false
  defdelegate disconnect(ref, err, state, opts), to: DBConnection.Connection

  @doc false
  defdelegate stop(ref, reason, state, opts), to: DBConnection.Connection

  ## Helpers

  defp broker_args(mod, opts) do
    broker     = Keyword.get(opts, :broker, @broker)
    start_opts = Keyword.get(opts, :broker_start_opt, [time_unit: @time_unit])
    args       = [__MODULE__.Broker, {broker, mod, opts}, start_opts]
    case Keyword.get(opts, :name) do
      nil                     -> args
      name when is_atom(name) -> [{:local, name} | args]
      name                    -> [name | args]
    end
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
