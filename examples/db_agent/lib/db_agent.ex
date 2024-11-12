defmodule DBAgent do
  use DBConnection

  defmodule Query do
    defstruct [:query]
  end

  @spec start_link((-> state :: any), Keyword.t()) :: GenServer.on_start()
  def start_link(fun, opts \\ []) when is_function(fun, 0) do
    opts = [init: fun, backoff: nil] ++ opts
    DBConnection.start_link(__MODULE__, opts)
  end

  @spec get(DBConnection.conn(), (state :: any -> value), timeout) :: value when value: var
  def get(conn, fun, timeout \\ 5_000) do
    DBConnection.execute!(conn, %Query{query: :get}, fun, timeout: timeout)
  end

  @spec update(DBConnection.conn(), (state :: any -> new_state :: any), timeout) :: :ok
  def update(conn, fun, timeout \\ 5_000) do
    DBConnection.execute!(conn, %Query{query: :update}, fun, timeout: timeout)
  end

  @spec get(DBConnection.conn(), (state :: any -> {value, new_state :: any}), timeout) :: value
        when value: var
  def get_and_update(conn, fun, timeout \\ 5_000) do
    DBConnection.execute!(conn, %Query{query: :get_and_update}, fun, timeout: timeout)
  end

  @spec transaction(DBConnection.conn(), (DBConnection.t() -> res), timeout) ::
          {:ok, res} | {:error, reason :: any}
        when res: var
  def transaction(conn, fun, timeout \\ 5_000) when is_function(fun, 1) do
    DBConnection.transaction(conn, fun, timeout: timeout)
  end

  @spec rollback(DBConnection.t(), reason :: any) :: no_return
  defdelegate rollback(conn, reason), to: DBConnection

  ## DBConnection API

  @impl true
  def connect(opts) do
    fun = Keyword.fetch!(opts, :init)
    {:ok, %{state: fun.(), status: :idle, rollback: nil}}
  end

  @impl true
  def checkout(s), do: {:ok, s}

  @impl true
  def checkin(s), do: {:ok, s}

  @impl true
  def ping(s), do: {:ok, s}

  @impl true
  def handle_execute(%Query{query: :get} = query, fun, _, %{state: state} = s) do
    {:ok, query, fun.(state), s}
  end

  def handle_execute(%Query{query: :update} = query, fun, _, %{state: state} = s) do
    {:ok, query, :ok, %{s | state: fun.(state)}}
  end

  def handle_execute(%Query{query: :get_and_update} = query, fun, _, s) do
    %{state: state} = s
    {res, state} = fun.(state)
    {:ok, query, res, %{s | state: state}}
  end

  @impl true
  def handle_close(_, _, s) do
    {:ok, nil, s}
  end

  @impl true
  def handle_begin(_, %{status: :idle, state: state} = s) do
    {:ok, :began, %{s | status: :transaction, rollback: state}}
  end

  @impl true
  def handle_commit(_, %{status: :transaction} = s) do
    {:ok, :committed, %{s | status: :idle, rollback: nil}}
  end

  @impl true
  def handle_rollback(_, %{status: :transaction, rollback: state} = s) do
    {:ok, :rolledback, %{s | state: state, status: :idle, rollback: nil}}
  end
end

defimpl DBConnection.Query, for: DBAgent.Query do
  alias DBAgent.Query

  def parse(%Query{query: tag} = query, _)
      when tag in [:get, :update, :get_and_update] do
    query
  end

  def describe(query, _), do: query

  def encode(_, fun, _) when is_function(fun, 1), do: fun

  def encode(_, other, _) do
    raise ArgumentError, "#{inspect(other)} is not 1-arity fun"
  end

  def decode(_, result, _), do: result
end
