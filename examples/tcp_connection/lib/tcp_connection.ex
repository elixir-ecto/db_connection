defmodule TCPConnection do
  use DBConnection

  defmodule Query do
    defstruct [:query]
  end

  defmodule Error do
    defexception [:function, :reason, :message]

    def exception({function, reason}) do
      message = "#{function} error: #{format_error(reason)}"
      %Error{function: function, reason: reason, message: message}
    end

    defp format_error(:closed), do: "closed"
    defp format_error(:timeout), do: "timeout"
    defp format_error(reason), do: :inet.format_error(reason)
  end

  def start_link(host, port, opts \\ []) do
    opts = [hostname: host, port: port] ++ opts
    DBConnection.start_link(__MODULE__, opts)
  end

  def send(conn, data) do
    case DBConnection.execute(conn, %Query{query: :send}, data) do
      {:ok, _, :ok} -> :ok
      {:error, _} = err -> err
    end
  end

  def recv(conn, bytes, timeout \\ 3000) do
    case DBConnection.execute(conn, %Query{query: :recv}, [bytes, timeout]) do
      {:ok, _query, result} -> {:ok, result}
      {:error, _} = err -> err
    end
  end

  def run(conn, fun, opts \\ []) when is_function(fun, 1) do
    DBConnection.run(conn, fun, opts)
  end

  @impl true
  def checkout(state) do
    {:ok, state}
  end

  @impl true
  def ping(state) do
    {:ok, state}
  end

  @impl true
  def connect(opts) do
    host = Keyword.fetch!(opts, :hostname) |> String.to_charlist()
    port = Keyword.fetch!(opts, :port)
    socket_opts = Keyword.get(opts, :socket_options, [])
    timeout = Keyword.get(opts, :connect_timeout, 5_000)

    enforced_opts = [packet: :raw, mode: :binary, active: false]
    # :gen_tcp.connect gives priority to options at tail, rather than head.
    socket_opts = Enum.reverse(socket_opts, enforced_opts)

    case :gen_tcp.connect(host, port, socket_opts, timeout) do
      {:ok, sock} ->
        # Monitor the socket so we can react to it being closed. See handle_info/2.
        _ref = :inet.monitor(sock)
        {:ok, {sock, <<>>}}

      {:error, reason} ->
        {:error, TCPConnection.Error.exception({:connect, reason})}
    end
  end

  @impl true
  def disconnect(_, {sock, _} = state) do
    :ok = :gen_tcp.close(sock)
    # If socket is active we flush any socket messages so the next
    # socket does not get the messages.
    _ = flush(state)
    :ok
  end

  @impl true
  def handle_execute(%Query{query: :send} = query, data, _, {sock, _} = state) do
    case :gen_tcp.send(sock, data) do
      :ok ->
        # A result is always required for handle_query/3
        {:ok, query, :ok, state}

      {:error, reason} ->
        {:disconnect, TCPConnection.Error.exception({:send, reason}), state}
    end
  end

  def handle_execute(%Query{query: :recv} = query, [bytes, timeout], _, {sock, <<>>} = state) do
    # The simplest case when there is no buffer. This callback is called
    # in the process that called DBConnection.execute/4 so has
    # to block until there is a result or error. `active: :once` can't
    # be used.
    case :gen_tcp.recv(sock, bytes, timeout) do
      {:ok, data} ->
        {:ok, query, data, state}

      {:error, :timeout} ->
        # Some errors can be handled without a disconnect. In most cases
        # though it might be better to disconnect on timeout or any
        # other socket error.
        {:error, TCPConnection.Error.exception({:recv, :timeout}), state}

      {:error, reason} ->
        {:disconnect, TCPConnection.Error.exception({:recv, reason}), state}
    end
  end

  def handle_execute(%Query{query: :recv} = query, [bytes, _], _, {sock, buffer})
      when byte_size(buffer) >= bytes do
    # If the state contains a buffer of data the client calls will need
    # to use the buffer before receiving more data.
    case bytes do
      0 ->
        {:ok, query, buffer, {sock, <<>>}}

      _ ->
        <<data::binary-size(bytes), buffer::binary>> = buffer
        {:ok, data, {sock, buffer}}
    end
  end

  def handle_execute(%Query{query: :recv} = query, [bytes, timeout], _, {sock, buffer} = state) do
    # The buffer may not have enough data, so a combination might be
    # required.
    bytes = bytes - byte_size(buffer)

    case :gen_tcp.recv(sock, bytes, timeout) do
      {:ok, data} ->
        {:ok, query, buffer <> data, {sock, <<>>}}

      {:error, :timeout} ->
        {:error, TCPConnection.Error.exception({:recv, :timeout}), state}

      {:error, reason} ->
        {:disconnect, TCPConnection.Error.exception({:recv, reason}), state}
    end
  end

  # The handle_info callback is optional and can be removed if not needed.
  # Here it is used to react to `:inet.monitor/1` messages which arrive
  # when socket gets closed while the connection is idle.
  def handle_info({:DOWN, _ref, _type, sock, _info}, {sock, _buffer}) do
    {:disconnect, TCPConnection.Error.exception({:idle, :closed})}
  end

  def handle_info(msg, state) do
    Logger.info(fn ->
      ["#{__MODULE__} (", inspect(self()), ") missed message: ", inspect(msg)]
    end)

    :ok
  end

  @impl true
  def handle_close(_, _, s) do
    {:ok, nil, s}
  end

  ## Helpers

  defp flush({sock, buffer} = state) do
    receive do
      {:tcp, ^sock, data} ->
        {:ok, {sock, buffer <> data}}

      {:tcp_closed, ^sock} ->
        {:disconnect, TCPConnection.Error.exception({:recv, :closed}), state}

      {:tcp_error, ^sock, reason} ->
        {:disconnect, TCPConnection.Error.exception({:recv, reason}), state}
    after
      0 ->
        # There might not be any socket messages.
        {:ok, state}
    end
  end
end

defimpl DBConnection.Query, for: TCPConnection.Query do
  alias TCPConnection.Query

  def parse(%Query{query: tag} = query, _) when tag in [:send, :recv], do: query

  def describe(query, _), do: query

  def encode(%Query{query: :send}, data, _) when is_binary(data), do: data
  def encode(%Query{query: :recv}, [_bytes, _timeout] = args, _), do: args

  def decode(_, result, _), do: result
end
