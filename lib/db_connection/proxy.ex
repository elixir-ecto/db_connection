defmodule DBConnection.Proxy do



  @doc """
  Use `DBConnection.Proxy` to set the behaviour and include default
  implementations. The default implementation of `checkout/3` stores
  the checkout options as the proxy's state, `checkin/4` acts a no-op
  and the remaining callbacks call the internal connection module with
  the given arguments and state.
  """
  defmacro __using__(_) do
    quote location: :keep do
      @behaviour DBConnection.Proxy

      def checkout(_, opts, conn), do: {:ok, conn, opts}

      def checkin(_, _, conn, _), do: {:ok, conn}

      def handle_begin(mod, opts, conn, state) do
        case apply(mod, :handle_begin, [opts, conn]) do
          {:ok, _} = ok ->
            Tuple.append(ok, state)
          {:error, _, _} = error ->
            Tuple.append(error, state)
          {:disconnect, _, _} = discon ->
            discon
          other ->
            raise DBConnection.Error, "bad return value: #{inspect other}"
        end
      end

      def handle_commit(mod, opts, conn, state) do
        case apply(mod, :handle_commit, [opts, conn]) do
          {:ok, _} = ok ->
            Tuple.append(ok, state)
          {:error, _, _} = error ->
            Tuple.append(error, state)
          {:disconnect, _, _} = discon ->
            discon
          other ->
            raise DBConnection.Error, "bad return value: #{inspect other}"
        end
      end

      def handle_rollback(mod, opts, conn, state) do
        case apply(mod, :handle_rollback, [opts, conn]) do
          {:ok, _} = ok ->
            Tuple.append(ok, state)
          {:error, _, _} = error ->
            Tuple.append(error, state)
          {:disconnect, _, _} = discon ->
            discon
          other ->
            raise DBConnection.Error, "bad return value: #{inspect other}"
        end
      end

      def handle_prepare(mod, query, opts, conn, state) do
        case apply(mod, :handle_prepare, [query, opts, conn]) do
          {:ok, _, _} = ok ->
            Tuple.append(ok, state)
          {:error, _, _} = error ->
            Tuple.append(error, state)
          {:disconnect, _, _} = discon ->
            discon
          other ->
            raise DBConnection.Error, "bad return value: #{inspect other}"
        end
      end

      def handle_execute(mod, query, params, opts, conn, state) do
        case apply(mod, :handle_execute, [query, params, opts, conn]) do
          {:ok, _, _} = ok ->
            Tuple.append(ok, state)
          {:prepare, _} = prepare ->
            Tuple.append(prepare, state)
          {:error, _, _} = error ->
            Tuple.append(error, state)
          {:disconnect, _, _} = discon ->
            discon
          other ->
            raise DBConnection.Error, "bad return value: #{inspect other}"
        end
      end

      def handle_execute_close(mod, query, params, opts, conn, state) do
        case apply(mod, :handle_execute_close, [query, params, opts, conn]) do
          {:ok, _, _} = ok ->
            Tuple.append(ok, state)
          {:prepaere, _} = prepare ->
            Tuple.append(prepare, state)
          {:error, _, _} = error ->
            Tuple.append(error, state)
          {:disconnect, _, _} = discon ->
            discon
          other ->
            raise DBConnection.Error, "bad return value: #{inspect other}"
        end
      end

      def handle_close(mod, query, opts, conn, state) do
        case apply(mod, :handle_close, [query, opts, conn]) do
          {:ok, _} = ok ->
            Tuple.append(ok, state)
          {:error, _, _} = error ->
            Tuple.append(error, state)
          {:disconnect, _, _} = discon ->
            discon
          other ->
            raise DBConnection.Error, "bad return value: #{inspect other}"
        end
      end

      defoverridable [checkout: 3, checkin: 4, handle_begin: 4,
                      handle_commit: 4, handle_rollback: 4, handle_prepare: 5,
                      handle_execute: 6, handle_execute_close: 6,
                      handle_close: 5]
    end
  end
end
