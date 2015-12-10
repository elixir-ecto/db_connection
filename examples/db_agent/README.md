DBAgent
=======

`DBAgent` is a simple `DBConnection` process that behaves in a similar
way to an `Agent` except the state is copied to the client process and functions
called there. Transactions are also supported.

This example should help show the way transactions are handled in
`DBConnection`, and allow easy experimentation with the API.

```elixir
{:ok, agent} = DBAgent.start_link(fn() -> %{} end)
%{} = DBAgent.get(agent, &(&1))
:ok = DBAgent.update(agent, &Map.put(&1, :foo, :bar))
{:error, :oops} = DBAgent.transaction(agent, fn(conn) ->
    :ok = DBAgent.update(conn, &Map.put(&1, :foo, :buzz))
    :buzz = DBAgent.get(conn, &Map.fetch!(&1, :foo))
    DBAgent.rollback(conn, :oops)
end)
:bar = DBAgent.get(agent, &Map.fetch!(&1, :foo))
```
