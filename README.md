# DBConnection

Database connection behaviour and database connection pool designed for
handling transaction, prepare/execute and client process
prepare/encode/decode.

Four pool implementations are provided: `DBConnection.Connection`
(default/single connection), `DBConnection.Poolboy` (poolboy pool),
`DBConnection.Sojourn` (sbroker pool) and `DBConnection.Ownership`
(ownership pool).

Examples of using the `DBConnection` behaviour are available in
`./examples/db_agent/` and `./examples/tcp_connection/`.
