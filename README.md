# DBConnection

Database connection behaviour and database connection pool designed for
handling transaction, prepare/execute and client process
prepare/encode/decode.

Three pool implementations are provided: `DBConnection.Connection`
(default/single connection), `DBConnection.Poolboy` (poolboy pool) and
`DBConnection.Sojourn` (sbroker pool).

Examples of using the `DBConnection` behaviour are available in
`./examples/db_agent/` and `./examples/tcp_connection/`.
