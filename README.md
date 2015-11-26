# DBConnection

Database connection behaviour and database connection pool designed for
handling transaction, prepare/execute and client process encode/decode.

Three pool implementations are provided: `DBConnection.Connection`
(default/single connection), `DBConnection.Poolboy` (poolboy pool) and
`DBConnection.Sojourn` (sbroker pool).

An example of using the `DBConnection` behaviour is available in
`./examples/db_agent/`.
