# DBConnection

Database connection behaviour and database connection pool designed for
handling transaction, prepare/execute, cursors and client process
describe/encode/decode.

Examples of using the `DBConnection` behaviour are available in
`./examples/db_agent/` and `./examples/tcp_connection/`.

There is also [a series of articles on building database adapters](http://blog.plataformatec.com.br/2018/11/building-a-new-mysql-adapter-for-ecto-part-i-hello-world/). It includes articles covering both DBConnection and Ecto integrations.

## Contributing

Run unit tests with:

    $ mix test

To run the integration tests (for each available pool):

    $ mix test.pools

To run all tests:

    $ mix test.all

For a small introduction to DBConnection's design, see the
documentation in `DBConnection.Holder`'s source code.

## License

Copyright 2015 James Fish

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
