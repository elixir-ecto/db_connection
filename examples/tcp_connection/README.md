TCPConnection
=============

`TCPConnection` is a simple `DBConnection` process that behaves in a
similar way to a `:gen_tcp` socket.

This example should help show how to handle the lifecycle of a socket
and checkout/checkin.

```elixir
{:ok, pid} = TCPConnection.start_link("localhost", 8000)
{:ok, listener} = :gen_tcp.listen(8000, [active: false, mode: :binary])
{:ok, socket} = :gen_tcp.accept(listener)
:ok = :gen_tcp.send(socket, "hello")
{:ok, "hello"} = TCPConnection.recv(pid, 5, 1000)
:ok = TCPConnection.send(pid, "hi")
{:ok, "hi"} = :gen_tcp.recv(socket, 2, 1000)
```
The TCPConnection process will automatically reconnect:
```elixir
:ok = :gen_tcp.close(socket)
{:ok, socket} = :gen_tcp.accept(listener)
:ok = TCPConnection.send(pid, "back!")
{:ok, "back!"} = :gen_tcp.recv(socket, 5, 1000)
```
