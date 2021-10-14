# Changelog

## v2.4.1 (2021-10-14)

* Enhancements
  * Add `DBConnection.disconnect_all/2`

## v2.4.0 (2021-04-02)

* Enhancements
  * Add telemetry events for connection errors
  * Use `:rand` default algorithm
  * Allow decentralized lookups on DBConnection.Ownership

## v2.3.1 (2020-11-25)

* Enhancements
  * Add `:connection_listeners` to `DBConnection.start_link/2`
  * Allow connection `~> 1.0`

## v2.3.0 (2020-10-14)

This release requires Elixir v1.7+.

* Bug fixes
  * Fix deprecation warnings related to the use of `System.stacktrace()`

## v2.2.2 (2020-04-22)

* Bug fixes
  * Make sure all idle connections in the pool are pinged on each idle interval

## v2.2.1 (2020-02-04)

* Enhancements
  * Remove warnings

## v2.2.0 (2019-12-11)

* Enhancements
  * Add `:idle_time` to `DBConnection.LogEntry`
  * Ping all stale connections on idle interval
  * Add `crash_reason` to relevant Logger error reports
  * Ping all stale connections on idle interval. One possible downside of this approach is that we may shut down all connections at once and if there is a request around this time, the response time will be higher. However, this is likely better than the current approach, where we ping only the first one, which means we can have a pool of stale connections. The current behaviour is the same as in v1.0

## v2.1.1 (2019-07-17)

* Enhancements
  * Reduce severity in client exits to info
  * Improve error message on redirect checkout

* Bug fixes
  * Make sure ownership timeout is respected on automatic checkouts

## v2.1.0 (2019-06-07)

* Enhancements
  * Require Elixir v1.6+
  * Include client stacktrace on check out timeouts
