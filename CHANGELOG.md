# Changelog

## v2.2.0

* Enhancements
  * Add `crash_reson` to relevant Logger error reports
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
