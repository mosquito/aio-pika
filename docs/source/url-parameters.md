# AMQP URL parameters

URL is the supported way to configure a connection.
For customisation of connection behaviour you might
pass parameters in URL query-string format.

## `aiormq` specific

* `name` (`str` url encoded) - A string that will be visible in the RabbitMQ management console and in
  the server logs, convenient for diagnostics.

* `cafile` (`str`) - Path to Certificate Authority file

* `capath` (`str`) - Path to Certificate Authority directory

* `cadata` (`str` url encoded) - URL encoded CA certificate content

* `keyfile` (`str`) - Path to client ssl private key file

* `certfile` (`str`) - Path to client ssl certificate file

* `no_verify_ssl` - No verify server SSL certificates. `0` by default and means `False` other value means
  `True`.

* `heartbeat` (`int`-like) - interval in seconds between AMQP heartbeat packets. `0` disables this feature.


## `aio_pika.connect` function and `aio_pika.Connection` class specific

* `interleave` (`int`-like) - controls address reordering when a host name resolves to multiple
  IP addresses. If 0 or unspecified, no reordering is done, and addresses are tried
  in the order returned by `getaddrinfo()`. If a positive integer is specified,
  the addresses are interleaved by address family, and the given integer is interpreted
  as "First Address Family Count" as defined in [RFC 8305](https://datatracker.ietf.org/doc/html/rfc8305.html).
  The default is `0` if
  `happy_eyeballs_delay` is not specified, and `1` if it is.

  :::{note}
  Really useful for RabbitMQ clusters with one DNS name with many `A`/`AAAA` records.
  :::

  :::{warning}
  This option is supported by `asyncio.DefaultEventLoopPolicy` and available since python 3.8.
  :::

* `happy_eyeballs_delay` (`float`-like) - if given, enables Happy Eyeballs for this connection.
  It should be a floating-point number representing the amount of time in seconds to wait for a connection attempt
  to complete, before starting the next attempt in parallel. This is the "Connection Attempt Delay" as defined in
  [RFC 8305](https://datatracker.ietf.org/doc/html/rfc8305.html). A sensible default value recommended by the RFC is `0.25` (250 milliseconds).

  :::{note}
  Really useful for RabbitMQ clusters with one DNS name with many `A`/`AAAA` records.
  :::

  :::{warning}
  This option is supported by `asyncio.DefaultEventLoopPolicy` and available since python 3.8.
  :::

## `aio_pika.connect_robust` function and `aio_pika.RobustConnection` class specific

For `aio_pika.RobustConnection` class is applicable all `aio_pika.Connection` related parameters like,
`name`/`interleave`/`happy_eyeballs_delay` and some specific:


* `reconnect_interval` (`float`-like) - is the period in seconds, not more often than the attempts to
  re-establish the connection will take place.


* `fail_fast` (`true`/`yes`/`y`/`enable`/`on`/`enabled`/`1` means `True`, otherwise `False`) -
  special behavior for the start connection attempt, if it fails, all other attempts stops and an exception will be
  thrown at the connection stage. Enabled by default, if you are sure you need to disable this feature, be ensures
  for the passed URL is really working. Otherwise, your program will go into endless reconnection attempts that can
  not be successed.


## URL examples

* `amqp://username:password@hostname/vhost?name=connection%20name&heartbeat=60&happy_eyeballs_delay=0.25`

* `amqps://username:password@hostname/vhost?reconnect_interval=5&fail_fast=1`

* `amqps://username:password@hostname/vhost?cafile=/path/to/ca.pem`

* `amqps://username:password@hostname/vhost?cafile=/path/to/ca.pem&keyfile=/path/to/key.pem&certfile=/path/to/sert.pem`
