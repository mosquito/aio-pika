# Quick start

Some useful examples.

## Simple consumer

Connect to RabbitMQ, declare a queue, and consume messages using an async
iterator. Each message is automatically acknowledged when the `process()`
context manager exits successfully.

```{literalinclude} examples/simple_consumer.py
:language: python
```

## Messages interrupted by a disconnection

An `IncomingMessage` belongs to the channel on which it was delivered.
After that channel closes, its `ack()`, `reject()`, and `nack()` methods
raise `ChannelInvalidStateError`. Reconnecting with `connect_robust()`
does not make an old message valid on the replacement channel.

If the body of `async with message.process():` raises an exception and
the delivery channel is closed, the context manager logs that rejection
could not be sent and preserves the original exception. If processing
succeeds but the channel has closed, automatic acknowledgement still
raises `ChannelInvalidStateError`; the message is not marked processed.
`ignore_processed=True` only skips automatic handling of a message that
has already been acknowledged or rejected. It does not revive its channel.

With manual acknowledgements, RabbitMQ requeues unacknowledged deliveries
when their channel closes. A robust queue iterator can continue consuming
after restoration and receive these messages again. Messages already held
by application code or buffered before the disconnection still belong to
the old channel. Handle failed acknowledgements and make processing
idempotent so that a redelivery does not repeat completed side effects.

## Simple publisher

Connect to RabbitMQ and publish a single message to a queue through the
default exchange. The default exchange is available as
`channel.default_exchange`; the routing key is the queue name.

```{literalinclude} examples/simple_publisher.py
:language: python
```

## Asynchronous message processing

Consume messages using a callback function instead of an async iterator.
This allows multiple messages to be processed concurrently, controlled by
the `prefetch_count` setting.

```{literalinclude} examples/simple_async_consumer.py
:language: python
```


## Publishing from a consumer callback

With publisher confirms enabled (the default), await `exchange.publish()`
inside the callback to handle broker errors such as `ChannelAccessRefused`.
An exception in a consumer callback does not propagate to the earlier
`queue.consume()` call: that call registers the consumer. Handle the error
inside the callback or report it to your application's supervisor.

Use `async with message.process():` instead of unconditionally calling
`message.ack()` in a `finally` block. If publishing closes the delivery
channel, that acknowledgement raises `ChannelInvalidStateError` and can
hide the original publishing error. The processing context preserves it.

A dedicated publishing channel isolates broker channel errors from the
consumer's delivery channel:

```python
import logging

import aio_pika
from aiormq.exceptions import ChannelAccessRefused

logger = logging.getLogger(__name__)
publisher = await connection.channel()

async def consume(message):
    try:
        async with message.process():
            await publisher.default_exchange.publish(
                aio_pika.Message(b"reply"), routing_key="replies",
            )
    except ChannelAccessRefused:
        logger.exception("Publishing denied; check exchange permissions")

await queue.consume(consume)
```

Here `queue` uses a different channel from `publisher`. If the broker
closes only the publishing channel, `process()` can still reject the
request on its delivery channel. Its default `requeue=False` discards the
request or routes it to a configured dead-letter exchange; choose this
policy deliberately. A connection failure can still close both channels.

When publishing on the delivery channel itself, a broker channel error
also prevents rejecting or acknowledging the request. RabbitMQ requeues
it, and a robust consumer may receive it again after restoration.
`requeue=False` and `reject_on_redelivered=True` cannot send a rejection
through a closed channel. Repeating the same forbidden publication on
every delivery can therefore loop. Fix the permissions, stop the consumer,
or apply a retry/dead-letter policy before repeating the publication.


## Working with RabbitMQ transactions

Publish messages atomically using AMQP transactions. Messages are only
delivered to the queue after the transaction is committed. Shows both
the context manager approach and manual `select`/`commit`/`rollback` usage.

```{literalinclude} examples/simple_publisher_transactions.py
:language: python
```

## Get single message example

Fetch a single message from a queue using `queue.get()` instead of
continuous consumption. This is useful for polling or one-off retrieval,
with manual acknowledgement of the received message.

```{literalinclude} examples/main.py
:language: python
```

## Set logging level

Sometimes you want to see only your debug logs, but when you just call
`logging.basicConfig(logging.DEBUG)` you set the debug log level for all
loggers, includes all aio_pika's modules. If you want to set logging level
independently see following example:

```{literalinclude} examples/log-level-set.py
:language: python
```

## Connection and channel callbacks

Connections, channels and queues expose callback collections. A callback
is a plain function, a coroutine function or a bound method. The first
argument is always the object that owns the collection (the *sender*),
the other arguments depend on the collection:

| Collection | Sender | Extra arguments |
|---|---|---|
| `connection.close_callbacks` | connection | `exc: BaseException \| None` |
| `connection.reconnect_callbacks` (robust) | connection | none |
| `channel.close_callbacks` | channel | `exc: BaseException \| None` |
| `channel.return_callbacks` | channel | `message: AbstractIncomingMessage` |
| `channel.reopen_callbacks` (robust) | channel | none |
| `queue.close_callbacks` | queue | `exc: BaseException \| None` |

The sender can be `None` when the owner was garbage collected before the
callback ran. The `exc` argument is the exception that closed the object.
It is set for a close requested by your code too: `ChannelClosed` for a
channel and `ConnectionClosed` for a connection. Use
`connection.close_called` to tell an intentional close from a failure.

A callback with a wrong number of arguments is not called; the error is
logged as `Callback ... error`. The collections are typed, so `mypy`
reports such a mismatch before you run the code.

```{literalinclude} examples/callbacks.py
:language: python
```

## External credentials example

Connect to RabbitMQ using TLS client certificates (x509) for
authentication instead of username/password. Requires CA certificate,
client certificate, and private key files.

```{literalinclude} examples/external-credentials.py
:language: python
```

(connection-pooling)=

## Connection pooling

A single AMQP connection multiplexes multiple channels over one TCP
socket, which is sufficient for most applications. However, a single
connection has a finite throughput limited by its TCP link and the
serialization of frames. Connection pooling may help when:

- You are publishing or consuming a very high volume of messages and
  a single TCP connection becomes saturated.
- You want to isolate groups of channels so that a blocked or slow
  connection does not affect other workloads.
- Your application runs many concurrent tasks that would contend for
  the same connection's write lock.

In most cases you **do not need** connection pooling — start with a
single `connect_robust()` connection and only add pooling after
profiling shows that the connection is a bottleneck.

Use `aio_pika.pool.Pool` to manage a pool of connections and channels.

```{literalinclude} examples/pooling.py
:language: python
```

## FastAPI example

Integrate aio-pika with a FastAPI application. The connection is
established during startup via the lifespan context manager and stored
in `app.state` for use across request handlers.

:::{note}
A single robust connection is sufficient for most use cases. AMQP
multiplexes work over channels within one connection. Consider
[connection pooling](#connection-pooling) only if you have measured
that a single connection is a bottleneck.
:::

```{literalinclude} examples/fastapi-pubsub.py
:language: python
```

## Starlette example

Integrate aio-pika with a Starlette application. The connection is
managed through startup/shutdown event handlers and stored in
`app.state` for use across request handlers.

:::{note}
A single robust connection is sufficient for most use cases. AMQP
multiplexes work over channels within one connection. Consider
[connection pooling](#connection-pooling) only if you have measured
that a single connection is a bottleneck.
:::

```{literalinclude} examples/starlette-pubsub.py
:language: python
```

## aiohttp example

Integrate aio-pika with an aiohttp web application. The connection is
created on application startup and stored in the `app` dict for access
from request handlers.

:::{note}
A single robust connection is sufficient for most use cases. AMQP
multiplexes work over channels within one connection. Consider
[connection pooling](#connection-pooling) only if you have measured
that a single connection is a bottleneck.
:::

```{literalinclude} examples/aiohttp-pubsub.py
:language: python
```

## Tornado example

Integrate aio-pika with a Tornado web application. The publisher handler
sends messages on POST requests, while the subscriber handler waits for
incoming messages and returns them as HTTP responses.

:::{note}
A single robust connection is sufficient for most use cases. AMQP
multiplexes work over channels within one connection. Consider
[connection pooling](#connection-pooling) only if you have measured
that a single connection is a bottleneck.
:::

```{literalinclude} examples/tornado-pubsub.py
:language: python
```
