6.5.1
-----

* Test fixes
* Add reopen method for channel #263

6.5.0
-----

* Add get methods for exchange and queue #282
* fix type annotation and documentation for Connection.add_close_callback #290

6.4.3
-----

* log channel close status
* add OSError to `CONNECTION_EXCEPTIONS`


6.4.2
-----

* [fix] heartbeat_last to heartbeat_last_received #274
* Fix memory leak #285
* Fix type hint #287
* Pass loop when connecting to aiormq #294

6.4.1
-----

* RobustConnection cleanup fixes #273

6.4.0
-----

* aiormq updates:
    * Fixes for python 3.8 
    [#69](https://github.com/mosquito/aiormq/pull/69) 
    [#67](https://github.com/mosquito/aiormq/pull/67)
    * [passing ``name=`` query parameter](https://github.com/mosquito/aiormq/pull/69/commits/a967502e6dbdf5de422cfb183932bcec134250ad)
    from URL to user defined connection name (Rabbitmq 3.8+) 
    * Fix connection drain [#68](https://github.com/mosquito/aiormq/pull/68)
    * Remove ``loop=`` argument from asyncio entities [#67](https://github.com/mosquito/aiormq/pull/67)
    * ChannelInvalidStateError exceptions instead of RuntimeError 
    [#65](https://github.com/mosquito/aiormq/pull/65)
* Update tests for python 3.8
* ``Pool.close()`` method and allow to use ``Pool`` as a context manager 
[#269](https://github.com/mosquito/aio-pika/pull/269)
* Fix stuck of ``RobustConnection`` when exclusive queues still locked 
on server-side [#267](https://github.com/mosquito/aio-pika/pull/267)
* Add ``global_`` parameter to ``Channel.set_qos`` method 
[#266](https://github.com/mosquito/aio-pika/pull/266)
* Fix ``Connection.drain()`` is ``None`` 
[Fix connection drain](https://github.com/mosquito/aiormq/pull/68)

6.3.0
-----

* passing `client_properties`

6.2.0
-----
* Allow str as an exchange type #260

6.1.2
-----
* Added typing on process method #252

6.1.1
-----

* Documentation fixes
* Missed timeout parameter on `connect()` #245

6.1.0
-----

* Unified `CallbackCollection`s for channels and connections
* Make RobustConnection more robust
* `JsonRPC` and `JsonMaster` adapters
* Improve patterns documentation

6.0.1
-----

* Extended ExchangeType #237. Added `x-modulus-hash` exchange type.

6.0.0
-----

* `RobustConnection` logic changes (see #234). 
  Thanks to @decaz for analysis and fixes. 

5.6.3
-----

* add more type annotations
* consistent setting headers for message #233

5.6.2
-----

* Fixes: set header value on HeaderProxy #232

5.5.3
-----

* Fixed #218. How to properly close RobustConnection?

5.5.2
-----

* Fixed #216. Exception in Queue.consume callback isn't propagated properly.

5.5.1
-----

* Allow to specify `requeue=` and `reject_on_redelivered=` in Master pattern #212


5.5.0
-----

* Fixed #209 int values for headers

5.4.1
-----

* update aiormq version
* use `AMQPError` instead of `AMQPException`. `AMQPException` is now alias for `AMQPError`

5.4.0
-----

* Fix routing key handling (#206 @decaz)
* Fix URL building (#207 @decaz)
* Test suite for `connect` function


5.3.2
-----

* Fix tests for `Pool`


5.3.1
-----

* no duplicate call message when exception
* add robust classes to apidoc

5.3.0
-----

* use None instead of Elipsis for initial state (@chibby0ne)
* `Pool`: enable arguments for pool constructor (@chibby0ne)
* Create py.typed (#176 @zarybnicky)
* 

5.2.4
-----

* Fix encode timestamp error on copy (#198 @tzoiker) 
* Bump `aiormq`

5.2.2
-----

* Fix HeaderProxy bug (#195 @tzoiker)

5.2.1
-----

* remove non-initialized channels when reconnect

5.2.0
-----

* robust connection close only when unclosed
* `heartbeat_last` property

5.1.1
-----

* Simple test suite for testing robust connection via tcp proxy

5.0.1
-----

* robust connection initialization hotfix

5.0.0
-----

* Connector is now `aiormq` and not `pika`
* Remove vendored `pika`
* Compatibility changes:
    * **[HIGH]** Exceptions hierarchy completely changed:
        * ``UnroutableError`` removed. Use ``DeliveryError`` instead.
        * ``ConnectionRefusedError`` is now standard ``ConnectionError``
        * Each error code has separate exception type.
    * **[LOW]** ``Connection.close`` method requires exception instead 
    of ``code`` ``reason`` pair or ``None``
    * **[MEDIUM]** ``IncomingMessage.ack`` ``IncomingMessage.nack`` 
    ``IncomingMessage.reject`` returns coroutines. Old usage compatible 
    but event loop might throw warnings.
    * **[HIGH]** ``Message.timestamp`` property is now ``datetime.datetime``
    * **[LOW]** Tracking of ``publisher confirms`` removed, using 
    similar feature from ``aiormq`` instead.
    * **[LOW]** non async context manager ``IncomingMessage.process()`` 
    is deprecated. Use ``async with message.process():`` instead.

4.9.1
-----

* Fix race condition on callback timeout #180

4.9.0
-----

* Add abstract pool #174
* Fixed Deprecation Warnings in Python 3.7 #153


4.8.1
-----

* Migrate from travis to drone.io
* Use pylava instead of pylama

4.8.0
-----

* save passive flag on reconnect #170

4.7.0
-----

* fixed inconsistent argument type for connection.connect #136
* fixed conditions for creating SSL connection. #135

4.6.4
-----

* Fix UnboundLocalError exception #163 

4.6.3
-----

* RobustConnection fixes #162
* Fix code examples in the README.rst

4.6.1
-----

* Close connection in examples

4.6.0
-----

* Add content_type for all patterns

4.5.0
-----

* Add special exceptions for Worker

4.4.0
-----

* More extendable Master

4.3.0
-----

* Fix #112
* Fix #155

4.2.0
-----

* Add default params for RPC.cereate()

4.1.0
-----

* Fix InvalidStateError when connection lost

4.0.1
-----

* Fix: RPC stuck when response deserialization error

4.0.0
-----

* Drop python 3.4 support

2.9.0
-----

* prevent `set_results` on cancelled future #133
* Added asynchronous context manager support for channels #130

2.8.3
-----

* BUGFIX: ChannelClosed exception was never retrieved

2.8.2
-----

* BUGFIX: handle coroutine double wrapping for Python 3.4

2.8.1
-----

* added example for URL which contains ssl required options.

2.8.0
-----

* `ssl_options` for coonect and connect_robust
* default ports for `amqp` and `amqps`

2.7.1
-----

* python 3.4 fix

2.7.0
-----

* Add `message_kwargs` for worker pattern

2.6.0
-----

* Added `timeout` parameter for `Exchange.declare`
* QueueEmpty exception public added to the module `__all__`

2.5.0
-----

* Ability to reconnect on Channel.Close
* Ability to reconnect on Channel.Cancel

2.4.0
-----

* Rollback to pika==0.10 because new one had issues.

2.3.0
-----

* Feature: abillity to use ExternalCredentials with blank login.

2.2.2
-----

* Bugfix: _on_getempty should delete _on_getok_callback #110.
  (thank's to @dhontecillas)

2.2.1
-----

* Fixes for pyflakes

2.2.0
-----

* Rework transactions

2.1.0
-----

* Use pika's asyncio adapter

2.0.0
-----

* Rework robust connector

1.9.0
-----

* Ability to disable robustness for single queue in `rubust_connect` mode.
* Ability to pass exchage by name.

1.8.1
-----

* Added `python_requires=">3.4.*, <4",` instead of `if sys.version_info` in the `setup.py`

1.8.0
-----

* Change `TimeoutError` to the `asyncio.TimeoutError`
* Allow to bind queue by exchange name
* Added `extras_require = {':python_version': 'typing >= 3.5.3',` to the `setup.py`

1.7.0
-----

* `aio_pika.patterns` submodule
    * `aio_pika.patterns.RPC` - RPC pattern
    * `aio_pika.patterns.Master` - Master/Worker pattern

1.5.1
-----

* `passive` argument for excahnge

1.5.0
-----

* `Channel.is_closed` property
* `Channel.close` just return `None` when channel already closed
* `Connection` might be used in `async with` expression
* `Queue` might be used in `async with` and returns `QueueIterator`
* Changing examples
* `Queue.iterator()` method
* `QueueIterator.close()` returns `asyncio.Future` instead of `asyncio.Task`
* Ability to use `QueueIterator` in `async for` expression
* `connect_robust` is a `coroutine` instead of function which returns a coroutine
(PyCharm type checking display warning instead)
* add tests


1.4.2
-----

* Improve documentation. Add examples for connection and channel
* `Conneciton.close` returns `asyncio.Task` instead coroutine.
* `connect_robust` now is function instead of `partial`.
