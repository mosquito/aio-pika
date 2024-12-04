9.3.0
-----

* new: add custom exchanges to rpc pattern #377 by @cloud-rocket

9.2.3
-----

* Fix restore bug of RobustChannel #578 by @aozupek

9.2.2
-----

* Fix bug with RPC when handling `on_close` with a RobustConnection #573 by @CodeCorrupt

9.2.1
-----

* Fix reopen of robust channel after close #571 by @decaz. Fixes #570

9.2.0
-----

* URL params passing to aiormq #569
  * `Connection.KWARGS_TYPES` renamed to `Connection.PARAMETERS` and rework it to `dataclass`
  * `Connection._parse_kwargs` renamed to `Connection._parse_parameters`
  * [AMQP URL parameters](https://docs.aio-pika.com/#amqp-url-parameters) documentation article

9.1.5
-----

* Fix race condition in RobustChannel in reopen/ready #566 by @isra17

9.1.4
-----

* use fork friendly random robust queue generation way #560

9.1.3
-----

* Ported publisher confirms tutorial by @MaPePeR #550
* Fixed errored response when `aio_pika.patterns.RPC`
  can not serialize the result #552

9.1.2
-----

* Fix badges in docs

9.1.1
-----

* Fix readthedocs build file

9.1.0
-----

The bulk of the changes are related to how the library entities are now
interconnected. In previous versions of `aio_pika.Channel` instances not
contains a link to the `aio_pika.Connection` instances for now is contains it.

While I don't want custom code to work directly with the `aiormq.Channel`
instance, this was a public API and I should warn you about the change here.
The `aio_pika.Channel.channel` property is deprecated. Use
`aio_pika.Channel.get_underlay_chanel()` instead.
Now all library entities already use this method.


9.0.7
-----

* Update aiormq version

9.0.6
-----

* Amend Exchange.__repr__ to include class name #527
  Also switch to f-strings rather than %-formatting, modelled after
  Queue.__repr__.
* Update example code of rpc tutorial #530
* bugfix: kwargs not working in `aio_pika.connect_robust` #531
* Improve type hints for `queue.get()` #542

9.0.5
-----

* Prevent 'Task exception was never retrieved' #524
  If future.exception() is not called (even on cancelled futures), it seems Python
  will then log 'Task exception was never retrieved'. Rewriting this logic
  slightly should hopefully achieve the same functionality while
  preventing the Python errors.
* Avoid implicitly depending on setuptools #526

9.0.4
-----

* fix README badge
* upgrade requirements

9.0.3
-----

* RPCs: Show exceptions on Host (remote side) #503
* Fixed queue_name was set as channel_name for `patterns/master.py` #523

9.0.2
-----

* Do not nack if consumer is no_ack in QueueIterator #521

9.0.1
-----

* change classifiers in pyproject.toml

9.0.0
-----

The main goal of this release is the migration to `poetry` and stronger type
checking with mypy.

User code should remain compatible, just test it with mypy. The tests still
work the same, without public API changes, this indicates that your code
should work without changes, but does not prove it.

### Deprecations

* `aio_pika.message.HeaderProxy` - removed
* `aio_pika.message.header_converter` - removed
* `aio_pika.message.format_headers` - removed
* `aio_pika.message.Message.headers_raw` - prints deprecation warning
* `aio_pika.abc.AbstractMessage.headers_raw` - removed

8.3.0
-----

* Update `aiormq~=6.6.3` #512
* Fix getting futures exceptions #509

8.2.4
-----

* Fix memory leaks around channel close callbacks #496
* Fastest way to reject all messages when queue iterator is closing #498

8.2.3
-----

* Fix memory leak when callback collections is chaining #495

8.2.2
-----

* Prevent "Task exception was never retrieved" on timeout #492

8.2.1
-----

* Fix memory leaks on channel close #491

8.2.0
-----

* allow passing ssl_context to the connection #474. A default parameter has
  been added to the public API, this does not break anything unless your
  code relies on the order of the arguments.

8.1.1
-----

* Generated anonymous queue name may conflict #486
* improve typing in multiple library actors #478

8.1.0
-----

* Bump `aiormq~=6.4.0` with `connection blocking` feature
* `Connection.update_secret` method (#481)

8.0.3
-----

* cannot use client_properties issue #469

8.0.2
-----

* linter fixes in `aio_pika.rpc.__all__`

8.0.1
-----

* aio_pika.rpc fix for `TypeError: invalid exception object` for future

8.0.0
-----

***Release notes***

In this release, there are many changes to the internal API and bug fixes
related to sudden disconnection and correct recovery after reconnection.

Unfortunately, the behavior that was in version 7.x was slightly affected.
It's the reason the major version has been updated.

The entire set of existing tests passes with minimal changes, therefore,
except for some minor changes in behavior, the user code should
work either without any modifications or with minimal changes,
such as replacing removed deprecated functions with alternatives.

This release has been already tested in a working environment, and now it seems
that we have completely resolved all the known issues related to
recovery after network failures.

***Changes***:

* Added tests for unexpected network connection resets and fixed
  many related problems.
* Added `UnderlayChannel` and `UnderlayConneciton`, this is `NamedTuple`s
  contains all connection and channel related properties.
  The `aiormq.Connection` and `aiormq.Channel` objects
  are now packaged in this `NamedTuple`s and can be atomically assigned
  to `aio_pika.Connection` and `aio_pika.Channel` objects.
  The main benefit is the not needed to add locks during the connection,
  in the best case, the container object is assigned to callee as usual,
  however, if something goes wrong during the connection, there is no need to
  clear something in `aio_pika.RobustConnection` or `aio_pika.RobustChannel`.
* An `__init__` method is now a part of abstract classes for most
  `aio_pika` entities.
* Removed explicit relations between `aio_pika.Channel`
  and `aio_pika.Connection`. Now you can't get a `aio_pika.Connection`
  instance from the `aio_pika.Channel` instance.
* Fixed a bug that caused the whole connection was closed when a timeout
  occurred in one of the channels, in case the channel was waiting for a
  response frame to an amqp-rpc call.
* Removed deprecated `add_close_callback` and `remove_close_callback` methods
  in `aio_pika.Channel`.
  Use `aio_pika.Channel.close_callbacks.add(callback, ...)` and
  `aio_pika.Channel.close_callbacks.remove(callback, ...)` instead.
* Fixed a bug in `aio_pika.RobustChannel` that caused `default_exchane`
  broken after reconnecting.
* The `publisher_confirms` property of `aio_pika.Channel` is public now.
* Function `get_exchange_name` is public now.
* Fixed an error in which the queue iterator could enter a deadlock state, with
  a sudden disconnection.
* The new entity `OneShotCallback` helps, for example, to call all the closing
  callbacks at the channel if the `Connection` was unexpectedly closed, and
  the channel closing frame did not come explicitly.


7.2.0
-----

* Make `aio_pika.patterns.rpc` more extendable.

7.1.0
-----

* Fixes in documentation

7.0.0
-----

This release brings support for a new version of `aiormq`, which is used as
a low-level driver for working with AMQP.

The release contains a huge number of changes in the internal structure of the
library, mainly related to type inheritance and abstract types, as well
as typehints checking via mypy.

The biggest change to the user API is the violation of the inheritance order,
due to the introduction of abstract types, so this release is a major one.

### Changes

* There are a lot of changes in the structure of the library,
  due to the widespread use of typing.
* `aio_pika.abc` module now contains all types and abstract class prototypes.
* Modern `aiormq~=6.1.1` used.
* Complete type checks coverage via mypy.
* The interface of `aio_pika`'s classes has undergone minimal changes,
  but you should double-check your code before migrating, at least because
  almost all types are now in `aio_pika.abc`. Module `aio_pika.types`
  still exists, but will produce a `DeprecationWarning`.
* Default value for argument `weak` is changed to `False` in
  `CallbackCollection.add(func, weak=False)`.


### Known 6.x to 7.x migration issues

* `pamqp.specification` module didn't exist in `pamqp==3.0.1` so you have to
  change it:
  * `pamqp.commands` for AMPQ-RPC–relates classes
  * `pamqp.base` for `Frame` class
  * `pamqp.body` for `ContentBody` class
  * `pamqp.commands` for `Basic`, `Channel`, `Confirm`, `Exchange`,
    `Queue`, `Tx` classes.
  * `pamqp.common` for `FieldArray`, `FieldTable`, `FieldValue` classes
  * `pamqp.constants` for constants like `REPLY_SUCCESS`.
  * `pamqp.header` for `ContentHeader` class.
  * `pamqp.heartbeat` for `Heartbeat` class.
* Type definitions related to imports from `aio_pika` might throw warnings
  like `'SomeType' is not declared in __all__ `. This is a normal situation,
  since now it is necessary to import types from `aio_pika.abc`. In this
  release, these are just warnings, but in the next major release, this will
  stop working, so you should take care of changes in your code.

  Just use `aio_pika.abc` in your imports.

  The list of deprecated imports:
  * `from aio_pika.message import ReturnCallback`
  * `from aio_pika.patterns.rpc import RPCMessageType` - renamed to
    `RPCMessageTypes`
  * `import aio_pika.types` - module deprecated use `aio_pika.abc` instead
  * `from aio_pika.connection import ConnectionType`

6.8.2
-----

* explicit `Channel.is_user_closed` property
* user-friendly exception when channel has been closed
* reopen channels which are closed from the broker side

6.8.1
-----

* Fix flapping test test_robust_duplicate_queue #424
* Fixed callback on_close for rpc #424

6.8.0
-----

* fix: master deserialize types #366
* fix: add missing type hint on exchange publish method #370
* Return self instead of select result in `__aenter__` #373
* fix: call remove_close_callback #374

6.7.1
-----

* Fix breaking change in callback definition #344

6.7.0
-----

* Reworked tests and finally applied PR #311
* Improve documentation examples and snippets #339
* Restore RobustChannel.default_exchange on reconnect #340
* Improve the docs a bit #335


6.6.1
-----

* Add generics to Pool and PoolItemContextManager #321
* Fix Docs for ``DeliveryError`` #322

6.6.0
-----

* message.reject called inside ProcessContext.__exit__ fails when channel is closed #302

6.5.3
-----

* Add docs and github links to setup.py #304

6.5.2
-----

* Type annotation fixes
* Add documentation

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
