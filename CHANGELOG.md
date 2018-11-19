4.6.3
-----

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
