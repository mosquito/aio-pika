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
