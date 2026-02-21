.. aio-pika documentation master file, created by
   sphinx-quickstart on Fri Mar 31 17:03:20 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. _aio-pika: https://github.com/mosquito/aio-pika
.. _asyncio: https://docs.python.org/3/library/asyncio.html
.. _aiormq: http://github.com/mosquito/aiormq/


Welcome to aio-pika's documentation!
====================================

.. image:: https://coveralls.io/repos/github/mosquito/aio-pika/badge.svg?branch=master
    :target: https://coveralls.io/github/mosquito/aio-pika
    :alt: Coveralls

.. image:: https://github.com/mosquito/aio-pika/workflows/tests/badge.svg
    :target: https://github.com/mosquito/aio-pika/actions?query=workflow%3Atests
    :alt: Github Actions

.. image:: https://img.shields.io/pypi/v/aio-pika.svg
    :target: https://pypi.python.org/pypi/aio-pika/
    :alt: Latest Version

.. image:: https://img.shields.io/pypi/wheel/aio-pika.svg
    :target: https://pypi.python.org/pypi/aio-pika/

.. image:: https://img.shields.io/pypi/pyversions/aio-pika.svg
    :target: https://pypi.python.org/pypi/aio-pika/

.. image:: https://img.shields.io/pypi/l/aio-pika.svg
    :target: https://pypi.python.org/pypi/aio-pika/


`aio-pika`_ is a wrapper for the `aiormq`_ for `asyncio`_ and humans.


Features
++++++++

* Completely asynchronous API.
* Object oriented API.
* Transparent auto-reconnects with complete state recovery with `connect_robust`
  (e.g. declared queues or exchanges, consuming state and bindings).
* Python 3.10+ compatible.
* Transparent `publisher confirms`_ support
* `Transactions`_ support
* Completely type-hints coverage.

.. _publisher confirms: https://www.rabbitmq.com/confirms.html
.. _Transactions: https://www.rabbitmq.com/semantics.html#tx

AMQP URL parameters
+++++++++++++++++++

URL is the supported way to configure connection.
For customisation of connection behaviour you might
pass the parameters in URL query-string like format.

This article describes a description for these parameters.

``aiormq`` specific
~~~~~~~~~~~~~~~~~~~

* ``name`` (``str`` url encoded) - A string that will be visible in the RabbitMQ management console and in
  the server logs, convenient for diagnostics.

* ``cafile`` (``str``) - Path to Certificate Authority file

* ``capath`` (``str``) - Path to Certificate Authority directory

* ``cadata`` (``str`` url encoded) - URL encoded CA certificate content

* ``keyfile`` (``str``) - Path to client ssl private key file

* ``certfile`` (``str``) - Path to client ssl certificate file

* ``no_verify_ssl`` - No verify server SSL certificates. ``0`` by default and means ``False`` other value means
  ``True``.

* ``heartbeat`` (``int``-like) - interval in seconds between AMQP heartbeat packets. ``0`` disables this feature.


``aio_pika.connect`` function and ``aio_pika.Connection`` class specific
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* ``interleave`` (``int``-like) - controls address reordering when a host name resolves to multiple
  IP addresses. If 0 or unspecified, no reordering is done, and addresses are tried
  in the order returned by ``getaddrinfo()``. If a positive integer is specified,
  the addresses are interleaved by address family, and the given integer is interpreted
  as "First Address Family Count" as defined in `RFC 8305`_. The default is ``0`` if
  ``happy_eyeballs_delay`` is not specified, and ``1`` if it is.

  .. note::

      Really useful for RabbitMQ clusters with one DNS name with many ``A``/``AAAA`` records.

  .. warning::

      This option is supported by ``asyncio.DefaultEventLoopPolicy`` and available since python 3.8.

* ``happy_eyeballs_delay`` (``float``-like) - if given, enables Happy Eyeballs for this connection.
  It should be a floating-point number representing the amount of time in seconds to wait for a connection attempt
  to complete, before starting the next attempt in parallel. This is the "Connection Attempt Delay" as defined in
  `RFC 8305`_. A sensible default value recommended by the RFC is ``0.25`` (250 milliseconds).

  .. note::

      Really useful for RabbitMQ clusters with one DNS name with many ``A``/``AAAA`` records.

  .. warning::

      This option is supported by ``asyncio.DefaultEventLoopPolicy`` and available since python 3.8.

``aio_pika.connect_robust`` function and ``aio_pika.RobustConnection`` class specific
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For ``aio_pika.RobustConnection`` class is applicable all ``aio_pika.Connection`` related parameters like,
``name``/``interleave``/``happy_eyeballs_delay`` and some specific:


* ``reconnect_interval`` (``float``-like) - is the period in seconds, not more often than the attempts to
  re-establish the connection will take place.


* ``fail_fast`` (``true``/``yes``/``y``/``enable``/``on``/``enabled``/``1`` means ``True``, otherwise ``False``) -
  special behavior for the start connection attempt, if it fails, all other attempts stops and an exception will be
  thrown at the connection stage. Enabled by default, if you are sure you need to disable this feature, be ensures
  for the passed URL is really working. Otherwise, your program will go into endless reconnection attempts that can
  not be successed.

.. _RFC 8305: https://datatracker.ietf.org/doc/html/rfc8305.html


URL examples
~~~~~~~~~~~~

* ``amqp://username:password@hostname/vhost?name=connection%20name&heartbeat=60&happy_eyeballs_delay=0.25``

* ``amqps://username:password@hostname/vhost?reconnect_interval=5&fail_fast=1``

* ``amqps://username:password@hostname/vhost?cafile=/path/to/ca.pem``

* ``amqps://username:password@hostname/vhost?cafile=/path/to/ca.pem&keyfile=/path/to/key.pem&certfile=/path/to/sert.pem``


Installation
++++++++++++

Installation with pip:

.. code-block:: shell

    pip install aio-pika


Installation from git:

.. code-block:: shell

    # via pip
    pip install https://github.com/mosquito/aio-pika/archive/master.zip

    # manually
    git clone https://github.com/mosquito/aio-pika.git
    cd aio-pika
    python setup.py install


Development
+++++++++++

Clone the project:

.. code-block:: shell

    git clone https://github.com/mosquito/aio-pika.git
    cd aio-pika


Install uv if you haven't already:

.. code-block:: shell

    curl -LsSf https://astral.sh/uv/install.sh | sh

Install all requirements for `aio-pika`_:

.. code-block:: shell

    uv sync

Table Of Contents
+++++++++++++++++

.. toctree::
   :glob:
   :maxdepth: 3

   quick-start
   patterns
   rabbitmq-tutorial/index
   apidoc


Thanks for contributing
+++++++++++++++++++++++

* `@mosquito`_ (author)
* `@decaz`_ (steel persuasiveness while code review)
* `@heckad`_ (bug fixes)
* `@smagafurov`_ (bug fixes)
* `@hellysmile`_ (bug fixes and ideas)
* `@altvod`_ (bug fixes)
* `@alternativehood`_ (bugfixes)
* `@cprieto`_ (bug fixes)
* `@akhoronko`_ (bug fixes)
* `@iselind`_ (bug fixes)
* `@DXist`_ (bug fixes)
* `@blazewicz`_ (bug fixes)
* `@chibby0ne`_ (bug fixes)
* `@jmccarrell`_ (bug fixes)
* `@taybin`_ (bug fixes)
* `@ollamh`_ (bug fixes)
* `@DriverX`_ (bug fixes)
* `@brianmedigate`_ (bug fixes)
* `@dan-stone`_ (bug fixes)
* `@Kludex`_ (bug fixes)
* `@bmario`_ (bug fixes)
* `@tzoiker`_ (bug fixes)
* `@Pehat`_ (bug fixes)
* `@WindowGenerator`_ (bug fixes)
* `@dhontecillas`_ (bug fixes)
* `@tilsche`_ (bug fixes)
* `@leenr`_ (bug fixes)
* `@la0rg`_ (bug fixes)
* `@SolovyovAlexander`_ (bug fixes)
* `@kremius`_ (bug fixes)
* `@zyp`_ (bug fixes)
* `@kajetanj`_ (bug fixes)
* `@Alviner`_ (moral support, debug sessions and good mood)
* `@Pavkazzz`_ (composure, and patience while debug sessions)
* `@bbrodriges`_ (supplying grammar while writing documentation)
* `@dizballanze`_ (review, grammar)

.. _@mosquito: https://github.com/mosquito
.. _@decaz: https://github.com/decaz
.. _@heckad: https://github.com/heckad
.. _@smagafurov: https://github.com/smagafurov
.. _@hellysmile: https://github.com/hellysmile
.. _@altvod: https://github.com/altvod
.. _@alternativehood: https://github.com/alternativehood
.. _@cprieto: https://github.com/cprieto
.. _@akhoronko: https://github.com/akhoronko
.. _@iselind: https://github.com/iselind
.. _@DXist: https://github.com/DXist
.. _@blazewicz: https://github.com/blazewicz
.. _@chibby0ne: https://github.com/chibby0ne
.. _@jmccarrell: https://github.com/jmccarrell
.. _@taybin: https://github.com/taybin
.. _@ollamh: https://github.com/ollamh
.. _@DriverX: https://github.com/DriverX
.. _@brianmedigate: https://github.com/brianmedigate
.. _@dan-stone: https://github.com/dan-stone
.. _@Kludex: https://github.com/Kludex
.. _@bmario: https://github.com/bmario
.. _@tzoiker: https://github.com/tzoiker
.. _@Pehat: https://github.com/Pehat
.. _@WindowGenerator: https://github.com/WindowGenerator
.. _@dhontecillas: https://github.com/dhontecillas
.. _@tilsche: https://github.com/tilsche
.. _@leenr: https://github.com/leenr
.. _@la0rg: https://github.com/la0rg
.. _@SolovyovAlexander: https://github.com/SolovyovAlexander
.. _@kremius: https://github.com/kremius
.. _@zyp: https://github.com/zyp
.. _@kajetanj: https://github.com/kajetanj
.. _@Alviner: https://github.com/Alviner
.. _@Pavkazzz: https://github.com/Pavkazzz
.. _@bbrodriges: https://github.com/bbrodriges
.. _@dizballanze: https://github.com/dizballanze


See also
++++++++

`aiormq`_
~~~~~~~~~

`aiormq` is a pure python AMQP client library. It is under the hood of **aio-pika** and might to be used when you really loving works with the protocol low level.
Following examples demonstrates the user API.

Simple consumer:

.. code-block:: python

    import asyncio
    import aiormq

    async def on_message(message):
        """
        on_message doesn't necessarily have to be defined as async.
        Here it is to show that it's possible.
        """
        print(f" [x] Received message {message!r}")
        print(f"Message body is: {message.body!r}")
        print("Before sleep!")
        await asyncio.sleep(5)   # Represents async I/O operations
        print("After sleep!")

    async def main():
        # Perform connection
        connection = await aiormq.connect("amqp://guest:guest@localhost/")

        # Creating a channel
        channel = await connection.channel()

        # Declaring queue
        declare_ok = await channel.queue_declare('helo')
        consume_ok = await channel.basic_consume(
            declare_ok.queue, on_message, no_ack=True
        )

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()

Simple publisher:

.. code-block:: python

    import asyncio
    from typing import Optional

    import aiormq
    from aiormq.abc import DeliveredMessage

    MESSAGE: Optional[DeliveredMessage] = None

    async def main():
        global MESSAGE
        body = b'Hello World!'

        # Perform connection
        connection = await aiormq.connect("amqp://guest:guest@localhost//")

        # Creating a channel
        channel = await connection.channel()
        declare_ok = await channel.queue_declare("hello", auto_delete=True)

        # Sending the message
        await channel.basic_publish(body, routing_key='hello')
        print(f" [x] Sent {body}")

        MESSAGE = await channel.basic_get(declare_ok.queue)
        print(f" [x] Received message from {declare_ok.queue!r}")

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

    assert MESSAGE is not None
    assert MESSAGE.routing_key == "hello"
    assert MESSAGE.body == b'Hello World!'

The `patio`_ and the `patio-rabbitmq`_
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**PATIO** is an acronym for Python Asynchronous Tasks for AsyncIO - an easily extensible library, for distributed task execution, like celery, only targeting asyncio as the main design approach.

**patio-rabbitmq** provides you with the ability to use *RPC over RabbitMQ* services with extremely simple implementation:

.. code-block:: python

   from patio import Registry, ThreadPoolExecutor
   from patio_rabbitmq import RabbitMQBroker

   rpc = Registry(project="patio-rabbitmq", auto_naming=False)

   @rpc("sum")
   def sum(*args):
       return sum(args)

   async def main():
       async with ThreadPoolExecutor(rpc, max_workers=16) as executor:
           async with RabbitMQBroker(
               executor, amqp_url="amqp://guest:guest@localhost/",
           ) as broker:
               await broker.join()

And the caller side might be written like this:

.. code-block:: python

    import asyncio
    from patio import NullExecutor, Registry
    from patio_rabbitmq import RabbitMQBroker

    async def main():
        async with NullExecutor(Registry(project="patio-rabbitmq")) as executor:
            async with RabbitMQBroker(
                executor, amqp_url="amqp://guest:guest@localhost/",
            ) as broker:
                print(await asyncio.gather(
                    *[
                        broker.call("mul", i, i, timeout=1) for i in range(10)
                     ]
                ))


`FastStream`_
~~~~~~~~~~~~~

**FastStream** is a powerful and easy-to-use Python library for building asynchronous services that interact with event streams..

If you need no deep dive into **RabbitMQ** details, you can use more high-level **FastStream** interfaces:

.. code-block:: python

   from faststream import FastStream
   from faststream.rabbit import RabbitBroker

   broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
   app = FastStream(broker)

   @broker.subscriber("user")
   async def user_created(user_id: int):
       assert isinstance(user_id, int)
       return f"user-{user_id}: created"

   @app.after_startup
   async def pub_smth():
       assert (
           await broker.publish(1, "user", rpc=True)
       ) ==  "user-1: created"

Also, **FastStream** validates messages by **pydantic**, generates your project **AsyncAPI** spec, supports In-Memory testing, RPC calls, and more.

In fact, it is a high-level wrapper on top of **aio-pika**, so you can use both of these libraries' advantages at the same time.

`python-socketio`_
~~~~~~~~~~~~~~~~~~

`Socket.IO`_ is a transport protocol that enables real-time bidirectional event-based communication between clients (typically, though not always, web browsers) and a server. This package provides Python implementations of both, each with standard and asyncio variants.

Also this package is suitable for building messaging services over **RabbitMQ** via **aio-pika** adapter:

.. code-block:: python

   import socketio
   from aiohttp import web

   sio = socketio.AsyncServer(client_manager=socketio.AsyncAioPikaManager())
   app = web.Application()
   sio.attach(app)

   @sio.event
   async def chat_message(sid, data):
       print("message ", data)

   if __name__ == '__main__':
       web.run_app(app)

And a client is able to call `chat_message` the following way:

.. code-block:: python

   import asyncio
   import socketio

   sio = socketio.AsyncClient()

   async def main():
       await sio.connect('http://localhost:8080')
       await sio.emit('chat_message', {'response': 'my response'})

   if __name__ == '__main__':
       asyncio.run(main())

The `taskiq`_ and the `taskiq-aio-pika`_
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Taskiq** is an asynchronous distributed task queue for python. The project takes inspiration from big projects such as Celery and Dramatiq. But taskiq can send and run both the sync and async functions.

The library provides you with **aio-pika** broker for running tasks too.

.. code-block:: python

   from taskiq_aio_pika import AioPikaBroker

   broker = AioPikaBroker()

   @broker.task
   async def test() -> None:
       print("nothing")

   async def main():
       await broker.startup()
       await test.kiq()

`Rasa`_
~~~~~~~

With over 25 million downloads, Rasa Open Source is the most popular open source framework for building chat and voice-based AI assistants.

With **Rasa**, you can build contextual assistants on:

* Facebook Messenger
* Slack
* Google Hangouts
* Webex Teams
* Microsoft Bot Framework
* Rocket.Chat
* Mattermost
* Telegram
* Twilio

Your own custom conversational channels or voice assistants as:

* Alexa Skills
* Google Home Actions

**Rasa** helps you build contextual assistants capable of having layered conversations with lots of back-and-forth. In order for a human to have a meaningful exchange with a contextual assistant, the assistant needs to be able to use context to build on things that were previously discussed – **Rasa** enables you to build assistants that can do this in a scalable way.

And it also uses **aio-pika** to interact with **RabbitMQ** deep inside!

Versioning
==========

This software follows `Semantic Versioning`_


.. _Semantic Versioning: http://semver.org/
.. _faststream: https://github.com/airtai/faststream
.. _patio: https://github.com/patio-python/patio
.. _patio-rabbitmq: https://github.com/patio-python/patio-rabbitmq
.. _Socket.IO: https://socket.io/
.. _python-socketio: https://python-socketio.readthedocs.io/en/latest/intro.html
.. _taskiq: https://github.com/taskiq-python/taskiq
.. _taskiq-aio-pika: https://github.com/taskiq-python/taskiq-aio-pika
.. _Rasa: https://rasa.com/docs/rasa/
