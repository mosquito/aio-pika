.. _documentation: https://aio-pika.readthedocs.org/
.. _adopted official RabbitMQ tutorial: https://aio-pika.readthedocs.io/en/latest/rabbitmq-tutorial/1-introduction.html

aio-pika
========

.. image:: https://readthedocs.org/projects/aio-pika/badge/?version=latest
    :target: https://aio-pika.readthedocs.org/
    :alt: ReadTheDocs

.. image:: https://coveralls.io/repos/github/mosquito/aio-pika/badge.svg?branch=master
    :target: https://coveralls.io/github/mosquito/aio-pika
    :alt: Coveralls

.. image:: https://github.com/mosquito/aio-pika/workflows/tox/badge.svg
    :target: https://github.com/mosquito/aio-pika/actions?query=workflow%3Atox
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


A wrapper around `aiormq`_ for asyncio and humans.

Check out the examples and the tutorial in the `documentation`_.

If you are a newcomer to RabbitMQ, please start with the `adopted official RabbitMQ tutorial`_.

.. _aiormq: http://github.com/mosquito/aiormq/

.. note::
   Since version ``5.0.0`` this library doesn't use ``pika`` as AMQP connector.
   Versions below ``5.0.0`` contains or requires ``pika``'s source code.

.. note::
   The version 7.0.0 has breaking API changes, see CHANGELOG.md
   for migration hints.


Features
--------

* Completely asynchronous API.
* Object oriented API.
* Transparent auto-reconnects with complete state recovery with `connect_robust`
  (e.g. declared queues or exchanges, consuming state and bindings).
* Python 3.7+ compatible.
* For python 3.5 users available `aio-pika<7`
* Transparent `publisher confirms`_ support
* `Transactions`_ support
* Completely type-hints coverage.


.. _Transactions: https://www.rabbitmq.com/semantics.html
.. _publisher confirms: https://www.rabbitmq.com/confirms.html


Installation
------------

.. code-block:: shell

    pip install aio-pika


Usage example
-------------

Simple consumer:

.. code-block:: python

    import asyncio
    import aio_pika
    import aio_pika.abc


    async def main(loop):
        # Connect with the givien parameters is also valiable.
        # aio_pika.connect_robust(host="host", login="login", password="password")
        # You can only choose one option to create a connection, url or kw-based params.
        connection = await aio_pika.connect_robust(
            "amqp://guest:guest@127.0.0.1/", loop=loop
        )

        async with connection:
            queue_name = "test_queue"

            # Creating channel
            channel: aio_pika.abc.AbstractChannel = await connection.channel()

            # Declaring queue
            queue: aio_pika.abc.AbstractQueue = await channel.declare_queue(
                queue_name,
                auto_delete=True
            )

            async with queue.iterator() as queue_iter:
                # Cancel consuming after __aexit__
                async for message in queue_iter:
                    async with message.process():
                        print(message.body)

                        if queue.name in message.body.decode():
                            break


    if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main(loop))
        loop.close()

Simple publisher:

.. code-block:: python

    import asyncio
    import aio_pika
    import aio_pika.abc


    async def main(loop):
        # Explicit type annotation
        connection: aio_pika.RobustConnection = await aio_pika.connect_robust(
            "amqp://guest:guest@127.0.0.1/", loop=loop
        )

        routing_key = "test_queue"

        channel: aio_pika.abc.AbstractChannel = await connection.channel()

        await channel.default_exchange.publish(
            aio_pika.Message(
                body='Hello {}'.format(routing_key).encode()
            ),
            routing_key=routing_key
        )

        await connection.close()


    if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main(loop))
        loop.close()


Get single message example:

.. code-block:: python

    import asyncio
    from aio_pika import connect_robust, Message


    async def main(loop):
        connection = await connect_robust(
            "amqp://guest:guest@127.0.0.1/",
            loop=loop
        )

        queue_name = "test_queue"
        routing_key = "test_queue"

        # Creating channel
        channel = await connection.channel()

        # Declaring exchange
        exchange = await channel.declare_exchange('direct', auto_delete=True)

        # Declaring queue
        queue = await channel.declare_queue(queue_name, auto_delete=True)

        # Binding queue
        await queue.bind(exchange, routing_key)

        await exchange.publish(
            Message(
                bytes('Hello', 'utf-8'),
                content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            routing_key
        )

        # Receiving message
        incoming_message = await queue.get(timeout=5)

        # Confirm message
        await incoming_message.ack()

        await queue.unbind(exchange, routing_key)
        await queue.delete()
        await connection.close()


    if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main(loop))


There are more examples and the RabbitMQ tutorial in the `documentation`_.


Versioning
==========

This software follows `Semantic Versioning`_


For contributors
----------------

Setting up development environment
__________________________________

Clone the project:

.. code-block:: shell

    git clone https://github.com/mosquito/aio-pika.git
    cd aio-pika

Create a new virtualenv for `aio-pika`_:

.. code-block:: shell

    python3 -m venv env
    source env/bin/activate

Install all requirements for `aio-pika`_:

.. code-block:: shell

    pip install -e '.[develop]'


Running Tests
_____________

**NOTE: In order to run the tests locally you need to run a RabbitMQ instance with default user/password (guest/guest) and port (5672).**

* ProTip: Use Docker for this:

.. code-block:: bash

    docker run -d -p 5671:5671 -p 5672:5672 -p 15671:15671 -p 15672:15672 mosquito/aiormq-rabbitmq

To test just run:

.. code-block:: bash

    make test


Editing Documentation
_____________________

To iterate quickly on the documentation live in your browser, try:

.. code-block:: bash

    nox -s docs -- serve

Creating Pull Requests
______________________

Please feel free to create pull requests, but you should describe your use cases and add some examples.

Changes should follow a few simple rules:

* When your changes break the public API, you must increase the major version.
* When your changes are safe for public API (e.g. added an argument with default value)
* You have to add test cases (see `tests/` folder)
* You must add docstrings
* Feel free to add yourself to `"thank's to" section`_


.. _"thank's to" section: https://github.com/mosquito/aio-pika/blob/master/docs/source/index.rst#thanks-for-contributing
.. _Semantic Versioning: http://semver.org/
.. _aio-pika: https://github.com/mosquito/aio-pika/
