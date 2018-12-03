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

.. image:: https://travis-ci.org/mosquito/aio-pika.svg
    :target: https://travis-ci.org/mosquito/aio-pika
    :alt: Travis CI

.. image:: https://img.shields.io/pypi/v/aio-pika.svg
    :target: https://pypi.python.org/pypi/aio-pika/
    :alt: Latest Version

.. image:: https://img.shields.io/pypi/wheel/aio-pika.svg
    :target: https://pypi.python.org/pypi/aio-pika/

.. image:: https://img.shields.io/pypi/pyversions/aio-pika.svg
    :target: https://pypi.python.org/pypi/aio-pika/

.. image:: https://img.shields.io/pypi/l/aio-pika.svg
    :target: https://pypi.python.org/pypi/aio-pika/


Wrapper for the PIKA for asyncio and humans. See examples and the tutorial in `documentation`_.

If you are newcomer in the RabbitMQ let's start the `adopted official RabbitMQ tutorial`_


Features
--------

* Completely asynchronous API.
* Object oriented API.
* Transparent auto-reconnects with complete state recovery with `connect_robust`
  (e.g. declared queues or exchanges, consuming state and bindings).
* Python 3.5+ compatible (include 3.7).
* For python 3.4 users available `aio-pika<4`
* Transparent `publisher confirms`_ support
* `Transactions`_ support


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


    async def main(loop):
        connection = await aio_pika.connect_robust(
            "amqp://guest:guest@127.0.0.1/", loop=loop
        )

        async with connection:
            queue_name = "test_queue"

            # Creating channel
            channel = await connection.channel()    # type: aio_pika.Channel

            # Declaring queue
            queue = await channel.declare_queue(
                queue_name,
                auto_delete=True
            )   # type: aio_pika.Queue

            async for message in queue:
                with message.process():
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


    async def main(loop):
        connection = await aio_pika.connect_robust(
            "amqp://guest:guest@127.0.0.1/", loop=loop
        )

        routing_key = "test_queue"

        channel = await connection.channel()    # type: aio_pika.Channel

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
        incoming_message.ack()

        await queue.unbind(exchange, routing_key)
        await queue.delete()
        await connection.close()


    if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main(loop))


See another examples and the tutorial in `documentation`_.


Versioning
==========

This software follows `Semantic Versioning`_


For contributors
----------------

You feel free to create pull request, but you should describe your cases and add some examples.

The changes should follow simple rules:

* When your changes breaks public API you must increase the major version.
* When your changes is safe for public API (e.g. added an argument with default value)
* You have to add test cases (see `tests/` folder)
* You must add docstrings
* You feel free to add yourself to `"thank's to" section`_


.. _"thank's to" section: https://github.com/mosquito/aio-pika/blob/master/docs/source/index.rst#thanks-for-contributing
.. _Semantic Versioning: http://semver.org/
