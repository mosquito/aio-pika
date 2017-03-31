aio-pika
========

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


Wrapper for the PIKA for asyncio and humans.


Installation
------------

.. code-block:: shell

    pip install aio-pika


Usage example
--------------

.. code-block:: python

    import asyncio
    from aio_pika import connect


    @asyncio.coroutine
    def main(loop):
        connection = yield from connect("amqp://guest:guest@127.0.0.1/", loop=loop)

        queue_name = "test_queue"
        routing_key = "test_queue"

        # Creating channel
        channel = yield from connection.channel()

        # Declaring exchange
        exchange = yield from channel.declare_exchange('direct', auto_delete=True)

        # Declaring queue
        queue = yield from channel.declare_queue(queue_name, auto_delete=True)

        # Binding queue
        yield from queue.bind(exchange, routing_key)


        yield from exchange.publish(
            Message(
                bytes('Hello', 'utf-8'),
                content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            routing_key
        )

        # Receiving message
        incoming_message = yield from queue.get(timeout=5)

        # Confirm message
        incoming_message.ack()

        yield from queue.unbind(exchange, routing_key)
        yield from queue.delete()
        yield from connection.close()


    if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main(loop))
