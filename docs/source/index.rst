.. aio-pika documentation master file, created by
   sphinx-quickstart on Fri Mar 31 17:03:20 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to aio-pika's documentation!
====================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

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


`aio_pika`_ it's a wrapper for the `PIKA`_ for asyncio and humans.


.. _aio_pika: https://github.com/mosquito/aio-pika
.. _PIKA: https://github.com/pika/pika


Installation
------------

.. code-block:: shell

    pip install aio-pika


Usage example
-------------

.. code-block:: python

    from aio_pika import connect


    async def main(loop):
        connection = await connect("amqp://guest:guest@127.0.0.1/", loop=loop)

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


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
