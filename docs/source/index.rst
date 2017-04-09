.. aio-pika documentation master file, created by
   sphinx-quickstart on Fri Mar 31 17:03:20 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. _aio_pika: https://github.com/mosquito/aio-pika
.. _asyncio: https://docs.python.org/3/library/asyncio.html
.. _PIKA: https://github.com/pika/pika


Welcome to aio-pika's documentation!
====================================

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


`aio_pika`_ it's a wrapper for the `PIKA`_ for `asyncio`_ and humans.


Installation
++++++++++++

.. code-block:: shell

    pip install aio-pika


Usage example
+++++++++++++

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


Tutorial
++++++++

.. toctree::
   :maxdepth: 3
   :caption: RabbitMQ tutorial adopted for aio-pika
   :glob:

   rabbitmq-tutorial/*
   apidoc


Thanks for contributing
+++++++++++++++++++++++

* `@mosquito`_ (author)
* `@hellysmile`_ (bug fixes and smart ideas)
* `@adelkhafizova`_ (helps with documentation)

.. _@mosquito: https://github.com/mosquito
.. _@hellysmile: https://github.com/hellysmile
.. _@adelkhafizova: https://github.com/adelkhafizova
