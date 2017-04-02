.. _aio-pika: https://github.com/mosquito/aio-pika
.. _publish-subscribe:

Publish/Subscribe
=================

.. note::
    Using the `aio-pika`_ async Python client

.. note::

    **Prerequisites**

    This tutorial assumes RabbitMQ is installed_ and running on localhost on standard port (`5672`).
    In case you use a different host, port or credentials, connections settings would require adjusting.

    .. _installed: https://www.rabbitmq.com/download.html

    **Where to get help**

    If you're having trouble going through this tutorial you can `contact us`_ through the mailing list.

    .. _contact us: https://groups.google.com/forum/#!forum/rabbitmq-users

In the :ref:`previous tutorial <work-queues>` we created a work queue. The assumption behind a work
queue is that each task is delivered to exactly one worker. In this part we'll do something completely
different — we'll deliver a message to multiple consumers. This pattern is known as "publish/subscribe".

To illustrate the pattern, we're going to build a simple logging system. It will consist of two
programs — the first will emit log messages and the second will receive and print them.

In our logging system every running copy of the receiver program will get the messages.
That way we'll be able to run one receiver and direct the logs to disk; and at the same time we'll be
able to run another receiver and see the logs on the screen.

Essentially, published log messages are going to be broadcast to all the receivers.


Exchanges
+++++++++

In previous parts of the tutorial we sent and received messages to and from a queue.
Now it's time to introduce the full messaging model in Rabbit.

Let's quickly go over what we covered in the previous tutorials:

* A producer is a user application that sends messages.
* A queue is a buffer that stores messages.
* A consumer is a user application that receives messages.

The core idea in the messaging model in RabbitMQ is that the producer never sends any
messages directly to a queue. Actually, quite often the producer doesn't even know if
a message will be delivered to any queue at all.

Instead, the producer can only send messages to an exchange. An exchange is a very
simple thing. On one side it receives messages from producers and the other side it
pushes them to queues. The exchange must know exactly what to do with a message it receives.
Should it be appended to a particular queue? Should it be appended to many queues?
Or should it get discarded. The rules for that are defined by the exchange type.

.. image:: https://www.rabbitmq.com/img/tutorials/exchanges.png
   :align: center

There are a few exchange types available: `DIRECT`, `TOPIC`, `HEADERS` and `FANOUT`
(see :class:`aio_pika.ExchangeType`).
We'll focus on the last one — the fanout. Let's create an exchange of that type, and call it `logs`:

.. code-block:: python

    from aio_pika import ExchangeType

    async def main():
        ...

        await channel.declare_exchange('logs', ExchangeType.FANOUT)

