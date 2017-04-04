.. _aio-pika: https://github.com/mosquito/aio-pika
.. _routing:

Routing
=======

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


In the :ref:`previous tutorial <publish-subscribe>` we built a simple logging system.
We were able to broadcast log messages to many receivers.

In this tutorial we're going to add a feature to it — we're going to make it possible to subscribe only to a subset
of the messages. For example, we will be able to direct only critical error messages to the log
file (to save disk space), while still being able to print all of the log messages on the console.


Bindings
++++++++

In previous examples we were already creating bindings. You may recall code like:

.. code-block:: python

    async def main():
        ...

        # Binding the queue to the exchange
        await queue.bind(logs_exchange)

    ...


A binding is a relationship between an exchange and a queue. This can be simply read as:
the queue is interested in messages from this exchange.

Bindings can take an extra *routing_key* parameter. To avoid the confusion with a
*basic_publish* parameter we're going to call it a *binding key*.
This is how we could create a binding with a key:

.. code-block:: python

    async def main():
        ...

        # Binding the queue to the exchange
        await queue.bind(logs_exchange, routing_key="black")

    ...


The meaning of a binding key depends on the exchange type. The *fanout* exchanges, which we
used previously, simply ignored its value.

Direct exchange
+++++++++++++++

Our logging system from the previous tutorial broadcasts all messages to all consumers.
We want to extend that to allow filtering messages based on their severity. For example
we may want the script which is writing log messages to the disk to only receive critical
errors, and not waste disk space on warning or info log messages.

We were using a fanout exchange, which doesn't give us too much flexibility — it's only
capable of mindless broadcasting.

We will use a direct exchange instead. The routing algorithm behind a direct exchange
is simple — a message goes to the queues whose binding key exactly matches the routing key of the message.

To illustrate that, consider the following setup:

.. image:: static/direct-exchange.png
   :align: center

In this setup, we can see the *direct* exchange X with two queues bound to it. The first queue is
bound with binding key *orange*, and the second has two bindings, one with
binding key *black* and the other one with *green*.

In such a setup a message published to the exchange with a routing key *orange*
will be routed to queue *Q1*. Messages with a routing key of *black* or *green* will go to *Q2*.
All other messages will be discarded.


Multiple bindings
+++++++++++++++++

.. image:: static/direct-exchange-multiple.png
   :align: center

It is perfectly legal to bind multiple queues with the same binding key. In our
example we could add a binding between *X* and *Q1* with binding key *black*. In that
case, the *direct* exchange will behave like fanout and will broadcast the message
to all the matching queues. A message with routing key black will be delivered to both *Q1* and *Q2*.


Emitting logs
+++++++++++++

We'll use this model for our logging system. Instead of *fanout* we'll send messages to a *direct* exchange.
We will supply the log severity as a *routing key*. That way the receiving script will be able to select
the severity it wants to receive. Let's focus on emitting logs first.

Like always we need to create an exchange first:

.. code-block:: python

    from aio_pika import ExchangeType

    async def main():
        ...

        direct_logs_exchange = await channel.declare_exchange('logs', ExchangeType.DIRECT)

And we're ready to send a message:

.. code-block:: python

    async def main():
        ...

        await direct_logs_exchange.publish(
            Message(message_body),
            routing_key=severity',
        )

To simplify things we will assume that `'severity'` can be one of `'info'`, `'warning'`, `'error'`.

Subscribing
+++++++++++

Receiving messages will work just like in the previous tutorial, with one exception - we're
going to create a new binding for each severity we're interested in.


.. code-block:: python

    async def main():
        ...

        # Declaring queue
        queue = await channel.declare_queue(exclusive=True)

        # Binding the queue to the exchange
        await queue.bind(direct_logs_exchange, routing_key=severity)

    ...


Putting it all together
+++++++++++++++++++++++

.. image:: static/python-four.png
   :align: center

The code for *emit_log_direct.py*:

.. code-block:: python

    import sys
    import asyncio
    from aio_pika import connect, Message

    async def main(loop):
        # Perform connection
        connection = await connect("amqp://guest:guest@localhost/", loop=loop)

        # Creating a channel
        channel = await connection.channel()

        direct_logs_exchange = await channel.declare_exchange('logs', ExchangeType.DIRECT)

        severity = sys.argv[1] if len(sys.argv) > 2 else 'info'
        message_body = b' '.join(sys.argv[2:]) or b"Hello World!"

        message = Message(
            message_body,
            delivery_mode=DeliveryMode.PERSISTENT
        )

        # Sending the message
        await direct_logs_exchange.publish(message, routing_key=severity)

        print(" [x] Sent %r" % message)

        await connection.close()

    if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main(loop))


The code for *receive_logs_direct.py*:

.. code-block:: python

    import asyncio
    from aio_pika import connect, IncomingMessage


    def on_message(message: IncomingMessage):
        print(" [x] %r:%r" % (message.routing_key, message.body))


    async def main(loop):
        # Perform connection
        connection = await connect("amqp://guest:guest@localhost/", loop=loop)

        # Creating a channel
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)

        severities = sys.argv[1:]
        if not severities:
            sys.stderr.write("Usage: %s [info] [warning] [error]\n" % sys.argv[0])
            sys.exit(1)

        # Declare an exchange
        direct_logs_exchange = await channel.declare_exchange('logs', ExchangeType.DIRECT)

        # Declaring queue
        queue = await channel.declare_queue('task_queue', durable=True)

        for severity in severities:
            await queue.bind(direct_logs_exchange, routing_key=severity)

        # Start listening the queue with name 'task_queue'
        await queue.consume(on_message)


    if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.add_callback(main(loop))

        # we enter a never-ending loop that waits for data and runs callbacks whenever necessary.
        print(" [*] Waiting for messages. To exit press CTRL+C")
        loop.run_forever()


If you want to save only *'warning'* and *'error'* (and not *'info'*) log messages to a file,
just open a console and type::

    $ python receive_logs_direct.py warning error > logs_from_rabbit.log

If you'd like to see all the log messages on your screen, open a new terminal and do::

    $ python receive_logs_direct.py info warning error
     [*] Waiting for logs. To exit press CTRL+C

And, for example, to emit an error log message just type::

    $ python emit_log_direct.py error "Run. Run. Or it will explode."
    [x] Sent 'error':'Run. Run. Or it will explode.'

Move on to :ref:`tutorial 5 <topics>` to find out how to listen for messages based on a pattern.