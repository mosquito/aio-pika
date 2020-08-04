.. _issue: https://github.com/mosquito/aio-pika/issues
.. _pull request: https://github.com/mosquito/aio-pika/compare
.. _aio-pika: https://github.com/mosquito/aio-pika
.. _official tutorial: https://www.rabbitmq.com/tutorials/tutorial-three-python.html
.. _publish-subscribe:

Publish/Subscribe
=================

.. warning::

    This is a beta version of the port from `official tutorial`_. Please when you found an
    error create `issue`_ or `pull request`_ for me.


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

.. image:: /_static/tutorial/exchanges.png
   :align: center

There are a few exchange types available: `DIRECT`, `TOPIC`, `HEADERS` and `FANOUT`
(see :class:`aio_pika.ExchangeType`).
We'll focus on the last one — the fanout. Let's create an exchange of that type, and call it `logs`:

.. literalinclude:: examples/3-publish-subscribe/emit_log.py
   :language: python
   :lines: 13

The fanout exchange is very simple. As you can probably guess from the name, it just broadcasts
all the messages it receives to all the queues it knows. And that's exactly what we need for our logger.

.. note::

    **Listing exchanges**

    To list the exchanges on the server you can run the ever useful rabbitmqctl::

        $ sudo rabbitmqctl list_exchanges
        Listing exchanges ...
        logs      fanout
        amq.direct      direct
        amq.topic       topic
        amq.fanout      fanout
        amq.headers     headers
        ...done.

    In this list there are some `amq.*` exchanges and the default (unnamed) exchange.
    These are created by default, but it is unlikely you'll need to use them at the moment.

    **Nameless exchange**

    In previous parts of the tutorial we knew nothing about exchanges, but still were able to
    send messages to queues. That was possible because we were using a default exchange,
    which we identify by the empty string ("").

    Recall how we published a message before:

    .. code-block:: python

        await channel.default_exchange.publish(
            Message(message_body),
            routing_key='hello',
        )

    The exchange parameter is the name of the exchange. The empty string denotes the
    default or nameless exchange: messages are routed to the queue with the name specified
    by routing_key, if it exists.


Now, we can publish to our named exchange instead:

.. literalinclude:: examples/3-publish-subscribe/emit_log.py
   :language: python
   :lines: 20


Temporary queues
++++++++++++++++

As you may remember previously we were using queues which had a specified name
(remember `hello` and `task_queue`?). Being able to name a queue was crucial for us — we needed to point
the workers to the same queue. Giving a queue a name is important when you want to share the
queue between producers and consumers.

But that's not the case for our logger. We want to hear about all log messages, not just a subset
of them. We're also interested only in currently flowing messages not in the old ones. To solve
that we need two things.

Firstly, whenever we connect to Rabbit we need a fresh, empty queue. To do it we could create a
queue with a random name, or, even better - let the server choose a random queue name for us.
We can do this by not supplying the queue parameter to `declare_queue`:

.. code-block:: python

    queue = await channel.declare_queue()

Secondly, once we disconnect the consumer the queue should be deleted. There's an exclusive flag for that:

.. literalinclude:: examples/3-publish-subscribe/receive_logs.py
   :language: python
   :lines: 28-29

Bindings
++++++++

.. image:: /_static/tutorial/bindings.png
   :align: center

We've already created a fanout exchange and a queue. Now we need to tell the exchange to
send messages to our queue. That relationship between exchange and a queue is called a binding.

.. literalinclude:: examples/3-publish-subscribe/receive_logs.py
   :language: python
   :lines: 22-29

From now on the logs exchange will append messages to our queue.


.. note::

    **Listing bindings**

    You can list existing bindings using, you guessed it, `rabbitmqctl list_bindings`.


Putting it all together
+++++++++++++++++++++++

.. image:: /_static/tutorial/python-three-overall.png
   :align: center

The producer program, which emits log messages, doesn't look much different from the previous tutorial.
The most important change is that we now want to publish messages to our logs exchange instead
of the nameless one. We need to supply a routing_key when sending, but its value is ignored
for fanout exchanges.
Here goes the code for :download:`emit_log.py <examples/3-publish-subscribe/emit_log.py>` script:


.. literalinclude:: examples/3-publish-subscribe/emit_log.py
   :language: python


As you see, after establishing the connection we declared the exchange. This step is
necessary as publishing to a non-existing exchange is forbidden.

The messages will be lost if no queue is bound to the exchange yet, but that's okay for
us; if no consumer is listening yet we can safely discard the message.

The code for :download:`receive_logs.py <examples/3-publish-subscribe/receive_logs.py>` script:

.. literalinclude:: examples/3-publish-subscribe/receive_logs.py
   :language: python


We're done. If you want to save logs to a file, just open a console and type::

    $ python receive_logs.py > logs_from_rabbit.log

If you wish to see the logs on your screen, spawn a new terminal and run::

    $ python receive_logs.py

And of course, to emit logs type::

    $ python emit_log.py

Using *rabbitmqctl list_bindings* you can verify that the code actually creates bindings and
queues as we want. With two *receive_logs.py* programs running you should see something like::

    $ sudo rabbitmqctl list_bindings
    Listing bindings ...
    logs    exchange        amq.gen-JzTY20BRgKO-HjmUJj0wLg  queue           []
    logs    exchange        amq.gen-vso0PVvyiRIL2WoV3i48Yg  queue           []
    ...done.

The interpretation of the result is straightforward: data from exchange logs goes to two queues
with server-assigned names. And that's exactly what we intended.

To find out how to listen for a subset of messages, let's move on to :ref:`tutorial 4 <routing>`


.. note::

    This material was adopted from `official tutorial`_ on **rabbitmq.org**.
