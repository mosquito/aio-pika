.. _issue: https://github.com/mosquito/aio-pika/issues
.. _pull request: https://github.com/mosquito/aio-pika/compare
.. _aio-pika: https://github.com/mosquito/aio-pika
.. _official tutorial: https://www.rabbitmq.com/tutorials/tutorial-seven-php.html
.. _publisher-confirms:

Publisher Confirms
==================

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


`Publisher confirms <https://www.rabbitmq.com/confirms.html#publisher-confirms>`_ are a RabbitMQ
extension to implement reliable publishing.
When publisher confirms are enabled on a channel, messages the client publishes are confirmed
asynchronously by the broker, meaning they have been taken care of on the server side.

Overview
++++++++

In this tutorial we're going to use publisher confirms to make sure published messages have safely reached the broker.
We will cover several strategies to using publisher confirms and explain their pros and cons.

Enabling Publisher Confirms on a Channel
++++++++++++++++++++++++++++++++++++++++

Publisher confirms are a RabbitMQ extension to the AMQP 0.9.1 protocol.
Publisher confirms are enabled at the channel level by setting the :code:`publisher_confirms` parameter to :code:`True`,
which is the default.

.. code-block:: python

   channel = await connection.channel(
      publisher_confirms=True, # This is the default
   )

Strategy #1: Publishing Messages Individually
+++++++++++++++++++++++++++++++++++++++++++++

Let's start with the simplest approach to publishing with confirms, that is, publishing a message and
waiting synchronously for its confirmation:

.. literalinclude:: examples/7-publisher-confirms/publish_individually.py
   :language: python
   :start-at: # Sending the messages
   :end-before: # Done sending messages

In the previous example we publish a message as usual and wait for its confirmation with the :code:`await` keyword.
The :code:`await` returns as soon as the message has been confirmed.
If the message is not confirmed within the timeout or if it is nack-ed (meaning the broker could not take care of it for
some reason), the :code:`await` will throw an exception.
The :code:`on_return_raises` parameter of :code:`aio_pika.connect()` and :code:`connection.channel()` controls this behaivior for if a mandatory
message is returned.
The handling of the exception usually consists in logging an error message and/or retrying to send the message.

Different client libraries have different ways to synchronously deal with publisher confirms, so make sure to read
carefully the documentation of the client you are using.

This technique is very straightforward but also has a major drawback: it **significantly slows down publishing**, as the
confirmation of a message blocks the publishing of all subsequent messages.
This approach is not going to deliver throughput of more than a few hundreds of published messages per second.
Nevertheless, this can be good enough for some applications.

Strategy #2: Publishing Messages in Batches
+++++++++++++++++++++++++++++++++++++++++++

To improve upon our previous example, we can publish a batch of messages and wait for this whole batch to be confirmed.
The following example uses a batch of 100:

.. literalinclude:: examples/7-publisher-confirms/publish_batches.py
   :language: python
   :start-at: batchsize = 100
   :end-before: # Done sending messages

Waiting for a batch of messages to be confirmed improves throughput drastically over waiting for a confirm for individual
message (up to 20-30 times with a remote RabbitMQ node).
One drawback is that we do not know exactly what went wrong in case of failure, so we may have to keep a whole batch in memory
to log something meaningful or to re-publish the messages.
And this solution is still synchronous, so it blocks the publishing of messages.

.. note::

   To initiate message sending asynchronously, a task is created with :code:`asyncio.create_task`, so the execution of our function
   is handled by the event-loop.
   The :code:`await asyncio.sleep(0)` is required to make the event loop switch to our coroutine.
   Any :code:`await` would have sufficed, though.
   Using :code:`async for` with an :code:`async` generator also requires the generator to yield control flow with :code:`await` for message
   sending to be initiated.

   Without the task and the :code:`await` the message sending would only be initiated with the :code:`asyncio.gather` call.
   For some applications this behaivior might be acceptable.


Strategy #3: Handling Publisher Confirms Asynchronously
+++++++++++++++++++++++++++++++++++++++++++++++++++++++

The broker confirms published messages asynchronously, our helper function will publish the messages and be notified of these confirms:

.. literalinclude:: examples/7-publisher-confirms/publish_asynchronously.py
   :language: python
   :start-at: with asyncio.TaskGroup
   :end-at: asyncio.sleep(0)

The :code:`TaskGroup` is required to ensure that all tasks are awaited properly.

The helper function publishes the message and awaits the confirmation.
This way the helper function knows which message the confirmation, timeout or rejection belongs to.

.. literalinclude:: examples/7-publisher-confirms/publish_asynchronously.py
   :language: python
   :pyobject: publish_and_handle_confirm


Summary
+++++++

Making sure published messages made it to the broker can be essential in some applications.
Publisher confirms are a RabbitMQ feature that helps to meet this requirement.
Publisher confirms are asynchronous in nature but it is also possible to handle them synchronously.
There is no definitive way to implement publisher confirms, this usually comes down to the constraints in the application
and in the overall system. Typical techniques are:

* publishing messages individually, waiting for the confirmation synchronously: simple, but very limited throughput.
* publishing messages in batch, waiting for the confirmation synchronously for a batch: simple, reasonable throughput, but hard to reason about when something goes wrong.
* asynchronous handling: best performance and use of resources, good control in case of error, but can be involved to implement correctly.



.. note::

    This material was adopted from `official tutorial`_ on **rabbitmq.org**.
