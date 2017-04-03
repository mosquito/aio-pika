.. _aio-pika: https://github.com/mosquito/aio-pika
.. _rpc:

Remote procedure call (RPC)
===========================

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


In the :ref:`second tutorial <work-queues>` we learned how to use *Work Queues* to distribute
time-consuming tasks among multiple workers.

But what if we need to run a function on a remote computer and wait for the result? Well, that's a
different story. This pattern is commonly known as *Remote Procedure Call or RPC*.

In this tutorial we're going to use RabbitMQ to build an RPC system: a client and a scalable RPC server.
As we don't have any time-consuming tasks that are worth distributing, we're going to create a dummy
RPC service that returns Fibonacci numbers.

