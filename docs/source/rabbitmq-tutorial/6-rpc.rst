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


Client interface
++++++++++++++++

To illustrate how an RPC service could be used we're going to create a simple client class. It's going to expose
a method named call which sends an RPC request and blocks until the answer is received:

.. code-block:: python

    async def main():
        fibonacci_rpc = FibonacciRpcClient()
        result = await fibonacci_rpc.call(4)
        print("fib(4) is %r" % result)


.. note::
    **A note on RPC**

    Although RPC is a pretty common pattern in computing, it's often criticised. The
    problems arise when a programmer is not aware whether a function call is local or
    if it's a slow RPC. Confusions like that result in an unpredictable system and adds
    unnecessary complexity to debugging. Instead of simplifying software, misused RPC can
    result in unmaintainable spaghetti code.

    Bearing that in mind, consider the following advice:

    * Make sure it's obvious which function call is local and which is remote.
    * Document your system. Make the dependencies between components clear.
    * Handle error cases. How should the client react when the RPC server is down for a long time?

    When in doubt avoid RPC. If you can, you should use an asynchronous pipeline - instead
    of RPC-like blocking, results are asynchronously pushed to a next computation stage.

Callback queue
++++++++++++++

In general doing RPC over RabbitMQ is easy. A client sends a request message and a server
replies with a response message. In order to receive a response the client needs to send
a 'callback' queue address with the request. Let's try it:

.. code-block:: python

    async def main():
        ...

        # Queue for results
        callback_queue = await channel.declare_queue(exclusive=True)

        await channel.default_exchange.publish(
            Message(
                request,
                reply_to=callback_queue.name
            ),
            routing_key='rpc_queue'
        )

        # ... and some code to read a response message from the callback_queue ...

    ...

.. note::

    **Message properties**

    The AMQP protocol predefines a set of 14 properties that go with a message. Most of the
    properties are rarely used, with the exception of the following:

    * `delivery_mode`: Marks a message as persistent (with a value of 2) or transient (any other value). You may
      remember this property from the :ref:`second tutorial <work-queues>`.
    * `content_type`: Used to describe the mime-type of the encoding. For example for the
      often used JSON encoding it is a good practice to set this property to: application/json.
    * `reply_to`: Commonly used to name a callback queue.
    * `correlation_id`: Useful to correlate RPC responses with requests.

    See additional info in :class:`aio_pika.Message`


Correlation id
++++++++++++++

In the method presented above we suggest creating a callback queue for
every RPC request. That's pretty inefficient, but fortunately there
is a better way - let's create a single callback queue per client.

That raises a new issue, having received a response in that queue
it's not clear to which request the response belongs. That's when the
`correlation_id` property is used. We're going to set it to a unique value
for every request. Later, when we receive a message in the callback queue
we'll look at this property, and based on that we'll be able to match a
response with a request. If we see an unknown `correlation_id` value, we
may safely discard the message - it doesn't belong to our requests.

You may ask, why should we ignore unknown messages in the callback queue,
rather than failing with an error? It's due to a possibility of a race
condition on the server side. Although unlikely, it is possible that the
RPC server will die just after sending us the answer, but before sending an
acknowledgment message for the request. If that happens, the restarted
RPC server will process the request again. That's why on the client we
must handle the duplicate responses gracefully, and the RPC should
ideally be idempotent.


Summary
+++++++

.. image:: static/python-six.png
   :align: center

Our RPC will work like this:

* When the Client starts up, it creates an anonymous exclusive callback queue.
* For an RPC request, the Client sends a message with two properties: `reply_to`,
  which is set to the callback queue and `correlation_id`, which is set to a
  unique value for every request.
* The request is sent to an rpc_queue queue.
* The RPC worker (aka: server) is waiting for requests on that queue. When a
  request appears, it does the job and sends a message with the result back to the
  Client, using the queue from the reply_to field.
* The client waits for data on the callback queue. When a message appears, it
  checks the `correlation_id` property. If it matches the value from the
  request it returns the response to the application.


Putting it all together
+++++++++++++++++++++++

The code for *rpc_server.py*:

.. code-block:: python
    :linenos:

    import asyncio
    from functools import partial
    from aio_pika import connect, IncomingMessage, Exchange, Message


    def fib(n):
        if n == 0:
            return 0
        elif n == 1:
            return 1
        else:
            return fib(n-1) + fib(n-2)


    def on_message(exchange: Exchange, message: IncomingMessage):
        with message.process():
            n = int(body)

            print(" [.] fib(%s)" % n)
            response = fib(n)

            exchange.publish(
                Message(
                    body=str(response)
                    correlation_id=message.correlation_id
                ),
                routing_key=message.reply_to
            )


    async def main(loop):
        # Perform connection
        connection = await connect("amqp://guest:guest@localhost/", loop=loop)

        # Creating a channel
        channel = await connection.channel()

        # Declaring queue
        queue = await channel.declare_queue('rpc_queue')

        # Start listening the queue with name 'hello'
        await queue.consume(
            partial(
                on_message,
                channel.default_exchange
            )
        )


    if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.add_callback(main(loop))

        # we enter a never-ending loop that waits for data and runs callbacks whenever necessary.
        print(" [x] Awaiting RPC requests")
        loop.run_forever()


The server code is rather straightforward:

* (33) As usual we start by establishing the connection and declaring the queue.
* (6) We declare our fibonacci function. It assumes only valid positive integer input.
  (Don't expect this one to work for big numbers, it's probably the slowest recursive implementation possible).
* (15) We declare a callback for basic_consume, the core of the RPC server.
  It's executed when the request is received. It does the work and sends the response back.


The code for rpc_client.py:


.. code-block:: python
    :linenos:

    import asyncio
    from functools import partial
    from aio_pika import connect, IncomingMessage, Message


    class FibonacciRpcClient:
        def __init__(self, loop):
            self.connection = None
            self.channel = None
            self.callback_queue = None
            self.futures = {}
            self.loop = loop

        async def connect(self):
            self.connection = await connect("amqp://guest:guest@localhost/", loop=loop)
            self.channel = await connection.channel()
            self.callback_queue = await channel.declare_queue(exclusive=True)

            await queue.consume(self.on_response)

            return self

        def on_response(self, message: IncomingMessage):
            future = self.futures.pop(message.correlation_id)
            future.set_result(message.body)

        async def call(self, n):
            correlation_id = str(uuid.uuid4())
            future = loop.create_future()

            self.futures[correlation_id] = future

            self.channel.default_exchange.publish(
                Message(
                    bytes(n),
                    correlation_id=correlation_id,
                    reply_to=self.callback_queue.name,
                ),
                routing_key='rpc_queue',
            )

            return int(await future)


    async def main(loop):
        fibonacci_rpc = await FibonacciRpcClient(loop).connect()
        print(" [x] Requesting fib(30)")
        response = await fibonacci_rpc.call(30)
        print(" [.] Got %r" % response)


    if __name__ == "__main__":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())

The client code is slightly more involved:

* (15) We establish a connection, channel and declare an exclusive 'callback' queue for replies.
* (19) We subscribe to the 'callback' queue, so that we can receive RPC responses.
* (23) The 'on_response' callback executed on every response is doing a very simple job,
 for every response message it checks if the correlation_id is the one we're looking for.
 If so, it saves the response in self.response and breaks the consuming loop.
* (27) Next, we define our main call method - it does the actual RPC request.
* (28) In this method, first we generate a unique correlation_id number and save it - the 'on_response' callback
 function will use this value to catch the appropriate response.
* (33) Next, we publish the request message, with two properties: reply_to and correlation_id.
* (42) And finally we return the response back to the user.

Our RPC service is now ready. We can start the server::

    $ python rpc_server.py
    [x] Awaiting RPC requests

To request a fibonacci number run the client::

    $ python rpc_client.py
    [x] Requesting fib(30)

The presented design is not the only possible implementation of
a RPC service, but it has some important advantages:

If the RPC server is too slow, you can scale up by just running another one.
Try running a second rpc_server.py in a new console.
On the client side, the RPC requires sending and receiving only one message.
No synchronous calls like queue_declare are required. As a result the RPC client
needs only one network round trip for a single RPC request.
Our code is still pretty simplistic and doesn't try to solve more
complex (but important) problems, like:

* How should the client react if there are no servers running?
* Should a client have some kind of timeout for the RPC?
* If the server malfunctions and raises an exception, should it be forwarded to the client?
* Protecting against invalid incoming messages (eg checking bounds) before processing.

.. note::

    If you want to experiment, you may find the rabbitmq-management plugin useful for viewing the queues.
