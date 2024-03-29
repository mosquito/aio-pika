.. _issue: https://github.com/mosquito/aio-pika/issues
.. _pull request: https://github.com/mosquito/aio-pika/compare
.. _aio-pika: https://github.com/mosquito/aio-pika
.. _syslog: http://en.wikipedia.org/wiki/Syslog
.. _official tutorial: https://www.rabbitmq.com/tutorials/tutorial-five-python.html
.. _topics:

Topics
======

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


In the :ref:`previous tutorial <routing>` we improved our logging system. Instead of using a fanout
exchange only capable of dummy broadcasting, we used a direct one, and gained a
possibility of selectively receiving the logs.

Although using the direct exchange improved our system, it still has limitations — it can't do routing based on
multiple criteria.

In our logging system we might want to subscribe to not only logs based on severity, but
also based on the source which emitted the log. You might know this concept from the syslog_
unix tool, which routes logs based on both severity (`info`/`warn`/`crit`...)
and facility (`auth`/`cron`/`kern`...).

That would give us a lot of flexibility - we may want to listen to just critical errors coming
from 'cron' but also all logs from 'kern'.

To implement that in our logging system we need to learn about a more complex topic exchange.

Topic exchange
++++++++++++++

Messages sent to a topic exchange can't have an arbitrary *routing_key* - it must be a list of words,
delimited by dots. The words can be anything, but usually they specify some features connected to
the message. A few valid routing key examples: `"stock.usd.nyse"`, `"nyse.vmw"`, `"quick.orange.rabbit"`.
There can be as many words in the routing key as you like, up to the limit of 255 bytes.

The binding key must also be in the same form. The logic behind the topic exchange is similar
to a direct one - a message sent with a particular routing key will be delivered to all the
queues that are bound with a matching binding key. However there are two important special
cases for binding keys:

* `*` (star) can substitute for exactly one word.
* `#` (hash) can substitute for zero or more words.

It's easiest to explain this in an example:

.. image:: /_static/tutorial/python-five.svg
   :align: center

In this example, we're going to send messages which all describe animals. The messages will be sent
with a routing key that consists of three words (two dots). The first word in the routing key will
describe a celerity, second a colour and third a species: `"<celerity>.<colour>.<species>"`.

We created three bindings: *Q1* is bound with binding key `"*.orange.*"` and Q2 with `"*.*.rabbit"` and `"lazy.#"`.

These bindings can be summarised as:

* Q1 is interested in all the orange animals.
* Q2 wants to hear everything about rabbits, and everything about lazy animals.
* A message with a routing key set to `"quick.orange.rabbit"` will be delivered to both queues.
  Message `"lazy.orange.elephant"` also will go to both of them. On the other hand `"quick.orange.fox"` will only go to
  the first queue, and `"lazy.brown.fox"` only to the second. `"lazy.pink.rabbit"` will be delivered to the second
  queue only once, even though it matches two bindings. "quick.brown.fox" doesn't match any binding so it will be
  discarded.

What happens if we break our contract and send a message with one or four words,
like `"orange"` or `"quick.orange.male.rabbit"`? Well, these messages won't match any bindings and will be lost.

On the other hand `"lazy.orange.male.rabbit"`, even though it has four words, will match the last binding and will be
delivered to the second queue.

.. note::

    **Topic exchange**

    Topic exchange is powerful and can behave like other exchanges.

    When a queue is bound with `"#"` (hash) binding key - it will receive all the messages, regardless of the routing
    key - like in fanout exchange.

    When special characters `"*"` (star) and `"#"` (hash) aren't used in bindings, the topic exchange will behave just
    like a direct one.


Putting it all together
+++++++++++++++++++++++

We're going to use a topic exchange in our logging system. We'll start off with a working assumption
that the routing keys of logs will have two words: `"<facility>.<severity>"`.

The code is almost the same as in the :ref:`previous tutorial <routing>`.

The code for :download:`emit_log_topic.py <examples/5-topics/emit_log_topic.py>`:

.. literalinclude:: examples/5-topics/emit_log_topic.py
   :language: python

The code for :download:`receive_logs_topic.py <examples/5-topics/receive_logs_topic.py>`:

.. literalinclude:: examples/5-topics/receive_logs_topic.py
   :language: python

To receive all the logs run::

    python receive_logs_topic.py "#"

To receive all logs from the facility `"kern"`::

    python receive_logs_topic.py "kern.*"

Or if you want to hear only about `"critical"` logs::

    python receive_logs_topic.py "*.critical"

You can create multiple bindings::

    python receive_logs_topic.py "kern.*" "*.critical"

And to emit a log with a routing key `"kern.critical"` type::

    python emit_log_topic.py "kern.critical" "A critical kernel error"

Have fun playing with these programs. Note that the code doesn't make any assumption
about the routing or binding keys, you may want to play with more than two routing key parameters.

Move on to :ref:`tutorial 6 <rpc>` to learn about RPC.


.. note::

    This material was adopted from `official tutorial`_ on **rabbitmq.org**.
