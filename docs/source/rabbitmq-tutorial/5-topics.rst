.. _aio-pika: https://github.com/mosquito/aio-pika
.. _topics:

Topics
======

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
also based on the source which emitted the log. You might know this concept from the syslog
unix tool, which routes logs based on both severity (info/warn/crit...) and facility (auth/cron/kern...).

That would give us a lot of flexibility - we may want to listen to just critical errors coming
from 'cron' but also all logs from 'kern'.

To implement that in our logging system we need to learn about a more complex topic exchange.

