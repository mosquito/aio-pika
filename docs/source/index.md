# aio-pika

[![Coveralls](https://coveralls.io/repos/github/mosquito/aio-pika/badge.svg?branch=master)](https://coveralls.io/github/mosquito/aio-pika)
[![Github Actions](https://github.com/mosquito/aio-pika/workflows/tests/badge.svg)](https://github.com/mosquito/aio-pika/actions?query=workflow%3Atests)
[![Latest Version](https://img.shields.io/pypi/v/aio-pika.svg)](https://pypi.python.org/pypi/aio-pika/)
[![](https://img.shields.io/pypi/wheel/aio-pika.svg)](https://pypi.python.org/pypi/aio-pika/)
[![](https://img.shields.io/pypi/pyversions/aio-pika.svg)](https://pypi.python.org/pypi/aio-pika/)
[![](https://img.shields.io/pypi/l/aio-pika.svg)](https://pypi.python.org/pypi/aio-pika/)

**aio-pika** is an async Python client for [RabbitMQ](https://www.rabbitmq.com/)
built on top of [aiormq](http://github.com/mosquito/aiormq/).
It provides an object-oriented API, transparent auto-reconnects with full state
recovery, [publisher confirms](https://www.rabbitmq.com/confirms.html),
[transactions](https://www.rabbitmq.com/semantics.html#tx), and complete
type-hints coverage. Python 3.10+.

```shell
pip install aio-pika
```

## Where to start

Pick a starting point depending on your experience level.

::::{grid} 1 2 2 3
:gutter: 3

:::{grid-item-card} RabbitMQ Tutorial
:link: rabbitmq-tutorial/index
:link-type: doc

New to RabbitMQ? Step-by-step walkthrough of queues, exchanges, routing and more.
:::

:::{grid-item-card} Quick Start
:link: quick-start
:link-type: doc

Working examples: consumer, publisher, transactions, connection pooling.
:::

:::{grid-item-card} API Reference
:link: apidoc
:link-type: doc

Full class and function reference for `aio_pika`.
:::

::::

## More

Ready-made helpers, connection tuning, and the broader ecosystem.

::::{grid} 1 2 2 4
:gutter: 2

:::{grid-item-card} Patterns
:link: patterns
:link-type: doc
:class-card: sd-text-center

Master/Worker & RPC
:::

:::{grid-item-card} URL Parameters
:link: url-parameters
:link-type: doc
:class-card: sd-text-center

AMQP connection options
:::

:::{grid-item-card} See Also
:link: see-also
:link-type: doc
:class-card: sd-text-center

Related projects
:::

:::{grid-item-card} GitHub
:link: https://github.com/mosquito/aio-pika
:link-type: url
:class-card: sd-text-center

Source code & issues
:::

::::

This software follows [Semantic Versioning](http://semver.org/).

```{toctree}
:maxdepth: 3
:hidden:

quick-start
rabbitmq-tutorial/index
patterns
url-parameters
apidoc
see-also
```
