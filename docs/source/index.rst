.. aio-pika documentation master file, created by
   sphinx-quickstart on Fri Mar 31 17:03:20 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. _aio-pika: https://github.com/mosquito/aio-pika
.. _asyncio: https://docs.python.org/3/library/asyncio.html
.. _aiormq: http://github.com/mosquito/aiormq/


Welcome to aio-pika's documentation!
====================================

.. image:: https://coveralls.io/repos/github/mosquito/aio-pika/badge.svg?branch=master
    :target: https://coveralls.io/github/mosquito/aio-pika
    :alt: Coveralls

.. image:: https://github.com/mosquito/aio-pika/workflows/tests/badge.svg
    :target: https://github.com/mosquito/aio-pika/actions?query=workflow%3Atests
    :alt: Github Actions

.. image:: https://img.shields.io/pypi/v/aio-pika.svg
    :target: https://pypi.python.org/pypi/aio-pika/
    :alt: Latest Version

.. image:: https://img.shields.io/pypi/wheel/aio-pika.svg
    :target: https://pypi.python.org/pypi/aio-pika/

.. image:: https://img.shields.io/pypi/pyversions/aio-pika.svg
    :target: https://pypi.python.org/pypi/aio-pika/

.. image:: https://img.shields.io/pypi/l/aio-pika.svg
    :target: https://pypi.python.org/pypi/aio-pika/


`aio-pika`_ is a wrapper for the `aiormq`_ for `asyncio`_ and humans.


Features
++++++++

* Completely asynchronous API.
* Object oriented API.
* Transparent auto-reconnects with complete state recovery with `connect_robust`
  (e.g. declared queues or exchanges, consuming state and bindings).
* Python 3.6+ compatible.
* For python 3.5 users available `aio-pika<7`
* Transparent `publisher confirms`_ support
* `Transactions`_ support
* Completely type-hints coverage.

.. _publisher confirms: https://www.rabbitmq.com/confirms.html
.. _Transactions: https://www.rabbitmq.com/semantics.html#tx

Installation
++++++++++++

Installation with pip:

.. code-block:: shell

    pip install aio-pika


Installation from git:

.. code-block:: shell

    # via pip
    pip install https://github.com/mosquito/aio-pika/archive/master.zip

    # manually
    git clone https://github.com/mosquito/aio-pika.git
    cd aio-pika
    python setup.py install


Development
+++++++++++

Clone the project:

.. code-block:: shell

    git clone https://github.com/mosquito/aio-pika.git
    cd aio-pika


Create a new virtualenv for `aio-pika`_:

.. code-block:: shell

    virtualenv -p python3.5 env

Install all requirements for `aio-pika`_:

.. code-block:: shell

    env/bin/pip install -e '.[develop]'

Table Of Contents
+++++++++++++++++

.. toctree::
   :glob:
   :maxdepth: 3

   quick-start
   patterns
   rabbitmq-tutorial/index
   apidoc


Thanks for contributing
+++++++++++++++++++++++

* `@mosquito`_ (author)
* `@decaz`_ (steel persuasiveness while code review)
* `@heckad`_ (bug fixes)
* `@smagafurov`_ (bug fixes)
* `@hellysmile`_ (bug fixes and ideas)
* `@altvod`_ (bug fixes)
* `@alternativehood`_ (bugfixes)
* `@cprieto`_ (bug fixes)
* `@akhoronko`_ (bug fixes)
* `@iselind`_ (bug fixes)
* `@DXist`_ (bug fixes)
* `@blazewicz`_ (bug fixes)
* `@chibby0ne`_ (bug fixes)
* `@jmccarrell`_ (bug fixes)
* `@taybin`_ (bug fixes)
* `@ollamh`_ (bug fixes)
* `@DriverX`_ (bug fixes)
* `@brianmedigate`_ (bug fixes)
* `@dan-stone`_ (bug fixes)
* `@Kludex`_ (bug fixes)
* `@bmario`_ (bug fixes)
* `@tzoiker`_ (bug fixes)
* `@Pehat`_ (bug fixes)
* `@WindowGenerator`_ (bug fixes)
* `@dhontecillas`_ (bug fixes)
* `@tilsche`_ (bug fixes)
* `@leenr`_ (bug fixes)
* `@la0rg`_ (bug fixes)
* `@SolovyovAlexander`_ (bug fixes)
* `@kremius`_ (bug fixes)
* `@zyp`_ (bug fixes)
* `@kajetanj`_ (bug fixes)
* `@Alviner`_ (moral support, debug sessions and good mood)
* `@Pavkazzz`_ (composure, and patience while debug sessions)
* `@bbrodriges`_ (supplying grammar while writing documentation)
* `@dizballanze`_ (review, grammar)

.. _@mosquito: https://github.com/mosquito
.. _@decaz: https://github.com/decaz
.. _@heckad: https://github.com/heckad
.. _@smagafurov: https://github.com/smagafurov
.. _@hellysmile: https://github.com/hellysmile
.. _@altvod: https://github.com/altvod
.. _@alternativehood: https://github.com/alternativehood
.. _@cprieto: https://github.com/cprieto
.. _@akhoronko: https://github.com/akhoronko
.. _@iselind: https://github.com/iselind
.. _@DXist: https://github.com/DXist
.. _@blazewicz: https://github.com/blazewicz
.. _@chibby0ne: https://github.com/chibby0ne
.. _@jmccarrell: https://github.com/jmccarrell
.. _@taybin: https://github.com/taybin
.. _@ollamh: https://github.com/ollamh
.. _@DriverX: https://github.com/DriverX
.. _@brianmedigate: https://github.com/brianmedigate
.. _@dan-stone: https://github.com/dan-stone
.. _@Kludex: https://github.com/Kludex
.. _@bmario: https://github.com/bmario
.. _@tzoiker: https://github.com/tzoiker
.. _@Pehat: https://github.com/Pehat
.. _@WindowGenerator: https://github.com/WindowGenerator
.. _@dhontecillas: https://github.com/dhontecillas
.. _@tilsche: https://github.com/tilsche
.. _@leenr: https://github.com/leenr
.. _@la0rg: https://github.com/la0rg
.. _@SolovyovAlexander: https://github.com/SolovyovAlexander
.. _@kremius: https://github.com/kremius
.. _@zyp: https://github.com/zyp
.. _@kajetanj: https://github.com/kajetanj
.. _@Alviner: https://github.com/Alviner
.. _@Pavkazzz: https://github.com/Pavkazzz
.. _@bbrodriges: https://github.com/bbrodriges
.. _@dizballanze: https://github.com/dizballanze


See also
==========

`Patio`_ and `Patio-RMQ`_
-------------------------

**PATIO** is an acronym for Python Asynchronous Tasks for AsyncIO - an easily extensible library, for distributed task execution, like celery, only targeting asyncio as the main design approach.

**Patio-RMQ** provides you with the ability to use *RPC over RabbitMQ* services with extremely simple implementation:

.. code-block:: python

   from patio import Registry, ThreadPoolExecutor
   from patio_rabbitmq import RabbitMQBroker

   rpc = Registry(project="patio-rabbitmq", auto_naming=False)
   
   @rpc("sum")
   def sum(*args):
       return sum(args)

   async def main():
       async with ThreadPoolExecutor(rpc, max_workers=16) as executor:
           async with RabbitMQBroker(
               executor, amqp_url="amqp://guest:guest@localhost/",
           ) as broker:
               await broker.join()


`Propan`_:fire:
---------------

**Propan** is a powerful and easy-to-use Python framework for building event-driven applications that interact with any MQ Broker.

If you need no deep dive into **RabbitMQ** details, you can use more high-level **Propan** interfaces:

.. code-block:: python

   from propane import Propaneapp, RabbitBroker
   
   broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
   app = Propane app(broker)
   
   @broker.handle("user")
   async def user_created(user_id: int):
       assert isinstance(user_id, int)

Also, **Propan** validates messages by **pydantic**, generates your project **AsyncAPI** spec, tests application locally, RPC calls, and more.

In fact, it is a high-level wrapper on top of **aio-pika**, so you can use both of these libraries' advantages at the same time.

Versioning
==========

This software follows `Semantic Versioning`_


.. _Semantic Versioning: http://semver.org/
.. _propan: https://github.com/Lancetnik/Propan
