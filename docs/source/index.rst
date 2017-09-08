.. aio-pika documentation master file, created by
   sphinx-quickstart on Fri Mar 31 17:03:20 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. _aio_pika: https://github.com/mosquito/aio-pika
.. _asyncio: https://docs.python.org/3/library/asyncio.html
.. _PIKA: https://github.com/pika/pika


Welcome to aio-pika's documentation!
====================================

.. image:: https://coveralls.io/repos/github/mosquito/aio-pika/badge.svg?branch=master
    :target: https://coveralls.io/github/mosquito/aio-pika
    :alt: Coveralls

.. image:: https://travis-ci.org/mosquito/aio-pika.svg
    :target: https://travis-ci.org/mosquito/aio-pika
    :alt: Travis CI

.. image:: https://img.shields.io/pypi/v/aio-pika.svg
    :target: https://pypi.python.org/pypi/aio-pika/
    :alt: Latest Version

.. image:: https://img.shields.io/pypi/wheel/aio-pika.svg
    :target: https://pypi.python.org/pypi/aio-pika/

.. image:: https://img.shields.io/pypi/pyversions/aio-pika.svg
    :target: https://pypi.python.org/pypi/aio-pika/

.. image:: https://img.shields.io/pypi/l/aio-pika.svg
    :target: https://pypi.python.org/pypi/aio-pika/


`aio_pika`_ it's a wrapper for the `PIKA`_ for `asyncio`_ and humans.


Features
++++++++

* Completely asynchronous API.
* Object oriented API.
* Auto-reconnects with complete state recovery with `connect_robust`
  (e.g. declared queues or exchanges, consuming state and bindings).
* Python 3.4+ compatible (include 3.6).


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


Usage example
+++++++++++++

.. literalinclude:: examples/main.py
   :language: python

Development
+++++++++++

Clone the project:

.. code-block:: shell

    git clone https://github.com/mosquito/aio-pika.git
    cd aio-pika


Create a new virtualenv for `aio_pika`_:

.. code-block:: shell

    virtualenv -p python3.5 env

Install all requirements for `aio_pika`_:

.. code-block:: shell

    env/bin/pip install -e '.[develop]'


Tutorial
++++++++

.. toctree::
   :maxdepth: 3
   :caption: RabbitMQ tutorial adopted for aio-pika
   :glob:

   rabbitmq-tutorial/*
   apidoc


Thanks for contributing
+++++++++++++++++++++++

* `@mosquito`_ (author)
* `@hellysmile`_ (bug fixes and ideas)
* `@alternativehood`_ (bugfixes)
* `@akhoronko`_
* `@zyp`_
* `@decaz`_ 
* `@kajetanj`_ 
* `@iselind`_

.. _@mosquito: https://github.com/mosquito
.. _@hellysmile: https://github.com/hellysmile
.. _@alternativehood: https://github.com/alternativehood
.. _@akhoronko: https://github.com/akhoronko
.. _@zyp: https://github.com/zyp
.. _@decaz: https://github.com/decaz
.. _@kajetanj: https://github.com/kajetanj
.. _@iselind: https://github.com/iselind
