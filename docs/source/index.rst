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
* `@hellysmile`_ (bug fixes and ideas)
* `@alternativehood`_ (bugfixes)
* `@akhoronko`_
* `@zyp`_
* `@decaz`_
* `@kajetanj`_
* `@iselind`_
* `@driverx`_

.. _@mosquito: https://github.com/mosquito
.. _@hellysmile: https://github.com/hellysmile
.. _@alternativehood: https://github.com/alternativehood
.. _@akhoronko: https://github.com/akhoronko
.. _@zyp: https://github.com/zyp
.. _@decaz: https://github.com/decaz
.. _@kajetanj: https://github.com/kajetanj
.. _@iselind: https://github.com/iselind
.. _@driverx: https://github.com/DriverX


Versioning
==========

This software follows `Semantic Versioning`_


.. _Semantic Versioning: http://semver.org/
