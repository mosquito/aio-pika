.. _aio_pika: https://github.com/mosquito/aio-pika


Patterns and helpers
++++++++++++++++++++

.. note:: Available since `aio-pika>=1.7.0`

`aio_pika`_ includes some useful patterns for creating distributed systems.


Master/Worker
~~~~~~~~~~~~~

Helper which implements Master/Worker pattern.
This applicable for balancing tasks between multiple workers.

The master creates tasks:

.. literalinclude:: examples/master.py
   :language: python


Worker code:

.. literalinclude:: examples/worker.py
   :language: python

The one or multiple workers executes tasks.


RPC
~~~

Helper which implements Remote Procedure Call pattern.
This applicable for balancing tasks between multiple workers.

The caller creates tasks and awaiting results:

.. literalinclude:: examples/rpc-caller.py
   :language: python


One or multimple callees executing tasks:

.. literalinclude:: examples/rpc-callee.py
   :language: python
