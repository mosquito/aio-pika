.. _aio-pika: https://github.com/mosquito/aio-pika


Patterns and helpers
++++++++++++++++++++

.. note:: Available since `aio-pika>=1.7.0`

`aio-pika`_ includes some useful patterns for creating distributed systems.


.. _patterns-worker:

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


.. _patterns-rpc:

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

Extending
~~~~~~~~~

Both patterns serialization behaviour might be changed by inheritance and
redefinition of methods :func:`aio_pika.patterns.base.serialize`
and :func:`aio_pika.patterns.base.deserialize`.


Following examples demonstrates it:

.. literalinclude:: examples/extend-patterns.py
   :language: python
