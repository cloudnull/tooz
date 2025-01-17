==========
Developers
==========

Interfaces
----------

.. autoclass:: tooz.coordination.CoordinationDriver
   :members:

File
~~~~

.. autoclass:: tooz.drivers.file.FileDriver
   :members:

IPC
~~~

.. autoclass:: tooz.drivers.ipc.IPCDriver
   :members:

Memcached
~~~~~~~~~

.. autoclass:: tooz.drivers.memcached.MemcachedDriver
   :members:

Mysql
~~~~~

.. autoclass:: tooz.drivers.mysql.MySQLDriver
   :members:

PostgreSQL
~~~~~~~~~~

.. autoclass:: tooz.drivers.pgsql.PostgresDriver
   :members:

Redis
~~~~~

.. autoclass:: tooz.drivers.redis.RedisDriver
   :members:

Zake
~~~~

.. autoclass:: tooz.drivers.zake.ZakeDriver
   :members:

Zookeeper
~~~~~~~~~

.. autoclass:: tooz.drivers.zookeeper.KazooDriver
   :members:

Exceptions
----------

.. autoclass:: tooz.coordination.ToozError
.. autoclass:: tooz.coordination.ToozConnectionError
.. autoclass:: tooz.coordination.OperationTimedOut
.. autoclass:: tooz.coordination.GroupNotCreated
.. autoclass:: tooz.coordination.GroupAlreadyExist
.. autoclass:: tooz.coordination.MemberAlreadyExist
.. autoclass:: tooz.coordination.MemberNotJoined
.. autoclass:: tooz.coordination.GroupNotEmpty
.. autofunction:: tooz.coordination.raise_with_cause
