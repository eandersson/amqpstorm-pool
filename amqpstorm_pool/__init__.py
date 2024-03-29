__version__ = '1.0.1'
__all__ = [
    'Error',
    'Timeout',
    'Overflow',
    'Connection',
    'Pool',
    'NullPool',
    'QueuedPool',
]

from datetime import datetime
import logging

try:
    # python 3
    import queue
except ImportError:
    # python 2
    import Queue as queue

import threading
import time

import amqpstorm


LOGGER = logging.getLogger(__name__)


class Error(Exception):
    pass


class Overflow(Error):
    """
    Raised when a `Pool.acquire` cannot allocate anymore connections.
    """
    pass


class Timeout(Error):
    """
    Raised when an attempt to `Pool.acquire` a connection has timedout.
    """
    pass


class Connection(object):
    """
    Connection acquired from a `Pool` instance. Get them like this:

    .. code:: python

        with pool.acquire() as cxn:
            print cxn.channel

    """

    #: Exceptions that imply connection has been invalidated.
    connectivity_errors = (
        amqpstorm.AMQPConnectionError,
        amqpstorm.AMQPChannelError,
    )

    @classmethod
    def is_connection_invalidated(cls, exc):
        """
        Says whether the given exception indicates the connection has
        been invalidated.

        :param exc: Exception object.

        :return: True if connection has been invalidated, otherwise False.
        """
        return any(
            isinstance(exc, error) for error in cls.connectivity_errors
        )

    def __init__(self, pool, fairy):
        self.pool = pool
        self.fairy = fairy

    @property
    def channel(self):
        if self.fairy.channel is None:
            self.fairy.channel = self.fairy.cxn.channel()
        return self.fairy.channel

    def close(self):
        self.pool.close(self.fairy)
        self.fairy = None

    def release(self):
        self.pool.release(self.fairy)
        self.fairy = None

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if type is None or not self.is_connection_invalidated(value):
            self.release()
        else:
            self.close()


class Pool(object):
    """
    Pool interface similar to:

        http://docs.sqlalchemy.org/en/latest/core/pooling.html#sqlalchemy.pool.Pool

    and used like:

    .. code:: python

        with pool.acquire(timeout=60) as cxn:
            cxn.channel.basic.publish(
            ...
            )

    """

    #: Acquired connection type.
    Connection = Connection

    def __init__(self, create):
        """
        :param create: Callable creating a new connection.
        """
        self.create = create

    def acquire(self, timeout=None):
        """
        Retrieve a connection from the pool or create a new one.
        """
        raise NotImplementedError

    def release(self, fairy):
        """
        Return a connection to the pool.
        """
        raise NotImplementedError

    def close(self, fairy):
        """
        Forcibly close a connection, suppressing any connection errors.
        """
        fairy.close()

    class Fairy(object):
        """
        Connection wrapper for tracking its associated state.
        """

        def __init__(self, cxn):
            self.cxn = cxn
            self.channel = None

        def close(self):
            if self.channel:
                try:
                    self.channel.close()
                except Connection.connectivity_errors as ex:
                    if not Connection.is_connection_invalidated(ex):
                        raise
                self.channel = None
            try:
                self.cxn.close()
            except Connection.connectivity_errors as ex:
                if not Connection.is_connection_invalidated(ex):
                    raise

        @property
        def cxn_str(self):
            if self.cxn:
                return '{0}:{1}/{2}'.format(
                    self.cxn.parameters.get('hostname'),
                    self.cxn.parameters.get('port'),
                    self.cxn.parameters.get('virtual_host')
                )

        def __str__(self):
            channel = int(self.channel) if self.channel else self.channel
            return ', '.join('{0}={1}'.format(k, v) for k, v in [
                ('cxn', self.cxn_str),
                ('channel', '{0}'.format(channel)),
            ])

    def _create(self):
        """
        All fairy creates go through here.
        """
        return self.Fairy(self.create())


class NullPool(Pool):
    """
    Dummy pool. It opens/closes connections on each acquire/release.
    """

    def acquire(self, timeout=None):
        return self.Connection(self, self._create())

    def release(self, fairy):
        self.close(fairy)


class QueuedPool(Pool):
    """
    Queue backed pool.
    """

    def __init__(self, create, max_size=10, max_overflow=10, timeout=30,
                 recycle=None, stale=None):
        """
        :param max_size:
            Maximum number of connections to keep queued.

        :param max_overflow:
            Maximum number of connections to create above `max_size`.

        :param timeout:
            Default number of seconds to wait for a connections to available.

        :param recycle:
            Lifetime of a connection (since creation) in seconds or None for no
            recycling. Expired connections are closed on acquire.

        :param stale:
            Threshold at which inactive (since release) connections are
            considered stale in seconds or None for no staleness. Stale
            connections are closed on acquire.
        """
        self.max_size = max_size
        self.max_overflow = max_overflow
        self.timeout = timeout
        self.recycle = recycle
        self.stale = stale
        self._queue = queue.Queue(maxsize=self.max_size)
        self._avail_lock = threading.Lock()
        self._avail = self.max_size + self.max_overflow
        super(QueuedPool, self).__init__(create)

    def acquire(self, timeout=None):
        try:
            fairy = self._queue.get(False)
        except queue.Empty:
            try:
                fairy = self._create()
            except Overflow:
                timeout = timeout or self.timeout
                try:
                    fairy = self._queue.get(timeout=timeout)
                except queue.Empty:
                    try:
                        fairy = self._create()
                    except Overflow:
                        raise Timeout()

        if self.is_expired(fairy):
            LOGGER.info('closing expired connection - %s', fairy)
            self.close(fairy)
            return self.acquire(timeout=timeout)
        if self.is_stale(fairy):
            LOGGER.info('closing stale connection - %s', fairy)
            self.close(fairy)
            return self.acquire(timeout=timeout)

        try:
            if fairy.channel:
                fairy.channel.check_for_errors()
            elif fairy.cxn:
                fairy.cxn.check_for_errors()
        except amqpstorm.AMQPError:
            LOGGER.info('closing broken connection - %s', fairy)
            self.close(fairy)
            return self.acquire(timeout=timeout)

        return self.Connection(self, fairy)

    def release(self, fairy):
        fairy.released_at = time.time()
        try:
            self._queue.put_nowait(fairy)
        except queue.Full:
            self.close(fairy)

    def close(self, fairy):
        # inc
        with self._avail_lock:
            self._avail += 1
        return super(QueuedPool, self).close(fairy)

    def _create(self):
        # dec
        with self._avail_lock:
            if self._avail <= 0:
                raise Overflow()
            self._avail -= 1
        try:
            return super(QueuedPool, self)._create()
        except Exception:
            # inc
            with self._avail_lock:
                self._avail += 1
            raise

    class Fairy(Pool.Fairy):

        def __init__(self, cxn):
            super(QueuedPool.Fairy, self).__init__(cxn)
            self.released_at = self.created_at = time.time()

        def __str__(self):
            channel = int(self.channel) if self.channel else self.channel
            created_at = datetime.fromtimestamp(self.created_at).isoformat()
            released_at = datetime.fromtimestamp(self.released_at).isoformat()

            return ', '.join('{0}={1}'.format(k, v) for k, v in [
                ('cxn', self.cxn_str),
                ('channel', '{0}'.format(channel)),
                ('created_at', '{0}'.format(created_at)),
                ('released_at', '{0}'.format(released_at)),
            ])

    def is_stale(self, fairy):
        if not self.stale:
            return False
        return (time.time() - fairy.released_at) > self.stale

    def is_expired(self, fairy):
        if not self.recycle:
            return False
        return (time.time() - fairy.created_at) > self.recycle
