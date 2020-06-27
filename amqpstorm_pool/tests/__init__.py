import json
import time

try:
    import unittest2 as unittest
except ImportError:
    import unittest

import amqpstorm
from amqpstorm.tests import utility

import amqpstorm_pool


def fake_write(_, __):
    pass


def create_fake_connection():
    connection = utility.FakeConnection(on_write=fake_write)
    channel = utility.Channel(1, connection, 0.01)
    channel.set_state(amqpstorm.Channel.OPEN)
    connection.channel = lambda: channel
    return connection


class QueuedPoolTests(unittest.TestCase):
    def test_simple_publish(self):
        pool = amqpstorm_pool.QueuedPool(
            create=lambda: create_fake_connection(),
            max_size=10,
            max_overflow=10,
            timeout=10,
            recycle=3600,
            stale=45,
        )

        self.assertEqual(0, pool._queue.qsize())

        with pool.acquire() as cxn:
            cxn.channel.basic.publish(
                body=json.dumps({
                    'type': 'banana',
                    'description': 'they are yellow'
                }),
                exchange='',
                routing_key='fruits',
                properties={
                    'content_type': 'text/plain',
                    'headers': {'key': 'value'}
                }
            )

        self.assertEqual(1, pool._queue.qsize())

    def test_pool_fairy_representation(self):
        pool = amqpstorm_pool.QueuedPool(
            create=lambda: create_fake_connection(),
            max_size=10,
            max_overflow=10,
            timeout=10,
            recycle=3600,
            stale=45,
        )

        with pool.acquire() as cxn:
            self.assertRegexpMatches(
                str(cxn.fairy),
                'cxn=localhost:1234/None, channel=None, '
                'created_at=.*, released_at=.*'
            )

    def test_create_two_connections(self):
        pool = amqpstorm_pool.QueuedPool(
            create=lambda: create_fake_connection(),
            max_size=10,
            max_overflow=10,
            timeout=10,
            recycle=3600,
            stale=45,
        )

        self.assertEqual(0, pool._queue.qsize())

        with pool.acquire():
            with pool.acquire():
                pass

        self.assertEqual(2, pool._queue.qsize())

    def test_create_two_connections_with_overflow(self):
        pool = amqpstorm_pool.QueuedPool(
            create=lambda: create_fake_connection(),
            max_size=1,
            max_overflow=1,
            timeout=10,
            recycle=3600,
            stale=45,
        )

        self.assertEqual(0, pool._queue.qsize())

        with pool.acquire():
            with pool.acquire():
                pass

        self.assertEqual(1, pool._queue.qsize())

    def test_queue_recycle(self):
        pool = amqpstorm_pool.QueuedPool(
            create=lambda: create_fake_connection(),
            max_size=10,
            max_overflow=0,
            timeout=0.1,
            recycle=0.1,
            stale=45,
        )

        self.assertEqual(0, pool._queue.qsize())

        with pool.acquire() as cxn1:
            self.assertIsNotNone(cxn1.channel)
            with pool.acquire() as cxn2:
                self.assertIsNotNone(cxn2.channel)

        self.assertEqual(2, pool._queue.qsize())

        time.sleep(0.125)

        with pool.acquire() as cxn:
            self.assertIsNotNone(cxn)

        self.assertEqual(1, pool._queue.qsize())

    def test_queue_stale(self):
        pool = amqpstorm_pool.QueuedPool(
            create=lambda: create_fake_connection(),
            max_size=10,
            max_overflow=0,
            timeout=0.1,
            recycle=45,
            stale=0.1,
        )

        self.assertEqual(0, pool._queue.qsize())

        with pool.acquire() as cxn1:
            self.assertIsNotNone(cxn1.channel)
            with pool.acquire() as cxn2:
                self.assertIsNotNone(cxn2.channel)

        self.assertEqual(2, pool._queue.qsize())

        time.sleep(0.125)

        with pool.acquire() as cxn:
            self.assertIsNotNone(cxn)

        self.assertEqual(1, pool._queue.qsize())

    def test_queue_raises_timeout(self):
        pool = amqpstorm_pool.QueuedPool(
            create=lambda: create_fake_connection(),
            max_size=1,
            max_overflow=0,
            timeout=0.1,
            recycle=3600,
            stale=45,
        )

        self.assertEqual(0, pool._queue.qsize())

        with pool.acquire():
            self.assertRaises(amqpstorm_pool.Timeout, pool.acquire)

        self.assertEqual(1, pool._queue.qsize())

    def test_queue_with_closed_connection(self):
        pool = amqpstorm_pool.QueuedPool(
            create=lambda: create_fake_connection(),
            max_size=1,
            max_overflow=1,
            timeout=10,
            recycle=3600,
            stale=45,
        )

        self.assertEqual(0, pool._queue.qsize())

        with pool.acquire() as cxn:
            cxn.fairy.close()

        with pool.acquire():
            pass

        self.assertEqual(1, pool._queue.qsize())
