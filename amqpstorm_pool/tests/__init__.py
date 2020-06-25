import json
import time
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


class PoolTests(unittest.TestCase):

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

        with pool.acquire():
            with pool.acquire():
                pass

        self.assertEqual(2, pool._queue.qsize())

        time.sleep(0.125)

        with pool.acquire():
            pass

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
