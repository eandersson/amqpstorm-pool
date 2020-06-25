AMQPStorm-Pool
==============
`AMQPStorm <https://github.com/eandersson/amqpstorm>`_ connection pooling based on `pika-pool <https://github.com/bninja/pika-pool>`_.

usage
-----

Get it:

.. code:: bash

    pip install amqpstorm-pool

and use it:

.. code:: python

    import json

    import amqpstorm
    import amqpstorm_pool

    uri = 'amqp://guest:guest@localhost:5672/%2F?heartbeat=60'
    pool = amqpstorm_pool.QueuedPool(
        create=lambda: amqpstorm.UriConnection(uri),
        max_size=10,
        max_overflow=10,
        timeout=10,
        recycle=3600,
        stale=45,
    )

    with pool.acquire() as cxn:
        cxn.channel.queue.declare('fruits')
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
