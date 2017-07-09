import asyncio
import os
import uuid
import logging
from typing import Tuple, Generator, Any

import pytest
import shortuuid
import time
import unittest
import aio_pika
import aio_pika.exceptions
from copy import copy
from aio_pika import connect, connect_url, Message, DeliveryMode
from aio_pika.exceptions import ProbableAuthenticationError, MessageProcessError
from aio_pika.exchange import ExchangeType
from aio_pika.tools import wait, create_future
from unittest import mock
from . import AsyncTestCase, AMQP_URL, tcp_proxy


log = logging.getLogger(__name__)


class TestCase(AsyncTestCase):
    @asyncio.coroutine
    def create_proxy_client(self) -> Generator[Any, None, Tuple[tcp_proxy.TCPProxy, aio_pika.RobustConnection]]:
        proxy = yield from tcp_proxy.create_proxy(
            remote_host=AMQP_URL.host,
            remote_port=AMQP_URL.port or 5672,
            loop=self.loop,
        )

        amqp_url = AMQP_URL.with_host(
            proxy.listen_host if proxy.listen_host != '0.0.0.0' else '127.0.0.1'
        ).with_port(
            proxy.listen_port
        )

        client = yield from aio_pika.robust_connect(amqp_url, loop=self.loop)

        def cleanup():
            nonlocal client, proxy
            yield from client.close()
            yield from client.closing
            yield from proxy.close()

        self.addCleanup(cleanup)

        return proxy, client

    def test_simple_reconnect(self):
        proxy, client = yield from self.create_proxy_client()

        channel = yield from client.channel()
        queue = yield from channel.declare_queue()
        exchange = channel.default_exchange

        body = self.get_random_name('queue').encode()

        self.messages = list()

        def on_message(message: aio_pika.IncomingMessage):
            self.messages.append(message)

        queue.consume(on_message, no_ack=True)

        yield from exchange.publish(
            Message(body=body),
            routing_key=queue.name
        )

        proxy.disconnect_clients()

        with self.assertRaises(ConnectionError):
            yield from exchange.publish(
                Message(body=body),
                routing_key=queue.name
            )

        yield from exchange.publish(
            Message(body=body),
            routing_key=queue.name
        )

        self.assertEqual(len(self.messages), 2)

