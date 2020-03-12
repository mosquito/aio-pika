import asyncio
import logging
import pytest
import shortuuid
from contextlib import suppress
from socket import socket

import aiormq.exceptions
from aio_pika.connection import Connection, connect
from aio_pika.exchange import Exchange
from aio_pika.message import Message
from aio_pika.queue import Queue
from aio_pika.robust_channel import RobustChannel
from aio_pika.robust_connection import RobustConnection, connect_robust
from aio_pika.robust_queue import RobustQueue
from aiormq import ChannelLockedResource
from tests import AMQP_URL
from tests.test_amqp import TestCase as AMQPTestCase


class Proxy:
    CHUNK_SIZE = 1500

    def __init__(self, *, loop, shost='127.0.0.1', sport,
                 dhost='127.0.0.1', dport):

        self.loop = loop

        self.src_host = shost
        self.src_port = sport
        self.dst_host = dhost
        self.dst_port = dport
        self.connections = set()

    async def _pipe(self, reader: asyncio.StreamReader,
                    writer: asyncio.StreamWriter):
        try:
            while not reader.at_eof():
                writer.write(await reader.read(self.CHUNK_SIZE))
        finally:
            writer.close()

    async def handle_client(self, creader: asyncio.StreamReader,
                            cwriter: asyncio.StreamWriter):
        sreader, swriter = await asyncio.open_connection(
            host=self.dst_host,
            port=self.dst_port,
        )

        self.connections.add(swriter)
        self.connections.add(cwriter)

        await asyncio.gather(
            self._pipe(sreader, cwriter),
            self._pipe(creader, swriter),
        )

    async def start(self):
        return await asyncio.start_server(
            self.handle_client,
            host=self.src_host,
            port=self.src_port,
        )

    async def disconnect(self, wait=True):
        tasks = list()

        async def close(writer):
            writer.close()
            await asyncio.gather(writer.drain(), return_exceptions=True)

        while self.connections:
            writer = self.connections.pop()     # type: asyncio.StreamWriter
            tasks.append(self.loop.create_task(close(writer)))

        if wait and tasks:
            await asyncio.wait(tasks)


class TestCase(AMQPTestCase):
    @staticmethod
    def get_unused_port() -> int:
        sock = socket()
        sock.bind(('', 0))
        port = sock.getsockname()[-1]
        sock.close()
        return port

    async def create_direct_connection(self, cleanup=True) -> Connection:
        client = await connect(
            str(AMQP_URL), loop=self.loop,
            client_properties={'connection_name': 'direct connection'},
        )

        if cleanup:
            self.addCleanup(client.close)

        return client

    async def create_connection(self, cleanup=True) -> RobustConnection:
        self.proxy = Proxy(
            dhost=AMQP_URL.host,
            dport=AMQP_URL.port,
            sport=self.get_unused_port(),
            loop=self.loop,
        )

        await self.proxy.start()

        url = AMQP_URL.with_host(
            self.proxy.src_host
        ).with_port(
            self.proxy.src_port
        ).update_query(reconnect_interval=1)

        client = await connect_robust(
            str(url), loop=self.loop,
            client_properties={'connection_name': 'proxy connection'},
        )

        if cleanup:
            self.addCleanup(client.close)
            self.addCleanup(self.proxy.disconnect)

        return client

    async def create_channel(self, connection=None,
                             cleanup=True, **kwargs) -> RobustChannel:
        # noinspection PyTypeChecker
        return await super().create_channel(
            connection=connection, cleanup=cleanup, **kwargs
        )

    async def test_set_qos(self):
        channel = await self.create_channel()
        await channel.set_qos(prefetch_count=1)

    async def test_revive_passive_queue_on_reconnect(self):
        client1 = await self.create_connection()
        self.assertIsInstance(client1, RobustConnection)

        client2 = await self.create_connection()
        self.assertIsInstance(client2, RobustConnection)

        reconnect_event = asyncio.Event()
        reconnect_count = 0

        def reconnect_callback(sender, conn):
            nonlocal reconnect_count
            reconnect_count += 1
            reconnect_event.set()
            reconnect_event.clear()

        client2.add_reconnect_callback(reconnect_callback)

        queue_name = self.get_random_name()
        channel1 = await client1.channel()
        self.assertIsInstance(channel1, RobustChannel)

        channel2 = await client2.channel()
        self.assertIsInstance(channel2, RobustChannel)

        queue1 = await self.declare_queue(
            queue_name,
            auto_delete=False,
            passive=False,
            channel=channel1
        )
        self.assertIsInstance(queue1, RobustQueue)

        queue2 = await self.declare_queue(
            queue_name,
            passive=True,
            channel=channel2
        )
        self.assertIsInstance(queue2, RobustQueue)

        await client2.connection.close(aiormq.AMQPError(320, 'Closed'))

        await reconnect_event.wait()

        self.assertEqual(reconnect_count, 1)

        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(
                reconnect_event.wait(),
                client2.reconnect_interval * 2
            )

        self.assertEqual(reconnect_count, 1)

    async def test_robust_reconnect(self):
        channel1 = await self.create_channel()
        channel2 = await self.create_channel()

        shared = []
        queue = await channel1.declare_queue()

        async def reader():
            nonlocal shared
            async with queue.iterator() as q:
                async for message in q:
                    shared.append(message)
                    await message.ack()

        reader_task = self.loop.create_task(reader())
        self.addCleanup(reader_task.cancel)

        for _ in range(5):
            await channel2.default_exchange.publish(
                Message(b''), queue.name,
            )

        logging.info("Disconnect all clients")
        await self.proxy.disconnect()

        logging.info("Waiting for reconnect")
        await asyncio.sleep(5)

        logging.info("Waiting connections")
        await asyncio.wait([
            channel1._connection.ready(),
            channel2._connection.ready()
        ])

        for _ in range(5):
            await channel2.default_exchange.publish(
                Message(b''), queue.name,
            )

        while len(shared) < 10:
            await asyncio.sleep(0.1)

        assert len(shared) == 10

    async def test_channel_locked_resource2(self):
        ch1 = await self.create_channel()
        ch2 = await self.create_channel()

        qname = self.get_random_name("channel", "locked", "resource")

        q1 = await ch1.declare_queue(qname, exclusive=True, robust=False)
        await q1.consume(print, exclusive=True)

        with self.assertRaises(ChannelLockedResource):
            q2 = await ch2.declare_queue(qname, exclusive=True, robust=False)
            await q2.consume(print, exclusive=True)

    async def test_channel_close_when_exclusive_queue(self):
        direct_conn, proxy_conn = await asyncio.gather(
            self.create_direct_connection(),
            self.create_connection()
        )

        direct_channel, proxy_channel = await asyncio.gather(
            direct_conn.channel(),
            proxy_conn.channel()
        )

        qname = self.get_random_name("robust", "exclusive", "queue")

        proxy_queue = await proxy_channel.declare_queue(
            qname, exclusive=True, durable=True
        )

        logging.info("Disconnecting all proxy connections")
        await self.proxy.disconnect(wait=True)
        await asyncio.sleep(0.5)

        logging.info("Declaring exclusive queue through direct channel")
        await direct_channel.declare_queue(
            qname, exclusive=True, durable=True
        )

        async def close_after(delay, closer):
            await asyncio.sleep(delay)
            await closer()
            logging.info("Closed")

        await self.loop.create_task(close_after(5, direct_conn.close))
        await proxy_conn.connected.wait()
        await proxy_queue.delete()

    async def test_context_process_abrupt_channel_close(self):
        # https://github.com/mosquito/aio-pika/issues/302
        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = await self.create_channel()
        exchange = await self.declare_exchange(
            'direct', auto_delete=True, channel=channel
        )
        queue = await self.declare_queue(
            queue_name, auto_delete=True, channel=channel
        )

        await queue.bind(exchange, routing_key)
        body = bytes(shortuuid.uuid(), 'utf-8')

        await exchange.publish(
            Message(body, content_type="text/plain", headers={"foo": "bar"}),
            routing_key,
        )

        incoming_message = await queue.get(timeout=5)
        # close aiormq channel to emulate abrupt connection/channel close
        await channel.channel.close()
        with pytest.raises(aiormq.exceptions.ChannelInvalidStateError):
            async with incoming_message.process():
                # emulate some activity on closed channel
                await channel.channel.basic_publish(
                    "dummy", exchange="", routing_key="non_existent"
                )

        # emulate connection/channel restoration of connect_robust
        await channel.reopen()

        # cleanup queue
        incoming_message = await queue.get(timeout=5)
        async with incoming_message.process():
            pass
        await queue.unbind(exchange, routing_key)

    async def test_robust_duplicate_queue(self):
        queue_name = "test"
        channel1 = await self.create_channel()
        channel2 = await self.create_channel()

        shared = []
        queue1 = await channel1.declare_queue(queue_name)
        queue2 = await channel1.declare_queue(queue_name)

        async def reader(queue):
            nonlocal shared
            async with queue.iterator() as q:
                async for message in q:
                    shared.append(message)
                    await message.ack()

        reader_task1 = self.loop.create_task(reader(queue1))
        reader_task2 = self.loop.create_task(reader(queue2))
        self.addCleanup(reader_task1.cancel)
        self.addCleanup(reader_task2.cancel)

        for _ in range(5):
            await channel2.default_exchange.publish(
                Message(b''), queue_name,
            )

        logging.info("Disconnect all clients")
        await self.proxy.disconnect()

        logging.info("Waiting for reconnect")
        await asyncio.sleep(5)

        logging.info("Waiting connections")
        await asyncio.wait([
            channel1._connection.ready(),
            channel2._connection.ready()
        ])

        for _ in range(5):
            await channel2.default_exchange.publish(
                Message(b''), queue_name,
            )

        while len(shared) < 10:
            await asyncio.sleep(0.1)

        self.assertEqual(len(shared), 10)


class TestCaseNoRobust(AMQPTestCase):
    async def create_connection(self, cleanup=True) -> Connection:
        client = await connect_robust(str(AMQP_URL), loop=self.loop)
        if cleanup:
            self.addCleanup(client.close)
        return client

    async def declare_queue(self, *args, **kwargs) -> Queue:
        kwargs = kwargs.copy()
        kwargs['robust'] = False
        return await super().declare_queue(*args, **kwargs)

    async def declare_exchange(self, *args, **kwargs) -> Exchange:
        kwargs = kwargs.copy()
        kwargs['robust'] = False
        return await super().declare_exchange(*args, **kwargs)
