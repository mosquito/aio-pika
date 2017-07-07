import asyncio
from contextlib import suppress


class TCPProxy:
    def __init__(self, listen_host: str, listen_port: int,
                 remote_host: str, remote_port: int, *,
                 loop=None):
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.remote_host = remote_host
        self.remote_port = remote_port
        self.server = None
        self.loop = loop
        self.closing = None
        self.clients = set()

    async def start(self):
        self.server = await asyncio.start_server(
            self._handle_client, self.listen_host, self.listen_port,
            loop=self.loop
        )       # type: asyncio.AbstractServer

        self.closing = asyncio.Future(loop=self.loop)

        if self.listen_port == 0:
            self.listen_port = self.server.sockets[0].getsockname()[1]

    async def _handle_client(self, reader, writer):
        try:
            remote_reader, remote_writer = await asyncio.open_connection(
                self.remote_host, self.remote_port, loop=self.loop
            )
        except Exception as e:
            raise

        async def _reader():
            nonlocal reader  # type: asyncio.StreamReader
            nonlocal remote_writer   # type: asyncio.StreamWriter

            while not self.closing.done():
                # This is too slow but not breaks the protocol
                data = await reader.read(1)
                if not data or reader.at_eof():
                    remote_writer.write_eof()
                    break
                remote_writer.write(data)

        async def _writer():
            nonlocal writer  # type: asyncio.StreamWriter
            nonlocal remote_reader  # type: asyncio.StreamReader

            while not self.closing.done():
                # This is too slow but not breaks the protocol
                data = await remote_reader.read(1)
                if not data or reader.at_eof():
                    writer.write_eof()
                    break
                writer.write(data)

        client = asyncio.gather(_reader(), _writer(), loop=self.loop, return_exceptions=True)
        self.clients.add(client)

        try:
            await client
        finally:
            await remote_writer.drain()
            await writer.drain()

            with suppress(Exception):
                remote_writer.close()

            with suppress(Exception):
                writer.close()

    def disconnect_clients(self):
        for cl in self.clients:
            cl.cancel()

    def close(self):
        if not self.closing.done():
            self.closing.set_result(True)

        self.server.close()
        return self.server.wait_closed()

    @classmethod
    async def create(cls, listen_host="127.0.0.1", listen_port=0,
                     remote_host="127.0.0.1", remote_port=None, loop=None):
        assert remote_host

        loop = loop or asyncio.get_event_loop()
        proxy = cls(
            listen_port=listen_port,
            listen_host=listen_host,
            remote_port=remote_port,
            remote_host=remote_host,
            loop=loop
        )

        await proxy.start()
        return proxy


create_proxy = TCPProxy.create
