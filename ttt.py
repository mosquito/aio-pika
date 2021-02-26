#!/usr/bin/env python3
import asyncio
import json
import logging

import aio_pika
import aiomisc


class Worker:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.queue = None
        self.queue_name = None

    async def run(self):
        self.connection = await aio_pika.connect()

        self.channel = await self.connection.channel()
        # await self.channel.set_qos(
        #     prefetch_count=2
        # )  # set to 0 to not trigger error
        await self.channel.flow(True)

        self.queue = await self.channel.declare_queue(name="minimal_test")
        self.queue_name = self.queue.name

        await self.publish({"number": 4})  # bootstrap queue
        await self.queue.consume(self.counter)

    async def publish(self, data):
        message = aio_pika.Message(
            bytes(json.dumps(data), encoding="utf-8"), content_encoding="utf-8",
        )
        await self.channel.default_exchange.publish(
            message, routing_key=self.queue_name,
        )

    async def counter(self, item: aio_pika.IncomingMessage):
        with item.process():
            received = json.loads(item.body)

            print(f"Received number: {received['number']}")

            publishers = [
                self.publish({"number": number})
                for number in range(
                    int(received["number"]), received["number"] * 2, 2,
                )
            ]
            await asyncio.gather(*publishers)


if __name__ == "__main__":
    with aiomisc.entrypoint(log_config=False) as loop:
        INFO_LOGGERS = (
            # 'aio_pika.exchange',
        )

        for logger_name in INFO_LOGGERS:
            logging.getLogger(logger_name).setLevel(logging.INFO)

        worker = Worker()
        asyncio.ensure_future(worker.run(), loop=loop)
        loop.run_forever()
