import asyncio
import logging
from aio_pika.robust_connection import connect


logging.basicConfig(level=logging.DEBUG)


async def main():
    connection = await connect()
    channel1 = await connection.channel()
    channel2 = await connection.channel()

    await asyncio.sleep(2)
    print('Done')


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(main())
    loop.run_forever()
