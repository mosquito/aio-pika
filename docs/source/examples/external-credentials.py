import asyncio
import ssl

import aio_pika


async def main() -> None:
    connection = await aio_pika.connect_robust(
        host="127.0.0.1",
        login="",
        ssl=True,
        ssl_options=dict(
            ca_certs="cacert.pem",
            certfile="cert.pem",
            keyfile="key.pem",
            cert_reqs=ssl.CERT_REQUIRED,
        ),
        client_properties={"connection_name": "aio-pika external credentials"},
    )

    async with connection:
        routing_key = "test_queue"

        channel = await connection.channel()

        await channel.default_exchange.publish(
            aio_pika.Message(body="Hello {}".format(routing_key).encode()),
            routing_key=routing_key,
        )


if __name__ == "__main__":
    asyncio.run(main())
