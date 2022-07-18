import asyncio
import ssl

import aio_pika


async def main() -> None:
    """
    This example demonstrates connection to broker which configured to use
    external authentication method, using ssl client certificate without
    login or password.

    More info: https://www.rabbitmq.com/ssl.html#peer-verification-clients
    """
    connection = await aio_pika.connect_robust(
        host="127.0.0.1",
        auth='external',
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
