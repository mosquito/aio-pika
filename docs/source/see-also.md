# See also

## [aiormq](http://github.com/mosquito/aiormq/)

`aiormq` is a pure python AMQP client library. It is under the hood of **aio-pika** and might to be used when you really loving works with the protocol low level.
Following examples demonstrates the user API.

Simple consumer:

```python
import asyncio
import aiormq

async def on_message(message):
    """
    on_message doesn't necessarily have to be defined as async.
    Here it is to show that it's possible.
    """
    print(f" [x] Received message {message!r}")
    print(f"Message body is: {message.body!r}")
    print("Before sleep!")
    await asyncio.sleep(5)   # Represents async I/O operations
    print("After sleep!")

async def main():
    # Perform connection
    connection = await aiormq.connect("amqp://guest:guest@localhost/")

    # Creating a channel
    channel = await connection.channel()

    # Declaring queue
    declare_ok = await channel.queue_declare('helo')
    consume_ok = await channel.basic_consume(
        declare_ok.queue, on_message, no_ack=True
    )

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
loop.run_forever()
```

Simple publisher:

```python
import asyncio
from typing import Optional

import aiormq
from aiormq.abc import DeliveredMessage

MESSAGE: Optional[DeliveredMessage] = None

async def main():
    global MESSAGE
    body = b'Hello World!'

    # Perform connection
    connection = await aiormq.connect("amqp://guest:guest@localhost//")

    # Creating a channel
    channel = await connection.channel()
    declare_ok = await channel.queue_declare("hello", auto_delete=True)

    # Sending the message
    await channel.basic_publish(body, routing_key='hello')
    print(f" [x] Sent {body}")

    MESSAGE = await channel.basic_get(declare_ok.queue)
    print(f" [x] Received message from {declare_ok.queue!r}")

loop = asyncio.get_event_loop()
loop.run_until_complete(main())

assert MESSAGE is not None
assert MESSAGE.routing_key == "hello"
assert MESSAGE.body == b'Hello World!'
```

## The [patio](https://github.com/patio-python/patio) and the [patio-rabbitmq](https://github.com/patio-python/patio-rabbitmq)

**PATIO** is an acronym for Python Asynchronous Tasks for AsyncIO - an easily extensible library, for distributed task execution, like celery, only targeting asyncio as the main design approach.

**patio-rabbitmq** provides you with the ability to use *RPC over RabbitMQ* services with extremely simple implementation:

```python
from patio import Registry, ThreadPoolExecutor
from patio_rabbitmq import RabbitMQBroker

rpc = Registry(project="patio-rabbitmq", auto_naming=False)

@rpc("sum")
def sum(*args):
    return sum(args)

async def main():
    async with ThreadPoolExecutor(rpc, max_workers=16) as executor:
        async with RabbitMQBroker(
            executor, amqp_url="amqp://guest:guest@localhost/",
        ) as broker:
            await broker.join()
```

And the caller side might be written like this:

```python
import asyncio
from patio import NullExecutor, Registry
from patio_rabbitmq import RabbitMQBroker

async def main():
    async with NullExecutor(Registry(project="patio-rabbitmq")) as executor:
        async with RabbitMQBroker(
            executor, amqp_url="amqp://guest:guest@localhost/",
        ) as broker:
            print(await asyncio.gather(
                *[
                    broker.call("mul", i, i, timeout=1) for i in range(10)
                 ]
            ))
```


## [FastStream](https://github.com/airtai/faststream)

**FastStream** is a powerful and easy-to-use Python library for building asynchronous services that interact with event streams..

If you need no deep dive into **RabbitMQ** details, you can use more high-level **FastStream** interfaces:

```python
from faststream import FastStream
from faststream.rabbit import RabbitBroker

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
app = FastStream(broker)

@broker.subscriber("user")
async def user_created(user_id: int):
    assert isinstance(user_id, int)
    return f"user-{user_id}: created"

@app.after_startup
async def pub_smth():
    assert (
        await broker.publish(1, "user", rpc=True)
    ) ==  "user-1: created"
```

Also, **FastStream** validates messages by **pydantic**, generates your project **AsyncAPI** spec, supports In-Memory testing, RPC calls, and more.

In fact, it is a high-level wrapper on top of **aio-pika**, so you can use both of these libraries' advantages at the same time.

## [python-socketio](https://python-socketio.readthedocs.io/en/latest/intro.html)

[Socket.IO](https://socket.io/) is a transport protocol that enables real-time bidirectional event-based communication between clients (typically, though not always, web browsers) and a server. This package provides Python implementations of both, each with standard and asyncio variants.

Also this package is suitable for building messaging services over **RabbitMQ** via **aio-pika** adapter:

```python
import socketio
from aiohttp import web

sio = socketio.AsyncServer(client_manager=socketio.AsyncAioPikaManager())
app = web.Application()
sio.attach(app)

@sio.event
async def chat_message(sid, data):
    print("message ", data)

if __name__ == '__main__':
    web.run_app(app)
```

And a client is able to call `chat_message` the following way:

```python
import asyncio
import socketio

sio = socketio.AsyncClient()

async def main():
    await sio.connect('http://localhost:8080')
    await sio.emit('chat_message', {'response': 'my response'})

if __name__ == '__main__':
    asyncio.run(main())
```

## The [taskiq](https://github.com/taskiq-python/taskiq) and the [taskiq-aio-pika](https://github.com/taskiq-python/taskiq-aio-pika)

**Taskiq** is an asynchronous distributed task queue for python. The project takes inspiration from big projects such as Celery and Dramatiq. But taskiq can send and run both the sync and async functions.

The library provides you with **aio-pika** broker for running tasks too.

```python
from taskiq_aio_pika import AioPikaBroker

broker = AioPikaBroker()

@broker.task
async def test() -> None:
    print("nothing")

async def main():
    await broker.startup()
    await test.kiq()
```

## [Rasa](https://rasa.com/docs/rasa/)

With over 25 million downloads, Rasa Open Source is the most popular open source framework for building chat and voice-based AI assistants.

With **Rasa**, you can build contextual assistants on:

* Facebook Messenger
* Slack
* Google Hangouts
* Webex Teams
* Microsoft Bot Framework
* Rocket.Chat
* Mattermost
* Telegram
* Twilio

Your own custom conversational channels or voice assistants as:

* Alexa Skills
* Google Home Actions

**Rasa** helps you build contextual assistants capable of having layered conversations with lots of back-and-forth. In order for a human to have a meaningful exchange with a contextual assistant, the assistant needs to be able to use context to build on things that were previously discussed – **Rasa** enables you to build assistants that can do this in a scalable way.

And it also uses **aio-pika** to interact with **RabbitMQ** deep inside!


## Thanks for contributing

* [@mosquito](https://github.com/mosquito) (author)
* [@decaz](https://github.com/decaz) (steel persuasiveness while code review)
* [@heckad](https://github.com/heckad) (bug fixes)
* [@smagafurov](https://github.com/smagafurov) (bug fixes)
* [@hellysmile](https://github.com/hellysmile) (bug fixes and ideas)
* [@altvod](https://github.com/altvod) (bug fixes)
* [@alternativehood](https://github.com/alternativehood) (bugfixes)
* [@cprieto](https://github.com/cprieto) (bug fixes)
* [@akhoronko](https://github.com/akhoronko) (bug fixes)
* [@iselind](https://github.com/iselind) (bug fixes)
* [@DXist](https://github.com/DXist) (bug fixes)
* [@blazewicz](https://github.com/blazewicz) (bug fixes)
* [@chibby0ne](https://github.com/chibby0ne) (bug fixes)
* [@jmccarrell](https://github.com/jmccarrell) (bug fixes)
* [@taybin](https://github.com/taybin) (bug fixes)
* [@ollamh](https://github.com/ollamh) (bug fixes)
* [@DriverX](https://github.com/DriverX) (bug fixes)
* [@brianmedigate](https://github.com/brianmedigate) (bug fixes)
* [@dan-stone](https://github.com/dan-stone) (bug fixes)
* [@Kludex](https://github.com/Kludex) (bug fixes)
* [@bmario](https://github.com/bmario) (bug fixes)
* [@tzoiker](https://github.com/tzoiker) (bug fixes)
* [@Pehat](https://github.com/Pehat) (bug fixes)
* [@WindowGenerator](https://github.com/WindowGenerator) (bug fixes)
* [@dhontecillas](https://github.com/dhontecillas) (bug fixes)
* [@tilsche](https://github.com/tilsche) (bug fixes)
* [@leenr](https://github.com/leenr) (bug fixes)
* [@la0rg](https://github.com/la0rg) (bug fixes)
* [@SolovyovAlexander](https://github.com/SolovyovAlexander) (bug fixes)
* [@kremius](https://github.com/kremius) (bug fixes)
* [@zyp](https://github.com/zyp) (bug fixes)
* [@kajetanj](https://github.com/kajetanj) (bug fixes)
* [@Alviner](https://github.com/Alviner) (moral support, debug sessions and good mood)
* [@Pavkazzz](https://github.com/Pavkazzz) (composure, and patience while debug sessions)
* [@bbrodriges](https://github.com/bbrodriges) (supplying grammar while writing documentation)
* [@dizballanze](https://github.com/dizballanze) (review, grammar)
