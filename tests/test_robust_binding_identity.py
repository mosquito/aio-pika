import pytest

import aio_pika
from tests import get_random_name


@pytest.fixture
def connection_fabric():
    return aio_pika.connect_robust


@pytest.fixture
async def binding_topology(declare_exchange, declare_queue):
    source = await declare_exchange(get_random_name("source"))
    destination = await declare_exchange(
        get_random_name("destination"),
        type=aio_pika.ExchangeType.FANOUT,
    )
    queue = await declare_queue(get_random_name("queue"))
    await queue.bind(destination)
    return source, destination, queue


async def publish_and_get(source, queue, routing_key):
    await source.publish(
        aio_pika.Message(routing_key.encode()),
        routing_key,
        mandatory=False,
    )
    return await queue.get(no_ack=True, fail=False)


@pytest.mark.parametrize("source_as_name", [False, True])
async def test_restore_multiple_exchange_routing_keys(
    binding_topology,
    channel,
    source_as_name,
):
    source, destination, queue = binding_topology
    reference = source.name if source_as_name else source
    routing_keys = ("first", "second")
    for routing_key in routing_keys:
        await destination.bind(reference, routing_key)
        message = await publish_and_get(source, queue, routing_key)
        assert message is not None
        assert message.body == routing_key.encode()

    underlay = await channel.get_underlay_channel()
    for routing_key in routing_keys:
        await underlay.exchange_unbind(
            destination=destination.name,
            source=source.name,
            routing_key=routing_key,
        )

    await destination.restore()
    for routing_key in routing_keys:
        message = await publish_and_get(source, queue, routing_key)
        assert message is not None
        assert message.body == routing_key.encode()


@pytest.mark.parametrize("source_as_name", [False, True])
@pytest.mark.parametrize("removed_key", ["first", "second"])
async def test_unbind_preserves_other_exchange_routing_key(
    binding_topology,
    channel,
    source_as_name,
    removed_key,
):
    source, destination, queue = binding_topology
    reference = source.name if source_as_name else source
    for routing_key in ("first", "second"):
        await destination.bind(reference, routing_key)

    await destination.unbind(reference, removed_key)
    remaining_key = "second" if removed_key == "first" else "first"
    assert await publish_and_get(source, queue, removed_key) is None
    assert await publish_and_get(source, queue, remaining_key) is not None

    underlay = await channel.get_underlay_channel()
    await underlay.exchange_unbind(
        destination=destination.name,
        source=source.name,
        routing_key=remaining_key,
    )

    await destination.restore()
    assert await publish_and_get(source, queue, removed_key) is None
    assert await publish_and_get(source, queue, remaining_key) is not None


@pytest.mark.parametrize("bind_as_name", [False, True])
@pytest.mark.parametrize("binding_owner", ["exchange", "queue"])
async def test_unbind_accepts_another_exchange_reference_form(
    binding_topology,
    bind_as_name,
    binding_owner,
):
    source, destination, queue = binding_topology
    target = destination if binding_owner == "exchange" else queue
    bind_reference = source.name if bind_as_name else source
    unbind_reference = source if bind_as_name else source.name
    await target.bind(bind_reference, "route")
    assert await publish_and_get(source, queue, "route") is not None

    await target.unbind(unbind_reference, "route")
    assert await publish_and_get(source, queue, "route") is None

    await target.restore()
    assert await publish_and_get(source, queue, "route") is None
