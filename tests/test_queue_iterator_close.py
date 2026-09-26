import asyncio
from unittest.mock import MagicMock

import pytest

from aio_pika.queue import QueueIterator
from aio_pika.robust_queue import RobustQueueIterator


@pytest.fixture(params=[QueueIterator, RobustQueueIterator])
async def iterator(request):
    queue = MagicMock()
    queue.channel.closed.return_value = (
        asyncio.get_running_loop().create_future()
    )
    queue.channel._connection.closed.return_value = (
        asyncio.get_running_loop().create_future()
    )
    result = request.param(queue, timeout=0.01)
    result._consumer_tag = "test-consumer"
    return result


@pytest.mark.parametrize("close_error", [None, RuntimeError, TimeoutError])
@pytest.mark.parametrize(
    "body_error", [None, ValueError, asyncio.CancelledError]
)
async def test_context_observes_deferred_close(
    iterator, monkeypatch, caplog, close_error, body_error
):
    release = asyncio.Event()
    finished = asyncio.Event()
    calls = 0

    async def close():
        nonlocal calls
        calls += 1
        try:
            await release.wait()
            if close_error:
                raise close_error("cleanup failed")
        finally:
            finished.set()

    monkeypatch.setattr(iterator, "close", close)
    try:
        with pytest.raises(TimeoutError):
            await anext(iterator)
        closing = iterator._QueueIterator__closing
        release.set()
        await finished.wait()

        async def exit_context():
            async with iterator:
                if body_error:
                    raise body_error("original error")

        expected = body_error or close_error
        if expected:
            with pytest.raises(expected) as exc:
                await exit_context()
            assert str(exc.value) == (
                "original error" if body_error else "cleanup failed"
            )
        else:
            await exit_context()
        assert calls == 1
        assert not hasattr(iterator, "_QueueIterator__closing")
        assert iterator._closed.done()
        if body_error and close_error:
            assert "cleanup failed" in caplog.text
    finally:
        release.set()
        closing = getattr(iterator, "_QueueIterator__closing", None)
        if closing is not None:
            await asyncio.gather(closing, return_exceptions=True)


async def test_context_cancels_expired_cleanup(iterator, monkeypatch):
    finished = asyncio.Event()

    async def close():
        try:
            await asyncio.Future()
        finally:
            finished.set()

    monkeypatch.setattr(iterator, "close", close)
    with pytest.raises(TimeoutError):
        await anext(iterator)
    closing = iterator._QueueIterator__closing
    exiting = asyncio.create_task(iterator.__aexit__(None, None, None))
    try:
        done, _ = await asyncio.wait({exiting}, timeout=1)
        assert exiting in done
        await exiting
        assert closing.done()
        assert finished.is_set()
        assert not hasattr(iterator, "_QueueIterator__closing")
        assert iterator._closed.done()
    finally:
        exiting.cancel()
        closing.cancel()
        await asyncio.gather(exiting, closing, return_exceptions=True)


async def test_repeated_cancellation_during_deferred_close(
    iterator, monkeypatch
):
    cancelling = asyncio.Event()

    async def close():
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            cancelling.set()
            await asyncio.Future()

    monkeypatch.setattr(iterator, "close", close)
    with pytest.raises(TimeoutError):
        await anext(iterator)
    closing = iterator._QueueIterator__closing
    exiting = asyncio.create_task(
        iterator.__aexit__(
            asyncio.CancelledError, asyncio.CancelledError(), None
        )
    )
    try:
        await asyncio.wait_for(cancelling.wait(), 1)
        exiting.cancel("second cancellation")
        done, _ = await asyncio.wait({exiting}, timeout=1)
        assert exiting in done
        with pytest.raises(asyncio.CancelledError, match="second cancellation"):
            await exiting
        assert closing.done()
        assert iterator._closed.done()
    finally:
        exiting.cancel()
        closing.cancel()
        await asyncio.gather(exiting, closing, return_exceptions=True)
