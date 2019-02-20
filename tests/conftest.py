import tracemalloc
import gc
import aiormq
import pamqp
import pytest

import aio_pika


@pytest.fixture(autouse=True)
def memory_tracer():
    tracemalloc.start()
    tracemalloc.clear_traces()

    filters = (
        tracemalloc.Filter(True, aiormq.__file__),
        tracemalloc.Filter(True, pamqp.__file__),
        tracemalloc.Filter(True, aio_pika.__file__),
    )

    snapshot_before = tracemalloc.take_snapshot().filter_traces(filters)

    try:
        yield

        gc.collect()

        snapshot_after = tracemalloc.take_snapshot().filter_traces(filters)

        top_stats = snapshot_after.compare_to(
            snapshot_before, 'lineno', cumulative=True
        )

        assert not top_stats
    finally:
        tracemalloc.stop()
