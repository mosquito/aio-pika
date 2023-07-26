from yarl import URL

import aio_pika


def test_connection_interleave(amqp_url: URL):
    url = amqp_url.update_query(interleave="1")
    connection = aio_pika.Connection(url=url)
    assert "interleave" in connection.kwargs
    assert connection.kwargs["interleave"] == 1

    connection = aio_pika.Connection(url=amqp_url)
    assert "interleave" not in connection.kwargs


def test_connection_happy_eyeballs_delay(amqp_url: URL):
    url = amqp_url.update_query(happy_eyeballs_delay=".1")
    connection = aio_pika.Connection(url=url)
    assert "happy_eyeballs_delay" in connection.kwargs
    assert connection.kwargs["happy_eyeballs_delay"] == 0.1

    connection = aio_pika.Connection(url=amqp_url)
    assert "happy_eyeballs_delay" not in connection.kwargs


def test_robust_connection_interleave(amqp_url: URL):
    url = amqp_url.update_query(interleave="1")
    connection = aio_pika.RobustConnection(url=url)
    assert "interleave" in connection.kwargs
    assert connection.kwargs["interleave"] == 1

    connection = aio_pika.RobustConnection(url=amqp_url)
    assert "interleave" not in connection.kwargs


def test_robust_connection_happy_eyeballs_delay(amqp_url: URL):
    url = amqp_url.update_query(happy_eyeballs_delay=".1")
    connection = aio_pika.RobustConnection(url=url)
    assert "happy_eyeballs_delay" in connection.kwargs
    assert connection.kwargs["happy_eyeballs_delay"] == 0.1

    connection = aio_pika.RobustConnection(url=amqp_url)
    assert "happy_eyeballs_delay" not in connection.kwargs
