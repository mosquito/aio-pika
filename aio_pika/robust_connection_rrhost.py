from typing import List, Optional, Any
from yarl import URL
from aio_pika.robust_connection import RobustConnection
from aio_pika.connection import make_url
from urllib.parse import urlparse
from .log import get_logger

log = get_logger(__name__)


class RobustConnectionRRHost:
    """
    Robust AMQP connection with round-robin host selection.

    This class manages a single RobustConnection instance internally,
    cycling through provided URLs until a successful connection is made.
    """

    def __init__(
        self, urls: List[str], default_port: int = 5672, **kwargs: Any
    ):
        """
        Initialize with a list of broker URLs, normalizing and applying
        default port if missing.

        :param urls: List of AMQP broker URLs as strings.
        :param default_port: Default port used if not specified in URLs.
        :param kwargs: Additional arguments passed to RobustConnection.
        """
        self.urls: List[URL] = []
        for url in urls:
            parsed = urlparse(url)
            if not parsed.scheme:
                url = f"amqp://{url}"
            url_obj = make_url(url)
            if not url_obj.host:
                raise ValueError(f"Host missing in URL {url_obj}")
            if url_obj.port is None:
                url_obj = URL.build(
                    scheme=url_obj.scheme,
                    user=url_obj.user,
                    password=url_obj.password,
                    host=url_obj.host,
                    port=default_port,
                    path=url_obj.path,
                    query=url_obj.query,
                    fragment=url_obj.fragment,
                )
            self.urls.append(url_obj)
        self._current_index = 0
        self._kwargs = kwargs
        self._connection: Optional[RobustConnection] = None
        self._connect_timeout: Optional[float] = None

    async def connect(self, timeout: Optional[float] = None) -> None:
        """
        Attempt to connect to one of the provided URLs in round-robin order.
        """
        self._connect_timeout = timeout
        last_exc: Optional[Exception] = None
        for _ in range(len(self.urls)):
            url = self.urls[self._current_index]
            try:
                self._connection = RobustConnection(url, **self._kwargs)
                await self._connection.connect(timeout=timeout)
                return
            except Exception as e:
                last_exc = e
                self._current_index = (self._current_index + 1) % len(self.urls)
        raise last_exc or RuntimeError("All connection attempts failed")

    async def reconnect(self) -> None:
        """
        Perform reconnection to the next URL in round-robin order.
        """
        self._current_index = (self._current_index + 1) % len(self.urls)
        try:
            await self.connect(timeout=self._connect_timeout)
            if self._connection:
                await self._connection.reconnect_callbacks()
        except Exception as e:
            log.info(
                f"Reconnect failed on {self.urls[self._current_index]}: {e}"
            )

    async def _on_connection_close(self, closing: Any) -> None:
        """
        Internal callback triggered on connection close to attempt reconnection.
        """
        if self._connection and not self._connection.is_closed:
            await self.reconnect()
        if self._connection:
            await self._connection._on_connection_close(closing)

    @property
    def is_closed(self) -> bool:
        return self._connection.is_closed if self._connection else True

    async def close(self) -> None:
        if self._connection:
            await self._connection.close()

    def __getattr__(self, name: str) -> Any:
        if self._connection:
            return getattr(self._connection, name)
        raise AttributeError(
            f"'RobustConnectionRRHost' object has no attribute '{name}'"
        )

__all__ = (
    "RobustConnectionRRHost"
)
