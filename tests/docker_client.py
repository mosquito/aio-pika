import http.client
import json
import os
import socket
from dataclasses import dataclass
from urllib.parse import urlparse


DEFAULT_DOCKER_SOCKET = "/var/run/docker.sock"


class DockerNotAvailableError(Exception):
    """Raised when Docker is not available."""


@dataclass
class DockerHostInfo:
    """Parsed DOCKER_HOST connection info."""
    is_tcp: bool
    # For Unix socket
    socket_path: str | None = None
    # For TCP
    host: str | None = None
    port: int | None = None

    @property
    def container_host(self) -> str:
        """Host where container ports are accessible."""
        if self.is_tcp:
            return self.host or "127.0.0.1"
        return "127.0.0.1"


def parse_docker_host(docker_host: str | None = None) -> DockerHostInfo:
    """Parse DOCKER_HOST environment variable or use default."""
    if docker_host is None:
        docker_host = os.environ.get("DOCKER_HOST", "")

    if not docker_host:
        return DockerHostInfo(
            is_tcp=False,
            socket_path=DEFAULT_DOCKER_SOCKET,
        )

    if docker_host.startswith("unix://"):
        return DockerHostInfo(
            is_tcp=False,
            socket_path=docker_host[7:],
        )

    if docker_host.startswith("tcp://"):
        parsed = urlparse(docker_host)
        return DockerHostInfo(
            is_tcp=True,
            host=parsed.hostname or "127.0.0.1",
            port=parsed.port or 2375,
        )

    # Assume it's a socket path
    return DockerHostInfo(
        is_tcp=False,
        socket_path=docker_host,
    )


def check_docker_available() -> DockerHostInfo:
    """
    Check if Docker is available and raise DockerNotAvailableError if not.

    Returns DockerHostInfo on success for use by DockerClient.
    """
    docker_host_env = os.environ.get("DOCKER_HOST", "")
    info = parse_docker_host(docker_host_env)

    if info.is_tcp:
        # TCP connection to remote Docker
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((info.host, info.port))
            sock.close()
            return info
        except (socket.error, OSError) as e:
            raise DockerNotAvailableError(
                f"Cannot connect to Docker daemon at "
                f"tcp://{info.host}:{info.port}: {e}\n"
                "\n"
                "Docker is required to run the test suite.\n"
                "\n"
                "Possible solutions:\n"
                "  1. Check if Docker daemon is running on remote host\n"
                "  2. Verify DOCKER_HOST is correct\n"
                "  3. Check firewall/network connectivity\n",
            ) from e
    else:
        # Unix socket connection
        socket_path = info.socket_path
        assert socket_path is not None

        if not os.path.exists(socket_path):
            hints = [
                "Docker is required to run the test suite.",
                "",
                "Possible solutions:",
                "  1. Start Docker daemon",
                "  2. Set DOCKER_HOST environment variable:",
                "     export DOCKER_HOST=unix:///var/run/docker.sock",
                "     export DOCKER_HOST=tcp://remote-host:2375",
                "",
            ]
            if docker_host_env:
                hints.insert(
                    0,
                    f"DOCKER_HOST is set to '{docker_host_env}' "
                    f"but socket '{socket_path}' does not exist.",
                )
            else:
                hints.insert(
                    0,
                    f"Docker socket not found at '{socket_path}' "
                    "and DOCKER_HOST is not set.",
                )
            raise DockerNotAvailableError("\n".join(hints))

        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect(socket_path)
            sock.close()
            return info
        except (socket.error, OSError) as e:
            raise DockerNotAvailableError(
                f"Cannot connect to Docker daemon at '{socket_path}': {e}\n"
                "\n"
                "Docker is required to run the test suite.\n"
                "\n"
                "Possible solutions:\n"
                "  1. Start Docker daemon\n"
                "  2. Check Docker socket permissions\n"
                "  3. Add your user to the 'docker' group\n",
            ) from e


@dataclass
class ContainerInfo:
    id: str
    ports: dict[str, int]  # {"5672/tcp": 32789, "5671/tcp": 32790}
    host: str  # "127.0.0.1" for local, remote host for tcp://


class UnixHTTPConnection(http.client.HTTPConnection):
    """HTTP connection over Unix socket for Docker API."""

    def __init__(self, socket_path: str):
        super().__init__("localhost")
        self.socket_path = socket_path

    def connect(self) -> None:
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(self.socket_path)


class DockerClient:
    def __init__(self, docker_host_info: DockerHostInfo | None = None):
        if docker_host_info is None:
            docker_host_info = parse_docker_host()
        self.docker_host_info = docker_host_info

    def _get_connection(self) -> http.client.HTTPConnection:
        if self.docker_host_info.is_tcp:
            assert self.docker_host_info.host is not None
            return http.client.HTTPConnection(
                self.docker_host_info.host,
                self.docker_host_info.port,
            )
        assert self.docker_host_info.socket_path is not None
        return UnixHTTPConnection(self.docker_host_info.socket_path)

    def _request(
        self, method: str, path: str, body: dict | None = None,
    ) -> dict | list | None:
        conn = self._get_connection()
        try:
            headers = {"Content-Type": "application/json"} if body else {}
            conn.request(
                method, path,
                body=json.dumps(body) if body else None,
                headers=headers,
            )
            resp = conn.getresponse()
            data = resp.read()
            if resp.status >= 400:
                raise RuntimeError(
                    f"Docker API error: {resp.status} {data.decode()}",
                )
            return json.loads(data) if data else None
        finally:
            conn.close()

    def _request_stream(self, method: str, path: str) -> None:
        """Make a request that returns a stream (e.g., image pull)."""
        conn = self._get_connection()
        try:
            conn.request(method, path)
            resp = conn.getresponse()
            # Consume the stream
            while True:
                chunk = resp.read(4096)
                if not chunk:
                    break
            if resp.status >= 400:
                raise RuntimeError(f"Docker API error: {resp.status}")
        finally:
            conn.close()

    def pull(self, image: str) -> None:
        """Pull an image from registry."""
        # Parse image name and tag
        if ":" in image:
            from_image, tag = image.rsplit(":", 1)
        else:
            from_image, tag = image, "latest"
        path = f"/images/create?fromImage={from_image}&tag={tag}"
        self._request_stream("POST", path)

    def create(
        self, image: str, ports: list[str],
        environment: dict[str, str] | None = None,
    ) -> str:
        """Create a container and return its ID."""
        # Build ExposedPorts config
        exposed_ports: dict[str, dict] = {}
        port_bindings: dict[str, list[dict[str, str]]] = {}
        for port in ports:
            exposed_ports[port] = {}
            # Bind to random host port
            port_bindings[port] = [{"HostIp": "0.0.0.0", "HostPort": ""}]

        # Build environment list
        env_list: list[str] = []
        if environment:
            env_list = [f"{k}={v}" for k, v in environment.items()]

        body = {
            "Image": image,
            "ExposedPorts": exposed_ports,
            "HostConfig": {
                "PortBindings": port_bindings,
                "PublishAllPorts": False,
            },
        }
        if env_list:
            body["Env"] = env_list

        result = self._request("POST", "/containers/create", body)
        assert isinstance(result, dict)
        return result["Id"]

    def start(self, container_id: str) -> None:
        """Start a container."""
        self._request("POST", f"/containers/{container_id}/start")

    def inspect(self, container_id: str) -> dict:
        """Inspect a container."""
        result = self._request("GET", f"/containers/{container_id}/json")
        assert isinstance(result, dict)
        return result

    def stop(self, container_id: str, timeout: int = 10) -> None:
        """Stop a container gracefully."""
        self._request("POST", f"/containers/{container_id}/stop?t={timeout}")

    def kill(self, container_id: str) -> None:
        """Kill a container immediately."""
        self._request("POST", f"/containers/{container_id}/kill")

    def remove(self, container_id: str) -> None:
        """Remove a container."""
        self._request("DELETE", f"/containers/{container_id}")

    def _parse_ports(self, info: dict) -> dict[str, int]:
        """Parse port mappings from container inspect result."""
        ports: dict[str, int] = {}
        network_settings = info.get("NetworkSettings", {})
        port_bindings = network_settings.get("Ports", {})
        for container_port, bindings in port_bindings.items():
            if bindings:
                # Take the first binding's host port
                host_port = int(bindings[0]["HostPort"])
                ports[container_port] = host_port
        return ports

    def run(
        self, image: str, ports: list[str],
        environment: dict[str, str] | None = None,
    ) -> ContainerInfo:
        """Pull, create, start, inspect - return ContainerInfo."""
        self.pull(image)
        container_id = self.create(image, ports, environment=environment)
        self.start(container_id)
        info = self.inspect(container_id)
        return ContainerInfo(
            id=container_id,
            ports=self._parse_ports(info),
            host=self.docker_host_info.container_host,
        )
