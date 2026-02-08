import http.client
import json
import os
import socket
from dataclasses import dataclass


@dataclass
class ContainerInfo:
    id: str
    ports: dict[str, int]  # {"5672/tcp": 32789, "5671/tcp": 32790}
    host: str  # usually "127.0.0.1" or "localhost"


class UnixHTTPConnection(http.client.HTTPConnection):
    """HTTP connection over Unix socket for Docker API."""

    def __init__(self, socket_path: str = "/var/run/docker.sock"):
        super().__init__("localhost")
        self.socket_path = socket_path

    def connect(self) -> None:
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(self.socket_path)


class DockerClient:
    def __init__(self, socket_path: str | None = None):
        if socket_path is None:
            socket_path = os.environ.get(
                "DOCKER_HOST", "/var/run/docker.sock",
            )
            # Strip unix:// prefix if present
            if socket_path.startswith("unix://"):
                socket_path = socket_path[7:]
        self.socket_path = socket_path

    def _request(
        self, method: str, path: str, body: dict | None = None,
    ) -> dict | list | None:
        conn = UnixHTTPConnection(self.socket_path)
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

    def _request_stream(self, method: str, path: str) -> None:
        """Make a request that returns a stream (e.g., image pull)."""
        conn = UnixHTTPConnection(self.socket_path)
        conn.request(method, path)
        resp = conn.getresponse()
        # Consume the stream
        while True:
            chunk = resp.read(4096)
            if not chunk:
                break
        if resp.status >= 400:
            raise RuntimeError(f"Docker API error: {resp.status}")

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
        """Stop a container."""
        self._request("POST", f"/containers/{container_id}/stop?t={timeout}")

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
            host="127.0.0.1",
        )
