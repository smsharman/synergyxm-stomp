"""A STOMP-over-WebSocket broker connection for SynergyXM workers.

Talks to RabbitMQ's ``rabbitmq_web_stomp`` plugin and mirrors the shape the
workers' pika code had, so a consume loop keeps its form::

    broker = Broker(ws_url, vhost="jobs", token=session.access_token)
    broker.connect()
    for msg in broker.consume(job_queue, inactivity_timeout=30):
        if session.needs_refresh():
            session.ensure_token(base_url, api_key)
            broker.reconnect(session.access_token)      # replaces update_secret
        if msg is None:
            continue                                     # inactivity tick
        try:
            handle(json.loads(msg.body))
            broker.ack(msg)
        except Exception:
            broker.nack(msg)                             # dead-letters (requeue:false)

Mapping to AMQP (see UPDATE_WORKERS.md in the MoshyCam repo):

* CONNECT ``login`` "" / ``passcode`` = node JWT, ``host`` = vhost
* SUBSCRIBE ``/amq/queue/<queue>`` ``ack:client-individual`` ``prefetch-count:1``
  (an existing queue; no configure permission needed)
* ACK / NACK (``requeue:false``) with the MESSAGE frame's ``ack`` header
* SEND ``/exchange/<exchange>/<routing-key>`` with ``user-id``,
  ``content-type``, ``persistent``. ``message-id`` is *not* allowed on SEND.

Token refresh is a reconnect: STOMP has no ``update-secret``. ``reconnect``
tears the socket down, connects with the new passcode and re-subscribes the
active consumer; the ``consume`` generator carries on.
"""

from __future__ import annotations

import logging
import socket
import threading
import time
from dataclasses import dataclass
from urllib.parse import urlparse

import websocket

from .frames import HEARTBEAT, Decoder, Frame, encode

logger = logging.getLogger(__name__)

DEFAULT_WS_PORT = 15674
DEFAULT_WS_PATH = "/ws"


class StompError(Exception):
    """The broker sent an ERROR frame, or the connection was refused."""


class ConnectionLost(Exception):
    """The WebSocket closed underneath us."""


def ws_url(broker: dict, override: str | None = None, tls: bool | None = None) -> str:
    """The WebSocket URL for a ``broker`` map from ``POST /auth/machine``.

    ``override`` (``SYNERGYXM_BROKER_WS_URL`` in worker.conf) wins; then the
    advertised ``ws-url``; else ``ws(s)://<host>:15674/ws`` — the AMQP port in
    the map is ignored because Web STOMP listens elsewhere. ``tls`` forces
    the derived scheme; by default it is ``ws`` for loopback/private hosts
    and ``wss`` otherwise.
    """
    if override:
        return override
    if broker.get("ws-url"):
        return broker["ws-url"]
    host = broker.get("host", "localhost")
    if tls is None:
        tls = not (host in ("localhost", "127.0.0.1", "::1") or host.startswith(("10.", "192.168.", "172.")))
    return f"{'wss' if tls else 'ws'}://{host}:{DEFAULT_WS_PORT}{DEFAULT_WS_PATH}"


@dataclass
class Message:
    """One delivered MESSAGE frame."""

    body: bytes
    headers: dict[str, str]
    ack_id: str
    subscription: str

    @property
    def text(self) -> str:
        return self.body.decode("utf-8", errors="replace")


class Broker:
    def __init__(
        self,
        url: str,
        vhost: str,
        token: str,
        *,
        heartbeat_ms: int = 10000,
        connect_timeout: float = 20.0,
        connection_name: str | None = None,
    ) -> None:
        self.url = url
        self.vhost = vhost
        self.token = token
        self.heartbeat_ms = heartbeat_ms
        self.connect_timeout = connect_timeout
        self.connection_name = connection_name
        self._ws: websocket.WebSocket | None = None
        self._decoder = Decoder()
        self._send_lock = threading.Lock()
        self._hb_thread: threading.Thread | None = None
        self._hb_stop = threading.Event()
        self._pending: list[Frame] = []
        self._sub: tuple[str, str] | None = None  # (id, queue)
        self._sub_seq = 0
        self._closed = False

    # -- connection ------------------------------------------------------------

    @property
    def connected(self) -> bool:
        return self._ws is not None and self._ws.connected

    def connect(self) -> None:
        """Open the WebSocket and complete the STOMP handshake."""
        self._closed = False
        parsed = urlparse(self.url)
        opts = {"subprotocols": ["v12.stomp", "v11.stomp"], "timeout": self.connect_timeout}
        if parsed.scheme == "wss":
            opts["sslopt"] = {}
        ws = websocket.create_connection(self.url, **opts)
        self._ws = ws
        self._decoder = Decoder()
        headers = {
            "accept-version": "1.2,1.1",
            "host": self.vhost,
            "login": "",
            "passcode": self.token,
            "heart-beat": f"{self.heartbeat_ms},{self.heartbeat_ms}",
        }
        self._raw_send(encode("CONNECT", headers))
        deadline = time.monotonic() + self.connect_timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self.close()
                raise StompError("timed out waiting for CONNECTED")
            frame = self._recv_frame(remaining)
            if frame is None:
                continue
            if frame.command == "CONNECTED":
                break
            if frame.command == "ERROR":
                self.close()
                raise StompError(_error_text(frame))
        logger.info("STOMP connected to %s (vhost %s)", self.url, self.vhost)
        self._start_heartbeat()
        if self._sub is not None:
            self._subscribe(self._sub[1])

    def reconnect(self, token: str | None = None) -> None:
        """Close and connect again (with a new token if given), keeping the
        active subscription."""
        if token:
            self.token = token
        sub = self._sub
        self.close()
        self._sub = sub
        self.connect()
        logger.info("STOMP reconnected%s", " with a refreshed token" if token else "")

    def close(self) -> None:
        """Close cleanly. Idempotent."""
        self._closed = True
        self._hb_stop.set()
        ws, self._ws = self._ws, None
        if ws is not None:
            try:
                if ws.connected:
                    self._raw_send(encode("DISCONNECT"), ws)
            except Exception:
                pass
            try:
                ws.close()
            except Exception:
                pass
        self._sub = None

    # -- consuming ---------------------------------------------------------------

    def consume(self, queue: str, inactivity_timeout: float | None = 30.0):
        """Generator of :class:`Message` (or ``None`` after ``inactivity_timeout``
        seconds without one, like pika's ``consume``). Subscribes with
        ``client-individual`` acks and prefetch 1. Raises :class:`ConnectionLost`
        when the socket drops and :class:`StompError` on an ERROR frame."""
        if not self.connected:
            raise ConnectionLost("not connected")
        self._subscribe(queue)
        while not self._closed:
            frame = self._recv_frame(inactivity_timeout)
            if frame is None:
                yield None
                continue
            if frame.command == "MESSAGE":
                yield Message(
                    body=frame.body,
                    headers=frame.headers,
                    ack_id=frame.headers.get("ack") or frame.headers.get("message-id", ""),
                    subscription=frame.headers.get("subscription", ""),
                )
            elif frame.command == "ERROR":
                raise StompError(_error_text(frame))
            elif frame.command == "RECEIPT":
                continue

    def ack(self, msg: Message) -> None:
        self._raw_send(encode("ACK", {"id": msg.ack_id}))

    def nack(self, msg: Message, requeue: bool = False) -> None:
        self._raw_send(encode("NACK", {"id": msg.ack_id, "requeue": "true" if requeue else "false"}))

    # -- publishing ------------------------------------------------------------

    def publish(
        self,
        exchange: str,
        routing_key: str,
        body: bytes | str,
        *,
        user_id: str | None = None,
        content_type: str = "application/json",
        persistent: bool = True,
        headers: dict[str, str] | None = None,
    ) -> None:
        """SEND to ``/exchange/<exchange>/<routing_key>``."""
        hdrs = {
            "destination": f"/exchange/{exchange}/{routing_key}",
            "content-type": content_type,
        }
        if persistent:
            hdrs["persistent"] = "true"
        if user_id:
            hdrs["user-id"] = user_id
        if headers:
            hdrs.update({k: v for k, v in headers.items() if k != "message-id"})
        self._raw_send(encode("SEND", hdrs, body))

    # -- internals -----------------------------------------------------------------

    def _subscribe(self, queue: str) -> None:
        self._sub_seq += 1
        sub_id = f"sub-{self._sub_seq}"
        self._sub = (sub_id, queue)
        self._raw_send(
            encode(
                "SUBSCRIBE",
                {
                    "id": sub_id,
                    "destination": f"/amq/queue/{queue}",
                    "ack": "client-individual",
                    "prefetch-count": "1",
                },
            )
        )

    def _raw_send(self, data: bytes, ws: websocket.WebSocket | None = None) -> None:
        ws = ws or self._ws
        if ws is None:
            raise ConnectionLost("not connected")
        with self._send_lock:
            try:
                ws.send_binary(data)
            except (websocket.WebSocketException, OSError) as e:
                raise ConnectionLost(str(e)) from e

    def _recv_frame(self, timeout: float | None) -> Frame | None:
        """Next complete frame, or None after ``timeout`` seconds of nothing."""
        if self._pending:
            return self._pending.pop(0)
        ws = self._ws
        if ws is None:
            raise ConnectionLost("not connected")
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                ws.settimeout(min(remaining, 5.0))
            else:
                ws.settimeout(5.0)
            try:
                opcode, data = ws.recv_data()
            except websocket.WebSocketTimeoutException:
                continue
            except (websocket.WebSocketConnectionClosedException, ConnectionError, socket.error) as e:
                if self._closed:
                    return None
                raise ConnectionLost(str(e)) from e
            if opcode == websocket.ABNF.OPCODE_CLOSE:
                if self._closed:
                    return None
                raise ConnectionLost("websocket closed by peer")
            if opcode in (websocket.ABNF.OPCODE_PING, websocket.ABNF.OPCODE_PONG):
                continue
            frames = self._decoder.feed(data)
            if not frames:
                continue
            self._pending.extend(frames[1:])
            return frames[0]

    def _start_heartbeat(self) -> None:
        self._hb_stop = threading.Event()
        interval = self.heartbeat_ms / 1000.0

        def run() -> None:
            while not self._hb_stop.wait(interval):
                try:
                    with self._send_lock:
                        ws = self._ws
                        if ws is not None and ws.connected:
                            ws.send_binary(HEARTBEAT)
                except Exception:
                    return

        self._hb_thread = threading.Thread(target=run, name="stomp-heartbeat", daemon=True)
        self._hb_thread.start()


def _error_text(frame: Frame) -> str:
    return f"STOMP error: {frame.headers.get('message', '')} {frame.text}".strip()
