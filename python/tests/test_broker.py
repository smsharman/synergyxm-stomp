"""Broker against a fake WebSocket in place of websocket.create_connection."""

from __future__ import annotations

import time
from collections import deque

import pytest
import websocket

from synergyxm_stomp import Broker, ConnectionLost, StompError, ws_url
from synergyxm_stomp import broker as broker_mod
from synergyxm_stomp.frames import Decoder, encode

BIN = websocket.ABNF.OPCODE_BINARY


class FakeWS:
    """Enough of websocket.WebSocket for Broker: records what was sent,
    replays what the test queued, and times out when the queue is empty."""

    def __init__(self, url: str, **opts) -> None:
        self.url = url
        self.opts = opts
        self.connected = True
        self.timeout: float | None = None
        self.sent: list[bytes] = []
        self.incoming: deque = deque()
        self.close_calls = 0

    # -- the websocket.WebSocket surface Broker uses
    def settimeout(self, value):
        self.timeout = value

    def send_binary(self, data: bytes):
        if not self.connected:
            raise websocket.WebSocketConnectionClosedException("socket is already closed")
        self.sent.append(bytes(data))

    def recv_data(self):
        if not self.incoming:
            # behave like a blocking socket rather than spinning hot
            time.sleep(min(self.timeout or 0.01, 0.01))
            raise websocket.WebSocketTimeoutException("timed out")
        return self.incoming.popleft()

    def close(self):
        self.close_calls += 1
        self.connected = False

    # -- test helpers
    def push(self, payload: bytes, opcode=BIN):
        self.incoming.append((opcode, payload))

    @property
    def frames(self):
        """Everything the broker sent, decoded (heart-beats decode to nothing)."""
        d = Decoder()
        out = []
        for chunk in self.sent:
            out.extend(d.feed(chunk))
        return out

    def frames_of(self, command):
        return [f for f in self.frames if f.command == command]

    def one(self, command):
        got = self.frames_of(command)
        assert len(got) == 1, f"expected exactly one {command}, got {len(got)}"
        return got[0]


@pytest.fixture
def sockets(monkeypatch):
    """List of FakeWS created, newest last. Each answers CONNECT with CONNECTED."""
    created: list[FakeWS] = []

    def create_connection(url, **opts):
        ws = FakeWS(url, **opts)
        ws.push(encode("CONNECTED", {"version": "1.2", "heart-beat": "10000,10000"}))
        created.append(ws)
        return ws

    monkeypatch.setattr(broker_mod.websocket, "create_connection", create_connection)
    return created


@pytest.fixture
def broker(sockets):
    b = Broker("ws://h:15674/ws", vhost="jobs", token="tok-1", heartbeat_ms=10_000, connect_timeout=2.0)
    yield b
    b.close()


def connected_broker(broker, sockets):
    broker.connect()
    return sockets[-1]


# -- connect ------------------------------------------------------------------


def test_connect_sends_a_connect_frame_and_returns_on_connected(broker, sockets):
    broker.connect()
    ws = sockets[-1]
    assert ws.url == "ws://h:15674/ws"
    assert ws.opts["subprotocols"] == ["v12.stomp", "v11.stomp"]
    assert "sslopt" not in ws.opts
    f = ws.one("CONNECT")
    assert f.headers == {
        "accept-version": "1.2,1.1",
        "host": "jobs",
        "login": "",
        "passcode": "tok-1",
        "heart-beat": "10000,10000",
    }
    assert broker.connected is True


def test_connect_over_wss_passes_sslopt(sockets):
    b = Broker("wss://h:15674/ws", vhost="jobs", token="t")
    b.connect()
    assert sockets[-1].opts["sslopt"] == {}
    b.close()


def test_connect_tolerates_heartbeats_before_connected(sockets, monkeypatch):
    def create_connection(url, **opts):
        ws = FakeWS(url, **opts)
        ws.push(b"\n")
        ws.push(encode("CONNECTED", {"version": "1.2"}))
        sockets.append(ws)
        return ws

    monkeypatch.setattr(broker_mod.websocket, "create_connection", create_connection)
    b = Broker("ws://h/ws", vhost="jobs", token="t", connect_timeout=2.0)
    b.connect()
    assert b.connected
    b.close()


def test_error_frame_during_connect_raises_and_closes(broker, sockets, monkeypatch):
    def create_connection(url, **opts):
        ws = FakeWS(url, **opts)
        ws.push(encode("ERROR", {"message": "not_authorised"}, b"bad passcode"))
        sockets.append(ws)
        return ws

    monkeypatch.setattr(broker_mod.websocket, "create_connection", create_connection)
    with pytest.raises(StompError) as e:
        broker.connect()
    assert "not_authorised" in str(e.value) and "bad passcode" in str(e.value)
    assert sockets[-1].close_calls == 1
    assert broker.connected is False


def test_connect_times_out_without_connected(sockets, monkeypatch):
    def create_connection(url, **opts):
        ws = FakeWS(url, **opts)
        sockets.append(ws)
        return ws

    monkeypatch.setattr(broker_mod.websocket, "create_connection", create_connection)
    b = Broker("ws://h/ws", vhost="jobs", token="t", connect_timeout=0.05)
    with pytest.raises(StompError, match="timed out"):
        b.connect()


# -- consuming ------------------------------------------------------------------


def test_consume_subscribes_with_client_individual_acks(broker, sockets):
    ws = connected_broker(broker, sockets)
    gen = broker.consume("jobs.site.node", inactivity_timeout=0.02)
    assert next(gen) is None  # subscribed, then an inactivity tick
    f = ws.one("SUBSCRIBE")
    assert f.headers == {
        "id": "sub-1",
        "destination": "/amq/queue/jobs.site.node",
        "ack": "client-individual",
        "prefetch-count": "1",
    }


def test_consume_without_a_connection_raises(broker):
    with pytest.raises(ConnectionLost):
        next(broker.consume("q"))


def test_message_frame_yields_a_message(broker, sockets):
    ws = connected_broker(broker, sockets)
    ws.push(encode("MESSAGE", {"subscription": "sub-1", "ack": "ack-7", "message-id": "m-1"}, b'{"a":1}'))
    msg = next(broker.consume("q", inactivity_timeout=0.5))
    assert msg.body == b'{"a":1}'
    assert msg.text == '{"a":1}'
    assert msg.ack_id == "ack-7"
    assert msg.subscription == "sub-1"
    assert msg.headers["message-id"] == "m-1"


def test_ack_id_falls_back_to_message_id(broker, sockets):
    ws = connected_broker(broker, sockets)
    ws.push(encode("MESSAGE", {"subscription": "sub-1", "message-id": "m-9"}, b"x"))
    msg = next(broker.consume("q", inactivity_timeout=0.5))
    assert msg.ack_id == "m-9"


def test_two_frames_in_one_websocket_message_are_both_delivered(broker, sockets):
    ws = connected_broker(broker, sockets)
    ws.push(encode("MESSAGE", {"ack": "a1"}, b"one") + encode("MESSAGE", {"ack": "a2"}, b"two"))
    gen = broker.consume("q", inactivity_timeout=0.5)
    assert [next(gen).text for _ in range(2)] == ["one", "two"]


def test_receipt_frames_are_ignored(broker, sockets):
    ws = connected_broker(broker, sockets)
    ws.push(encode("RECEIPT", {"receipt-id": "1"}))
    ws.push(encode("MESSAGE", {"ack": "a1"}, b"after"))
    assert next(broker.consume("q", inactivity_timeout=0.5)).text == "after"


def test_none_is_yielded_on_inactivity(broker, sockets):
    connected_broker(broker, sockets)
    gen = broker.consume("q", inactivity_timeout=0.05)
    started = time.monotonic()
    assert next(gen) is None
    assert next(gen) is None
    assert time.monotonic() - started >= 0.05


def test_error_frame_during_consume_raises(broker, sockets):
    ws = connected_broker(broker, sockets)
    ws.push(encode("ERROR", {"message": "access_refused"}, b"queue gone"))
    with pytest.raises(StompError, match="access_refused"):
        next(broker.consume("q", inactivity_timeout=0.5))


def test_close_opcode_from_the_peer_raises_connection_lost(broker, sockets):
    ws = connected_broker(broker, sockets)
    ws.push(b"", opcode=websocket.ABNF.OPCODE_CLOSE)
    with pytest.raises(ConnectionLost):
        next(broker.consume("q", inactivity_timeout=0.5))


def test_ping_frames_are_skipped(broker, sockets):
    ws = connected_broker(broker, sockets)
    ws.push(b"ping", opcode=websocket.ABNF.OPCODE_PING)
    ws.push(encode("MESSAGE", {"ack": "a1"}, b"body"))
    assert next(broker.consume("q", inactivity_timeout=0.5)).text == "body"


def test_ack_and_nack_frames(broker, sockets):
    ws = connected_broker(broker, sockets)
    ws.push(encode("MESSAGE", {"subscription": "sub-1", "ack": "ack-7"}, b"x"))
    msg = next(broker.consume("q", inactivity_timeout=0.5))
    broker.ack(msg)
    assert ws.one("ACK").headers == {"id": "ack-7"}
    broker.nack(msg)
    assert ws.frames_of("NACK")[0].headers == {"id": "ack-7", "requeue": "false"}
    broker.nack(msg, requeue=True)
    assert ws.frames_of("NACK")[1].headers == {"id": "ack-7", "requeue": "true"}


# -- publishing ------------------------------------------------------------------


def test_publish_builds_the_exchange_destination(broker, sockets):
    ws = connected_broker(broker, sockets)
    broker.publish("job-events", "job.site.node.CAMERA_JOB_RECEIVED", b'{"e":1}', user_id="node-uuid")
    f = ws.one("SEND")
    assert f.headers["destination"] == "/exchange/job-events/job.site.node.CAMERA_JOB_RECEIVED"
    assert f.headers["content-type"] == "application/json"
    assert f.headers["persistent"] == "true"
    assert f.headers["user-id"] == "node-uuid"
    assert f.headers["content-length"] == "7"
    assert f.body == b'{"e":1}'


def test_publish_strips_message_id_and_keeps_other_headers(broker, sockets):
    ws = connected_broker(broker, sockets)
    broker.publish("e", "rk", "x", headers={"message-id": "nope", "x-trace": "abc"})
    f = ws.one("SEND")
    assert "message-id" not in f.headers
    assert f.headers["x-trace"] == "abc"


def test_publish_without_persistence_or_user_id(broker, sockets):
    ws = connected_broker(broker, sockets)
    broker.publish("e", "rk", "x", persistent=False, content_type="text/plain")
    f = ws.one("SEND")
    assert "persistent" not in f.headers and "user-id" not in f.headers
    assert f.headers["content-type"] == "text/plain"


def test_publish_on_a_closed_broker_raises(broker, sockets):
    connected_broker(broker, sockets)
    broker.close()
    with pytest.raises(ConnectionLost):
        broker.publish("e", "rk", "x")


# -- reconnect / close ------------------------------------------------------------


def test_reconnect_uses_the_new_token_and_resubscribes(broker, sockets):
    old = connected_broker(broker, sockets)
    gen = broker.consume("jobs.q", inactivity_timeout=0.02)
    assert next(gen) is None
    broker.reconnect("tok-2")

    assert old.frames_of("DISCONNECT"), "the old socket should be disconnected"
    assert old.close_calls == 1
    new = sockets[-1]
    assert new is not old
    assert new.one("CONNECT").headers["passcode"] == "tok-2"
    sub = new.one("SUBSCRIBE")
    assert sub.headers["destination"] == "/amq/queue/jobs.q"
    assert sub.headers["id"] == "sub-2"  # a fresh id on the fresh connection
    assert broker.token == "tok-2"

    # and the generator carries on against the new socket
    new.push(encode("MESSAGE", {"subscription": "sub-2", "ack": "a-1"}, b"after-reconnect"))
    assert next(gen).text == "after-reconnect"


def test_reconnect_without_a_token_keeps_the_old_one(broker, sockets):
    connected_broker(broker, sockets)
    broker.reconnect()
    assert sockets[-1].one("CONNECT").headers["passcode"] == "tok-1"


def test_close_sends_disconnect_and_is_idempotent(broker, sockets):
    ws = connected_broker(broker, sockets)
    broker.close()
    assert ws.frames_of("DISCONNECT")
    assert ws.close_calls == 1
    assert broker.connected is False
    broker.close()  # no raise, no second DISCONNECT
    assert len(ws.frames_of("DISCONNECT")) == 1
    assert ws.close_calls == 1


def test_close_during_consume_ends_the_generator(broker, sockets):
    connected_broker(broker, sockets)
    gen = broker.consume("q", inactivity_timeout=0.02)
    assert next(gen) is None
    broker.close()
    with pytest.raises(StopIteration):
        next(gen)


# -- heart-beats ------------------------------------------------------------------


def test_heartbeat_thread_sends_a_newline(sockets):
    b = Broker("ws://h/ws", vhost="jobs", token="t", heartbeat_ms=20)
    b.connect()
    ws = sockets[-1]
    assert ws.one("CONNECT").headers["heart-beat"] == "20,20"
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline and b"\n" not in ws.sent:
        time.sleep(0.01)
    assert b"\n" in ws.sent
    assert ws.frames_of("CONNECT"), "heart-beats must not corrupt the frame stream"
    b.close()
    beats = ws.sent.count(b"\n")
    time.sleep(0.1)
    assert ws.sent.count(b"\n") == beats, "the heart-beat thread should stop on close"


# -- ws_url ------------------------------------------------------------------------


def test_ws_url_override_wins():
    broker = {"host": "example.com", "ws-url": "ws://advertised:15674/ws"}
    assert ws_url(broker, "ws://override/ws") == "ws://override/ws"


def test_ws_url_uses_the_advertised_url():
    assert ws_url({"host": "example.com", "ws-url": "ws://a:15674/ws"}) == "ws://a:15674/ws"


@pytest.mark.parametrize(
    "host,expected",
    [
        ("192.168.1.241", "ws://192.168.1.241:15674/ws"),
        ("localhost", "ws://localhost:15674/ws"),
        ("127.0.0.1", "ws://127.0.0.1:15674/ws"),
        ("::1", "ws://::1:15674/ws"),
        ("10.0.0.5", "ws://10.0.0.5:15674/ws"),
        ("broker.synergyxm.com", "wss://broker.synergyxm.com:15674/ws"),
    ],
)
def test_ws_url_derives_the_scheme_from_the_host(host, expected):
    assert ws_url({"host": host, "port": 5672}) == expected


def test_ws_url_defaults_to_localhost():
    assert ws_url({}) == "ws://localhost:15674/ws"


def test_ws_url_tls_forces_the_scheme():
    assert ws_url({"host": "localhost"}, None, True) == "wss://localhost:15674/ws"
    assert ws_url({"host": "broker.synergyxm.com"}, None, False) == "ws://broker.synergyxm.com:15674/ws"
