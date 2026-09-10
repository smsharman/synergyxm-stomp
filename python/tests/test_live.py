"""End-to-end checks against a running SynergyXM server and RabbitMQ.

Skipped unless SYNERGYXM_LIVE_BASE_URL and SYNERGYXM_LIVE_API_KEY are set::

    SYNERGYXM_LIVE_BASE_URL=http://localhost:4002 \
    SYNERGYXM_LIVE_API_KEY=<node access key> \
    SYNERGYXM_LIVE_MGMT_URL=http://guest:guest@localhost:15672 \
    .venv/bin/pytest -q tests/test_live.py

SYNERGYXM_LIVE_WS_URL overrides the advertised broker URL (useful when the
server advertises a LAN address that this host cannot reach).
"""

from __future__ import annotations

import base64
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

import pytest

from synergyxm_stomp import Broker, ws_url

BASE_URL = os.environ.get("SYNERGYXM_LIVE_BASE_URL")
API_KEY = os.environ.get("SYNERGYXM_LIVE_API_KEY")
WS_URL = os.environ.get("SYNERGYXM_LIVE_WS_URL")
MGMT_URL = os.environ.get("SYNERGYXM_LIVE_MGMT_URL")

pytestmark = pytest.mark.skipif(
    not (BASE_URL and API_KEY),
    reason="set SYNERGYXM_LIVE_BASE_URL and SYNERGYXM_LIVE_API_KEY for live broker tests",
)


def _post_json(url: str, payload: dict, headers: dict | None = None) -> dict:
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"content-type": "application/json", **(headers or {})},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode())


def _mgmt_post(path: str, payload: dict) -> dict:
    parts = urllib.parse.urlsplit(MGMT_URL)
    auth = base64.b64encode(f"{parts.username or 'guest'}:{parts.password or 'guest'}".encode()).decode()
    root = urllib.parse.urlunsplit((parts.scheme, parts.hostname + (f":{parts.port}" if parts.port else ""), "", "", ""))
    return _post_json(root + path, payload, {"authorization": f"Basic {auth}"})


@pytest.fixture(scope="module")
def session() -> dict:
    """POST /auth/machine — access token, node uuid and the broker map."""
    try:
        out = _post_json(f"{BASE_URL.rstrip('/')}/auth/machine", {"api-key": API_KEY})
    except urllib.error.URLError as e:  # server not up / key rejected
        pytest.skip(f"/auth/machine unavailable: {e}")
    assert out.get("success") is True, out
    assert out["access-token"] and out["node-uuid"] and out["broker"]
    return out


@pytest.fixture()
def broker(session):
    url = ws_url(session["broker"], WS_URL)
    b = Broker(url, vhost=session["broker"].get("vhost", "jobs"), token=session["access-token"], heartbeat_ms=10_000)
    b.connect()
    yield b
    b.close()


def test_auth_machine_advertises_a_broker(session):
    b = session["broker"]
    assert b["job-queue"].startswith("jobs.")
    assert b.get("events-exchange") == "job-events"
    assert ws_url(b, WS_URL).startswith(("ws://", "wss://"))


def test_connect_and_publish_an_event(broker, session):
    queue = session["broker"]["job-queue"]
    rk = f"job.{queue[len('jobs.'):]}.CAMERA_JOB_RECEIVED"
    assert broker.connected
    broker.publish(
        session["broker"].get("events-exchange", "job-events"),
        rk,
        json.dumps({"event": "CAMERA_JOB_RECEIVED", "node": session["node-uuid"], "test-id": str(uuid.uuid4())}),
        user_id=session["node-uuid"],
    )
    # a bad publish comes back as an ERROR frame; a quiet tick means it landed
    assert next(broker.consume(queue, inactivity_timeout=2.0)) is None


@pytest.mark.skipif(not MGMT_URL, reason="set SYNERGYXM_LIVE_MGMT_URL to inject a dispatch")
def test_dispatch_is_delivered_and_acked(broker, session):
    queue = session["broker"]["job-queue"]
    vhost = session["broker"].get("vhost", "jobs")
    test_id = str(uuid.uuid4())

    consumer = broker.consume(queue, inactivity_timeout=1.0)
    assert next(consumer) is None  # subscribed and idle

    out = _mgmt_post(
        f"/api/exchanges/{urllib.parse.quote(vhost, safe='')}/jobs/publish",
        {
            "properties": {"delivery_mode": 2, "content_type": "application/json"},
            "routing_key": f"job.{queue[len('jobs.'):]}.TEST_DISPATCH",
            "payload": json.dumps({"job-type": "TEST_DISPATCH", "test-id": test_id}),
            "payload_encoding": "string",
        },
    )
    assert out.get("routed") is True, f"the dispatch was not routed to {queue}: {out}"

    deadline = time.monotonic() + 15
    got = None
    while got is None and time.monotonic() < deadline:
        msg = next(consumer)
        if msg is not None and json.loads(msg.body).get("test-id") == test_id:
            got = msg
    assert got is not None, "the injected dispatch was not delivered"
    assert got.ack_id
    broker.ack(got)

    # once acked it must not come back
    for _ in range(2):
        msg = next(consumer)
        assert msg is None or json.loads(msg.body).get("test-id") != test_id


@pytest.mark.skipif(not MGMT_URL, reason="set SYNERGYXM_LIVE_MGMT_URL to inject a dispatch")
def test_reconnect_with_a_refreshed_token_keeps_consuming(broker, session):
    queue = session["broker"]["job-queue"]
    vhost = session["broker"].get("vhost", "jobs")
    consumer = broker.consume(queue, inactivity_timeout=1.0)
    assert next(consumer) is None

    fresh = _post_json(f"{BASE_URL.rstrip('/')}/auth/machine", {"api-key": API_KEY})
    broker.reconnect(fresh["access-token"])
    assert broker.connected

    test_id = str(uuid.uuid4())
    _mgmt_post(
        f"/api/exchanges/{urllib.parse.quote(vhost, safe='')}/jobs/publish",
        {
            "properties": {},
            "routing_key": f"job.{queue[len('jobs.'):]}.TEST_DISPATCH",
            "payload": json.dumps({"job-type": "TEST_DISPATCH", "test-id": test_id}),
            "payload_encoding": "string",
        },
    )
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        msg = next(consumer)
        if msg is not None and json.loads(msg.body).get("test-id") == test_id:
            broker.ack(msg)
            return
    pytest.fail("no delivery after reconnecting with a refreshed token")
