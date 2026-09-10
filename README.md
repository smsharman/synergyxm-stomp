# synergyxm-stomp

STOMP 1.2 over WebSocket clients for SynergyXM workers, against RabbitMQ's
`rabbitmq_web_stomp` plugin. One implementation per language; every worker
depends on this repo instead of carrying its own broker code.

- `python/` — `synergyxm_stomp` (`Broker`, `ws_url`, frame codec). Depends on
  `websocket-client` only.
- `clojure/` — `synergyxm.stomp` (`connect`, `subscribe!`, `publish!`,
  `reconnect!`, `run-with-refresh!`, `ws-url`). JDK `java.net.http.WebSocket`;
  no extra dependency.

Why STOMP over WebSocket, and the exact AMQP → STOMP mapping, are in
`UPDATE_WORKERS.md` in the [moshy-cam](https://github.com/smsharman/moshy-cam)
repo and in `doc/WORKER.md` on the server.

## Using it

Python (`pyproject.toml`):

```toml
dependencies = ["synergyxm-stomp @ git+https://github.com/smsharman/synergyxm-stomp.git@<sha>#subdirectory=python"]
```

Clojure (`deps.edn`):

```clojure
io.github.smsharman/synergyxm-stomp {:git/url "https://github.com/smsharman/synergyxm-stomp.git"
                                     :git/sha "<sha>" :deps/root "clojure"}
```

## Tests

```bash
cd python  && python -m venv .venv && .venv/bin/pip install -e '.[dev]' && .venv/bin/pytest
cd clojure && clojure -M:test
```

Live checks against a real broker are gated on `SYNERGYXM_LIVE_BASE_URL`,
`SYNERGYXM_LIVE_API_KEY` (a node access key) and optionally
`SYNERGYXM_LIVE_WS_URL` / `SYNERGYXM_LIVE_MGMT_URL`.
