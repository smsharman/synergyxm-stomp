"""STOMP 1.2 frame encoding and decoding.

A frame is ``COMMAND\\nheader:value\\n...\\n\\nbody\\0``. Header values escape
``\\``, ``\\n``, ``\\r`` and ``:`` (except on CONNECT/CONNECTED, per the
spec). A lone ``\\n`` (optionally ``\\r\\n``) between frames is a heart-beat.
Bodies are read by ``content-length`` when present, otherwise up to the NUL.
"""

from __future__ import annotations

from dataclasses import dataclass, field

NUL = b"\x00"

_ESCAPES = {"\\": "\\\\", "\r": "\\r", "\n": "\\n", ":": "\\c"}
_UNESCAPES = {"\\\\": "\\", "\\r": "\r", "\\n": "\n", "\\c": ":"}


@dataclass
class Frame:
    command: str
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes = b""

    @property
    def text(self) -> str:
        return self.body.decode("utf-8", errors="replace")


def _escape(value: str, raw: bool) -> str:
    if raw:
        return value
    return "".join(_ESCAPES.get(ch, ch) for ch in value)


def _unescape(value: str, raw: bool) -> str:
    if raw or "\\" not in value:
        return value
    out = []
    i = 0
    while i < len(value):
        pair = value[i : i + 2]
        if pair in _UNESCAPES:
            out.append(_UNESCAPES[pair])
            i += 2
        else:
            out.append(value[i])
            i += 1
    return "".join(out)


def encode(command: str, headers: dict[str, str] | None = None, body: bytes | str = b"") -> bytes:
    """Serialise one frame. ``content-length`` is added for any non-empty
    body so that bodies containing NUL survive."""
    if isinstance(body, str):
        body = body.encode("utf-8")
    raw = command in ("CONNECT", "CONNECTED")
    lines = [command]
    hdrs = dict(headers or {})
    if body and "content-length" not in hdrs:
        hdrs["content-length"] = str(len(body))
    for k, v in hdrs.items():
        if v is None:
            continue
        lines.append(f"{_escape(str(k), raw)}:{_escape(str(v), raw)}")
    head = ("\n".join(lines) + "\n\n").encode("utf-8")
    return head + body + NUL


HEARTBEAT = b"\n"


class Decoder:
    """Incremental decoder: feed bytes, take complete frames.

    RabbitMQ's Web STOMP sends one frame per WebSocket message in practice,
    but nothing in the protocol guarantees it, so this buffers."""

    def __init__(self) -> None:
        self._buf = b""

    def feed(self, data: bytes | str) -> list[Frame]:
        if isinstance(data, str):
            data = data.encode("utf-8")
        self._buf += data
        frames: list[Frame] = []
        while True:
            # skip heart-beats / blank lines between frames
            while self._buf[:1] in (b"\n", b"\r"):
                self._buf = self._buf[1:]
            if not self._buf:
                break
            # end of headers: a blank line, LF- or CRLF-terminated
            lf, crlf = self._buf.find(b"\n\n"), self._buf.find(b"\n\r\n")
            if lf < 0 and crlf < 0:
                break
            sep, sep_len = (crlf, 3) if lf < 0 or (0 <= crlf < lf) else (lf, 2)
            head = self._buf[:sep].decode("utf-8", errors="replace").replace("\r\n", "\n")
            if head.endswith("\r"):  # CRLF: the last header line's own CR
                head = head[:-1]
            lines = head.split("\n")
            command = lines[0].strip()
            raw = command in ("CONNECT", "CONNECTED")
            headers: dict[str, str] = {}
            for line in lines[1:]:
                if ":" not in line:
                    continue
                k, _, v = line.partition(":")
                k = _unescape(k, raw)
                if k not in headers:  # first occurrence wins (spec)
                    headers[k] = _unescape(v, raw)
            body_start = sep + sep_len
            if "content-length" in headers:
                try:
                    n = int(headers["content-length"])
                except ValueError:
                    n = -1
                end = body_start + n
                if n < 0 or len(self._buf) < end + 1:
                    break  # incomplete
                body = self._buf[body_start:end]
                self._buf = self._buf[end + 1 :]  # drop the NUL
            else:
                nul = self._buf.find(NUL, body_start)
                if nul < 0:
                    break  # incomplete
                body = self._buf[body_start:nul]
                self._buf = self._buf[nul + 1 :]
            frames.append(Frame(command, headers, body))
        return frames
