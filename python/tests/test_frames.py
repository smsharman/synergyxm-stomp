"""Frame codec: encoding, header escaping, and the incremental decoder."""

from __future__ import annotations

import pytest

from synergyxm_stomp.frames import Decoder, Frame, encode


# -- encode -------------------------------------------------------------------


def test_encode_shape_no_body():
    assert encode("DISCONNECT") == b"DISCONNECT\n\n\x00"


def test_encode_headers_and_body_with_content_length():
    out = encode("SEND", {"destination": "/queue/a"}, b"hi")
    assert out == b"SEND\ndestination:/queue/a\ncontent-length:2\n\nhi\x00"


def test_encode_content_length_is_bytes_not_characters():
    out = encode("SEND", {}, "é")  # 2 bytes in UTF-8
    assert out == b"SEND\ncontent-length:2\n\n\xc3\xa9\x00"


def test_encode_accepts_str_body():
    assert encode("SEND", {"destination": "/x"}, "hi") == encode("SEND", {"destination": "/x"}, b"hi")


def test_encode_keeps_an_explicit_content_length():
    out = encode("SEND", {"content-length": "99"}, b"hi")
    assert b"content-length:99" in out
    assert out.count(b"content-length") == 1


def test_encode_drops_none_valued_headers():
    out = encode("SEND", {"a": "1", "b": None, "c": "2"})
    assert out == b"SEND\na:1\nc:2\n\n\x00"


def test_encode_escapes_header_values():
    out = encode("SEND", {"a:b": "x\\y\nz\r:"})
    head = out.decode().split("\n\n")[0]
    assert head.splitlines()[1] == r"a\cb:x\\y\nz\r\c"


@pytest.mark.parametrize("command", ["CONNECT", "CONNECTED"])
def test_no_escaping_on_connect_frames(command):
    out = encode(command, {"passcode": "a:b\\c"})
    assert b"passcode:a:b\\c\n" in out


def test_escaping_does_apply_to_other_commands():
    out = encode("SEND", {"passcode": "a:b\\c"})
    assert b"passcode:a\\cb\\\\c\n" in out


# -- decode -------------------------------------------------------------------


def test_one_frame_per_chunk():
    d = Decoder()
    frames = d.feed(encode("MESSAGE", {"subscription": "sub-1"}, b"hello"))
    assert len(frames) == 1
    f = frames[0]
    assert (f.command, f.headers["subscription"], f.body) == ("MESSAGE", "sub-1", b"hello")
    assert f.text == "hello"
    assert d.feed(b"") == []


def test_two_frames_in_one_chunk():
    d = Decoder()
    frames = d.feed(encode("MESSAGE", {"id": "1"}, b"a") + encode("MESSAGE", {"id": "2"}, b"b"))
    assert [f.headers["id"] for f in frames] == ["1", "2"]
    assert [f.body for f in frames] == [b"a", b"b"]


def test_frame_split_across_chunks_in_the_headers():
    d = Decoder()
    raw = encode("MESSAGE", {"destination": "/amq/queue/q"}, b"body")
    cut = raw.index(b"destination") + 4
    assert d.feed(raw[:cut]) == []
    frames = d.feed(raw[cut:])
    assert len(frames) == 1 and frames[0].body == b"body"


def test_frame_split_across_chunks_inside_the_body():
    d = Decoder()
    raw = encode("MESSAGE", {"id": "1"}, b"0123456789")
    cut = raw.index(b"0123456789") + 4
    assert d.feed(raw[:cut]) == []
    frames = d.feed(raw[cut:])
    assert len(frames) == 1 and frames[0].body == b"0123456789"


def test_frame_split_byte_by_byte():
    raw = encode("MESSAGE", {"a": "1"}, b"hello there")
    d = Decoder()
    got = []
    for i in range(len(raw)):
        got.extend(d.feed(raw[i : i + 1]))
    assert len(got) == 1 and got[0].body == b"hello there"


def test_body_containing_nul_is_read_by_content_length():
    d = Decoder()
    body = b"a\x00b\x00c"
    frames = d.feed(encode("MESSAGE", {"id": "1"}, body))
    assert len(frames) == 1 and frames[0].body == body


def test_body_without_content_length_is_read_to_the_nul():
    d = Decoder()
    frames = d.feed(b"MESSAGE\nid:1\n\nplain\x00")
    assert len(frames) == 1 and frames[0].body == b"plain"


def test_empty_body():
    d = Decoder()
    frames = d.feed(encode("RECEIPT", {"receipt-id": "1"}))
    assert len(frames) == 1 and frames[0].body == b""


def test_leading_heart_beat_newlines_are_skipped():
    d = Decoder()
    assert d.feed(b"\n\n") == []
    frames = d.feed(b"\n" + encode("MESSAGE", {"id": "1"}, b"x") + b"\n")
    assert len(frames) == 1 and frames[0].command == "MESSAGE"
    assert d.feed(b"\r\n") == []


def test_heart_beat_between_two_frames_in_one_chunk():
    d = Decoder()
    frames = d.feed(encode("MESSAGE", {"id": "1"}) + b"\n" + encode("MESSAGE", {"id": "2"}))
    assert [f.headers["id"] for f in frames] == ["1", "2"]


def test_first_occurrence_wins_for_duplicate_headers():
    d = Decoder()
    frames = d.feed(b"MESSAGE\nid:first\nid:second\n\n\x00")
    assert frames[0].headers["id"] == "first"


def test_crlf_line_endings():
    d = Decoder()
    frames = d.feed(b"MESSAGE\r\nid:1\r\ncontent-length:5\r\n\r\nhello\x00")
    assert len(frames) == 1
    assert frames[0].command == "MESSAGE"
    assert frames[0].headers == {"id": "1", "content-length": "5"}
    assert frames[0].body == b"hello"


def test_crlf_without_content_length():
    d = Decoder()
    frames = d.feed(b"MESSAGE\r\nid:1\r\n\r\nhello\x00")
    assert len(frames) == 1 and frames[0].body == b"hello"


def test_crlf_frame_split_across_chunks():
    raw = b"MESSAGE\r\nid:1\r\ncontent-length:3\r\n\r\nabc\x00"
    d = Decoder()
    got = []
    for i in range(len(raw)):
        got.extend(d.feed(raw[i : i + 1]))
    assert len(got) == 1 and got[0].body == b"abc"


def test_decoder_unescapes_header_values():
    d = Decoder()
    frames = d.feed(encode("MESSAGE", {"a:b": "x\\y\nz\r:"}))
    assert frames[0].headers == {"a:b": "x\\y\nz\r:"}


def test_decoder_does_not_unescape_connected():
    d = Decoder()
    frames = d.feed(b"CONNECTED\nversion:1.2\nserver:a\\cb\n\n\x00")
    assert frames[0].headers["server"] == "a\\cb"


def test_escaped_backslash_followed_by_n_round_trips():
    # "\\n" as data must not decode to a newline.
    d = Decoder()
    frames = d.feed(encode("MESSAGE", {"k": "\\n"}))
    assert frames[0].headers["k"] == "\\n"


def test_unknown_escape_sequences_pass_through():
    d = Decoder()
    frames = d.feed(b"MESSAGE\nk:a\\qb\n\n\x00")
    assert frames[0].headers["k"] == "a\\qb"


def test_header_line_without_a_colon_is_ignored():
    d = Decoder()
    frames = d.feed(b"MESSAGE\ngarbage\nid:1\n\n\x00")
    assert frames[0].headers == {"id": "1"}


def test_feed_accepts_str():
    d = Decoder()
    frames = d.feed("MESSAGE\nid:1\n\nx\x00")
    assert frames[0].body == b"x"


def test_bad_content_length_is_not_fatal_but_yields_nothing():
    d = Decoder()
    assert d.feed(b"MESSAGE\ncontent-length:nope\n\nx\x00") == []


def test_round_trip_of_every_command_we_send():
    d = Decoder()
    for command, headers, body in [
        ("CONNECT", {"host": "jobs", "passcode": "tok:en"}, b""),
        ("SUBSCRIBE", {"id": "sub-1", "destination": "/amq/queue/q"}, b""),
        ("SEND", {"destination": "/exchange/e/rk"}, b'{"a": 1}'),
        ("ACK", {"id": "1"}, b""),
        ("NACK", {"id": "1", "requeue": "false"}, b""),
        ("DISCONNECT", {}, b""),
    ]:
        (frame,) = d.feed(encode(command, headers, body))
        assert frame.command == command
        assert frame.body == body
        for k, v in headers.items():
            assert frame.headers[k] == v


def test_frame_text_replaces_invalid_utf8():
    assert Frame("MESSAGE", {}, b"\xff").text == "�"
