"""Application-layer protocol for the CSI201 Networked Quiz Tournament.

Framing
-------
Every application message is a UTF-8 encoded JSON object prefixed by a
four-byte unsigned big-endian length header (``struct`` format ``"!I"``).

The receiver must treat the TCP connection as a byte stream: one ``recv`` call
may return part of a message, exactly one message, or several messages at once.
:class:`MessageReader` keeps an internal buffer and therefore handles both TCP
fragmentation and coalescing.  Nothing in the design assumes that one ``recv``
equals one application message.

Envelope
--------
Every message shares the same envelope::

    {"v": 1, "kind": <kind>, "type": <name>, "id": <int|null>, "data": {...}}

``kind`` distinguishes the three required classes of traffic:

``request``
    Client -> server.  ``id`` is a client-chosen request identifier.
``response``
    Server -> client.  ``id`` echoes the request that produced it.
``event``
    Server -> client, asynchronous and not tied to a single request.
``error``
    Server -> client.  ``id`` is set when the error answers a request and
    ``null`` when the error is fatal (the server then closes the connection).
"""

from __future__ import annotations

import json
import struct
from typing import Any, Dict, List, Optional

#: Version tag carried in every envelope.
PROTOCOL_VERSION = 1

#: Four-byte unsigned big-endian length header (network byte order).
HEADER = struct.Struct("!I")
HEADER_SIZE = HEADER.size

#: Hard limit for a single application message.
MAX_MESSAGE_BYTES = 1024 * 1024

# ---- envelope kinds ----------------------------------------------------
KIND_REQUEST = "request"
KIND_RESPONSE = "response"
KIND_EVENT = "event"
KIND_ERROR = "error"

# ---- request types (client -> server) ---------------------------------
REQ_LOGIN = "login"
REQ_LIST_QUIZZES = "list_quizzes"
REQ_CREATE_ROOM = "create_room"
REQ_LIST_ROOMS = "list_rooms"
REQ_JOIN_ROOM = "join_room"
REQ_LEAVE_ROOM = "leave_room"
REQ_ROOM_INFO = "room_info"
REQ_SET_READY = "set_ready"
REQ_START_QUIZ = "start_quiz"
REQ_ANSWER = "answer"
REQ_PING = "ping"

# ---- asynchronous event types (server -> client) ----------------------
EV_ROOM_STATE = "room_state"
EV_QUESTION_STARTED = "question_started"
EV_QUESTION_CLOSED = "question_closed"
EV_QUIZ_FINISHED = "quiz_finished"

# ---- error codes ------------------------------------------------------
ERR_BAD_REQUEST = "bad_request"
ERR_NOT_LOGGED_IN = "not_logged_in"
ERR_ALREADY_LOGGED_IN = "already_logged_in"
ERR_DUPLICATE_ID = "duplicate_id"
ERR_UNKNOWN_QUIZ = "unknown_quiz"
ERR_UNKNOWN_ROOM = "unknown_room"
ERR_ROOM_FULL = "room_full"
ERR_NOT_IN_ROOM = "not_in_room"
ERR_ALREADY_IN_ROOM = "already_in_room"
ERR_NOT_HOST = "not_host"
ERR_NOT_READY = "not_ready"
ERR_QUIZ_IN_PROGRESS = "quiz_in_progress"
ERR_ROOM_NOT_JOINABLE = "room_not_joinable"
ERR_INVALID_CHOICE = "invalid_choice"
ERR_ALREADY_ANSWERED = "already_answered"
ERR_LATE_ANSWER = "late_answer"
ERR_NOT_PLAYING = "not_playing"
ERR_PROTOCOL_ERROR = "protocol_error"
ERR_INTERNAL_ERROR = "internal_error"


class ProtocolError(Exception):
    """Raised when a byte stream violates the framing contract."""


def encode_message(
    kind: str,
    type_: str,
    data: Optional[Dict[str, Any]] = None,
    id: Optional[int] = None,
) -> bytes:
    """Serialise one envelope into a length-prefixed frame."""
    envelope = {
        "v": PROTOCOL_VERSION,
        "kind": kind,
        "type": type_,
        "id": id,
        "data": data or {},
    }
    payload = json.dumps(envelope, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(payload) > MAX_MESSAGE_BYTES:
        raise ProtocolError("message too large to encode")
    return HEADER.pack(len(payload)) + payload


def make_request(type_: str, data: Optional[Dict[str, Any]] = None, id: Optional[int] = None) -> Dict[str, Any]:
    """Build a request envelope as a plain dict (used by tests and clients)."""
    return {
        "v": PROTOCOL_VERSION,
        "kind": KIND_REQUEST,
        "type": type_,
        "id": id,
        "data": data or {},
    }


def is_request(message: Dict[str, Any]) -> bool:
    return isinstance(message, dict) and message.get("kind") == KIND_REQUEST


class MessageReader:
    """Incremental de-framer that copes with TCP fragmentation and coalescing."""

    def __init__(self) -> None:
        self._buffer = bytearray()

    def feed(self, chunk: bytes) -> List[Dict[str, Any]]:
        """Add received bytes and return every complete message now available."""
        if chunk:
            self._buffer.extend(chunk)
        messages: List[Dict[str, Any]] = []
        while True:
            if len(self._buffer) < HEADER_SIZE:
                break
            (length,) = HEADER.unpack_from(self._buffer, 0)
            if length == 0 or length > MAX_MESSAGE_BYTES:
                raise ProtocolError(f"invalid frame length {length}")
            if len(self._buffer) < HEADER_SIZE + length:
                break
            start = HEADER_SIZE
            end = start + length
            payload = bytes(self._buffer[start:end])
            del self._buffer[:end]
            try:
                message = json.loads(payload.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ProtocolError(f"frame is not valid UTF-8 JSON: {exc}") from exc
            if not isinstance(message, dict):
                raise ProtocolError("message must be a JSON object")
            messages.append(message)
        return messages
