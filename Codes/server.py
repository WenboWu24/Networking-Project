#!/usr/bin/env python3
"""CSI201 Networked Quiz Tournament - server.

Launch (inside the examiner Docker image)::

    python3 server.py --host <server_IP> --port <port> --questions <json_path>

Start-up contract:
    * the complete question bank is validated first;
    * on failure a diagnostic is printed to stderr and the process exits
      non-zero without listening;
    * on success the server binds, listens and prints ``SERVER_READY``.

Concurrency model:
    * one accept loop in the main thread;
    * one reader thread per connected client;
    * one game thread per room that is playing a quiz;
    * a shared :class:`ServerState` plus a per-room lock protect mutable state.

Only the server reads ``questions.json``.  Correct answers never leave the
server before the corresponding question closes.
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import socket
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

# Allow ``python3 server.py`` from any working directory.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import protocol as proto
from protocol import MessageReader, ProtocolError
from questionbank import (
    Question,
    QuestionBank,
    QuestionBankError,
    load_question_bank,
    question_view,
    quiz_summary,
)

LOG = logging.getLogger("quiz.server")

ROOM_LOBBY = "lobby"
ROOM_PLAYING = "playing"
ROOM_FINISHED = "finished"

#: Pause between two questions of the same quiz.
INTERMISSION_SECONDS = 1.0

#: Upper bound on participants in one room.
MAX_PARTICIPANTS = 20

#: Maximum accepted display-name length.
MAX_NAME_LENGTH = 40

# Bound socket operations so an idle or slow client cannot block shutdown.
SOCKET_TIMEOUT = 0.5


class ClientSession:
    """One connected client: socket, identity and room membership."""

    def __init__(self, conn: socket.socket, addr, server: "QuizServer") -> None:
        self.conn = conn
        self.addr = addr
        self.server = server
        self.user_id: Optional[str] = None
        self.display_name: Optional[str] = None
        self.room_id: Optional[str] = None
        self._send_lock = threading.RLock()
        self._closed = False

    # -- outbound ------------------------------------------------------
    def send(self, kind: str, type_: str, data: Optional[Dict[str, Any]] = None, id: Optional[int] = None) -> None:
        try:
            frame = proto.encode_message(kind, type_, data, id)
        except ProtocolError:
            self.close()
            return
        with self._send_lock:
            if self._closed:
                return
            try:
                self.conn.sendall(frame)
            except OSError:
                self.close()

    def send_response(self, id: Optional[int], type_: str, data: Optional[Dict[str, Any]] = None) -> None:
        LOG.debug("-> response %s id=%s", type_, id)
        self.send(proto.KIND_RESPONSE, type_, data or {}, id)

    def send_event(self, type_: str, data: Optional[Dict[str, Any]] = None) -> None:
        self.send(proto.KIND_EVENT, type_, data or {})

    def send_error(self, id: Optional[int], code: str, message: str) -> None:
        LOG.debug("-> error %s id=%s: %s", code, id, message)
        self.send(proto.KIND_ERROR, "error", {"code": code, "message": message}, id)

    # -- lifecycle -----------------------------------------------------
    def close(self) -> None:
        with self._send_lock:
            if self._closed:
                return
            self._closed = True
        try:
            self.conn.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.conn.close()
        except OSError:
            pass


class Room:
    """A quiz room with its own participants, readiness and score state."""

    def __init__(self, room_id: str, quiz, host_id: str) -> None:
        self.room_id = room_id
        self.quiz = quiz
        self.host_id = host_id
        self.state = ROOM_LOBBY
        self.members: Dict[str, ClientSession] = {}
        self.ready: set = set()
        self.scores: Dict[str, int] = {}
        self.correct: Dict[str, int] = {}
        self.answers: Dict[str, Dict[str, str]] = {}
        self.current_question: Optional[Question] = None
        self.question_deadline: Optional[float] = None
        self.current_index = -1
        self.current_players: List[str] = []
        self.cond = threading.Condition(threading.RLock())
        self.stop_event = threading.Event()
        self.game_thread: Optional[threading.Thread] = None

    # -- membership (call with/without lock; internal locking used) ----
    def add_member(self, session: ClientSession) -> None:
        with self.cond:
            self.members[session.user_id] = session
            self.scores.setdefault(session.user_id, 0)
            self.correct.setdefault(session.user_id, 0)

    def remove_member(self, user_id: str) -> bool:
        """Remove a member.  Returns True when the room is now empty."""
        with self.cond:
            self.members.pop(user_id, None)
            self.ready.discard(user_id)
            self.scores.pop(user_id, None)
            self.correct.pop(user_id, None)
            for answers in self.answers.values():
                answers.pop(user_id, None)
            if self.host_id == user_id and self.members:
                # Deterministic host handover: lowest user_id still present.
                self.host_id = sorted(self.members)[0]
            self.cond.notify_all()
            return not self.members

    def set_ready(self, user_id: str, ready: bool) -> None:
        with self.cond:
            if ready:
                self.ready.add(user_id)
            else:
                self.ready.discard(user_id)
            self.cond.notify_all()

    def all_ready(self) -> bool:
        with self.cond:
            return bool(self.members) and all(user_id in self.ready for user_id in self.members)

    # -- views ---------------------------------------------------------
    def snapshot(self) -> Dict[str, Any]:
        with self.cond:
            participants = [
                {
                    "user_id": user_id,
                    "display_name": session.display_name,
                    "ready": user_id in self.ready,
                    "is_host": user_id == self.host_id,
                    "score": self.scores.get(user_id, 0),
                    "correct": self.correct.get(user_id, 0),
                }
                for user_id, session in sorted(self.members.items())
            ]
            return {
                "room_id": self.room_id,
                "quiz_id": self.quiz.quiz_id,
                "title": self.quiz.title,
                "host_id": self.host_id,
                "state": self.state,
                "participants": participants,
                "participant_count": len(participants),
                "ready_count": sum(1 for p in participants if p["ready"]),
                "question_index": self.current_index if self.state == ROOM_PLAYING else None,
                "question_total": len(self.quiz.questions),
            }

    def _scores_sorted(self) -> List[Dict[str, Any]]:
        with self.cond:
            rows = [
                {
                    "user_id": user_id,
                    "display_name": session.display_name,
                    "score": self.scores.get(user_id, 0),
                    "correct": self.correct.get(user_id, 0),
                }
                for user_id, session in self.members.items()
            ]
        # Score desc, then correct desc, then user_id asc for determinism.
        rows.sort(key=lambda row: (-row["score"], -row["correct"], row["user_id"]))
        return rows

    def _leaderboard(self) -> List[Dict[str, Any]]:
        rows = self._scores_sorted()
        for position, row in enumerate(rows, start=1):
            row["rank"] = position
            row["total_questions"] = len(self.quiz.questions)
        return rows

    # -- broadcasting --------------------------------------------------
    def _member_list(self) -> List[ClientSession]:
        with self.cond:
            return list(self.members.values())

    def broadcast_event(self, type_: str, data: Dict[str, Any]) -> None:
        for session in self._member_list():
            session.send_event(type_, data)

    # -- gameplay ------------------------------------------------------
    def submit_answer(self, user_id: str, question_id: str, choice: str):
        """Return ``(status, detail)``; status is ``accepted`` or an error code."""
        with self.cond:
            if self.state != ROOM_PLAYING or self.stop_event.is_set():
                return proto.ERR_NOT_PLAYING, "no question is open"
            if user_id not in self.members or user_id not in self.current_players:
                return proto.ERR_NOT_IN_ROOM, "you are not an active player for this question"
            if (self.current_question is None
                    or self.current_question.question_id != question_id
                    or self.question_deadline is None
                    or time.monotonic() >= self.question_deadline):
                return proto.ERR_LATE_ANSWER, "that question is not open"
            if choice not in self.current_question.options:
                return proto.ERR_INVALID_CHOICE, "choice must be one of A, B, C or D"
            answers = self.answers.setdefault(question_id, {})
            if user_id in answers:
                return proto.ERR_ALREADY_ANSWERED, "you have already answered this question"
            answers[user_id] = choice
            self.cond.notify_all()
            return "accepted", None

    def _active_current_players(self) -> List[str]:
        return [user_id for user_id in self.current_players if user_id in self.members]

    def _all_players_answered(self, question_id: str) -> bool:
        active = self._active_current_players()
        answered = self.answers.get(question_id, {})
        return bool(active) and all(user_id in answered for user_id in active)

    def _close_question(self, question: Question) -> Dict[str, Any]:
        with self.cond:
            self.current_question = None
            self.question_deadline = None
            answers = dict(self.answers.get(question.question_id, {}))
            results = []
            for user_id, choice in sorted(answers.items()):
                is_correct = choice == question.answer
                awarded = question.points if is_correct else 0
                if is_correct:
                    self.scores[user_id] = self.scores.get(user_id, 0) + awarded
                    self.correct[user_id] = self.correct.get(user_id, 0) + 1
                results.append(
                    {
                        "user_id": user_id,
                        "choice": choice,
                        "correct": is_correct,
                        "points_awarded": awarded,
                    }
                )
            missing = [user_id for user_id in self._active_current_players() if user_id not in answers]
            return {
                "question_id": question.question_id,
                "correct_answer": question.answer,
                "points": question.points,
                "results": results,
                "missing": missing,
                "scores": self._scores_sorted(),
            }

    def start(self) -> None:
        with self.cond:
            if self.state != ROOM_LOBBY:
                return
            self.state = ROOM_PLAYING
            self.current_index = -1
        self.stop_event.clear()
        self.game_thread = threading.Thread(target=self._run_game, name=f"room-{self.room_id}", daemon=True)
        self.game_thread.start()

    def _run_game(self) -> None:
        question_total = len(self.quiz.questions)
        for index, question in enumerate(self.quiz.questions):
            if self.stop_event.is_set():
                return
            with self.cond:
                self.current_question = question
                self.question_deadline = time.monotonic() + question.time_limit
                deadline = self.question_deadline
                self.current_index = index
                self.current_players = list(self.members.keys())
                self.answers[question.question_id] = {}
                payload = question_view(question, index, question_total)
            payload["room_id"] = self.room_id
            self.broadcast_event(proto.EV_QUESTION_STARTED, payload)

            with self.cond:
                while not self.stop_event.is_set():
                    if self._all_players_answered(question.question_id):
                        break
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    # Wake early when an answer arrives so a question can close
                    # as soon as every active player has answered.
                    self.cond.wait(min(remaining, 0.2))

                if self.stop_event.is_set():
                    self.current_question = None
                    self.question_deadline = None
                    return

            closed = self._close_question(question)
            closed["room_id"] = self.room_id
            self.broadcast_event(proto.EV_QUESTION_CLOSED, closed)

            if index < question_total - 1:
                self.stop_event.wait(INTERMISSION_SECONDS)

        with self.cond:
            self.state = ROOM_FINISHED
            self.current_question = None
            self.question_deadline = None
        self.broadcast_event(
            proto.EV_QUIZ_FINISHED,
            {
                "room_id": self.room_id,
                "quiz_id": self.quiz.quiz_id,
                "title": self.quiz.title,
                "leaderboard": self._leaderboard(),
            },
        )
        LOG.info("room %s finished quiz %s", self.room_id, self.quiz.quiz_id)

    def stop(self) -> None:
        self.stop_event.set()
        with self.cond:
            self.current_question = None
            self.question_deadline = None
            self.cond.notify_all()
        thread = self.game_thread
        if thread is not None and thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=2.0)


class QuizServer:
    """Accepts connections and dispatches requests to room logic."""

    def __init__(self, host: str, port: int, bank: QuestionBank) -> None:
        self.host = host
        self.port = port
        self.bank = bank
        self._listen_socket: Optional[socket.socket] = None
        self._shutdown = threading.Event()
        self._state_lock = threading.RLock()
        self._sessions: Dict[str, ClientSession] = {}
        self._connections: set[ClientSession] = set()
        self._rooms: Dict[str, Room] = {}
        self._connection_threads: List[threading.Thread] = []
        self._handlers: Dict[str, Callable[[ClientSession, Optional[int], Dict[str, Any]], bool]] = {
            proto.REQ_LOGIN: self._req_login,
            proto.REQ_LIST_QUIZZES: self._req_list_quizzes,
            proto.REQ_CREATE_ROOM: self._req_create_room,
            proto.REQ_LIST_ROOMS: self._req_list_rooms,
            proto.REQ_JOIN_ROOM: self._req_join_room,
            proto.REQ_LEAVE_ROOM: self._req_leave_room,
            proto.REQ_ROOM_INFO: self._req_room_info,
            proto.REQ_SET_READY: self._req_set_ready,
            proto.REQ_START_QUIZ: self._req_start_quiz,
            proto.REQ_ANSWER: self._req_answer,
            proto.REQ_PING: self._req_ping,
        }

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------
    def serve_forever(self) -> None:
        listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            listen_socket.bind((self.host, self.port))
            listen_socket.listen(128)
        except OSError:
            listen_socket.close()
            raise
        self._listen_socket = listen_socket
        listen_socket.settimeout(SOCKET_TIMEOUT)

        print("SERVER_READY", flush=True)
        LOG.info("listening on %s:%s", self.host, self.port)

        try:
            while not self._shutdown.is_set():
                try:
                    conn, addr = listen_socket.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break
                conn.settimeout(SOCKET_TIMEOUT)
                session = ClientSession(conn, addr, self)
                with self._state_lock:
                    self._connections.add(session)
                thread = threading.Thread(
                    target=self._handle_connection,
                    args=(session,),
                    name=f"client-{addr[0]}:{addr[1]}",
                    daemon=True,
                )
                # Drop threads that have already finished so a long-running
                # server does not accumulate them.
                self._connection_threads = [t for t in self._connection_threads if t.is_alive()]
                self._connection_threads.append(thread)
                thread.start()
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        if self._shutdown.is_set():
            return
        self._shutdown.set()
        if self._listen_socket is not None:
            try:
                self._listen_socket.close()
            except OSError:
                pass
        with self._state_lock:
            rooms = list(self._rooms.values())
            sessions = list(self._connections)
        for session in sessions:
            session.close()
        for room in rooms:
            room.stop()
        for thread in self._connection_threads:
            if thread.is_alive():
                thread.join(timeout=1.0)
        LOG.info("server stopped")

    # ------------------------------------------------------------------
    # connection handling
    # ------------------------------------------------------------------
    def _handle_connection(self, session: ClientSession) -> None:
        conn, addr = session.conn, session.addr
        reader = MessageReader()
        LOG.info("connection from %s:%s", addr[0], addr[1])
        try:
            while not self._shutdown.is_set():
                try:
                    chunk = conn.recv(4096)
                except socket.timeout:
                    continue
                except OSError:
                    break
                if not chunk:
                    break
                try:
                    messages = reader.feed(chunk)
                except ProtocolError as exc:
                    # Malformed framing ends this connection only.
                    LOG.warning("protocol error from %s: %s", addr[0], exc)
                    session.send_error(None, proto.ERR_PROTOCOL_ERROR, str(exc))
                    break
                for message in messages:
                    if not self._dispatch(session, message):
                        return
        except Exception:  # pragma: no cover - defensive
            LOG.exception("unexpected error on connection from %s", addr[0])
        finally:
            self._cleanup_session(session)

    def _dispatch(self, session: ClientSession, message: Dict[str, Any]) -> bool:
        """Route one message.  Returns False when the connection must close."""
        if not isinstance(message, dict):
            session.send_error(None, proto.ERR_PROTOCOL_ERROR, "message must be a JSON object")
            return False

        req_id = message.get("id")
        kind = message.get("kind")
        type_ = message.get("type")

        if type(message.get("v")) is not int or message["v"] != proto.PROTOCOL_VERSION:
            session.send_error(req_id if type(req_id) is int else None, proto.ERR_BAD_REQUEST, "unsupported protocol version")
            return True

        if kind != proto.KIND_REQUEST:
            session.send_error(req_id if isinstance(req_id, int) else None, proto.ERR_BAD_REQUEST, "expected a request message")
            return True
        if not isinstance(type_, str):
            session.send_error(req_id if isinstance(req_id, int) else None, proto.ERR_BAD_REQUEST, "request type must be a string")
            return True
        if req_id is not None and type(req_id) is not int:
            session.send_error(None, proto.ERR_BAD_REQUEST, "request id must be an integer or null")
            return True

        data = message.get("data", {})
        if data is None:
            data = {}
        if not isinstance(data, dict):
            session.send_error(req_id, proto.ERR_BAD_REQUEST, "request data must be an object")
            return True

        handler = self._handlers.get(type_)
        if handler is None:
            session.send_error(req_id, proto.ERR_BAD_REQUEST, f"unknown request type {type_!r}")
            return True
        try:
            return handler(session, req_id, data)
        except Exception:  # pragma: no cover - defensive
            LOG.exception("unhandled error while handling %s", type_)
            session.send_error(req_id, proto.ERR_INTERNAL_ERROR, "internal server error")
            return True

    def _cleanup_session(self, session: ClientSession) -> None:
        session.close()
        user_id = session.user_id
        if user_id is not None:
            room_id = session.room_id
            if room_id is not None:
                self._leave_room(session)
        with self._state_lock:
            if user_id is not None and self._sessions.get(user_id) is session:
                self._sessions.pop(user_id)
            self._connections.discard(session)
        LOG.info("connection from %s closed", session.addr[0])

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    def _require_login(self, session: ClientSession, req_id: Optional[int]) -> bool:
        if session.user_id is None:
            session.send_error(req_id, proto.ERR_NOT_LOGGED_IN, "send a login request first")
            return False
        return True

    def _broadcast_room_state(self, room: Room) -> None:
        room.broadcast_event(proto.EV_ROOM_STATE, room.snapshot())

    def _leave_room(self, session: ClientSession) -> Optional[Room]:
        room_id = session.room_id
        if room_id is None:
            return None
        with self._state_lock:
            room = self._rooms.get(room_id)
            if room is None:
                session.room_id = None
                return None
            with room.cond:
                session.room_id = None
                empty = room.remove_member(session.user_id)
                if empty:
                    self._rooms.pop(room_id, None)
        if empty:
            room.stop()
            LOG.info("room %s removed (empty)", room_id)
        else:
            self._broadcast_room_state(room)
        return room

    # ------------------------------------------------------------------
    # request handlers
    # ------------------------------------------------------------------
    def _req_login(self, session: ClientSession, req_id: Optional[int], data: Dict[str, Any]) -> bool:
        if session.user_id is not None:
            session.send_error(req_id, proto.ERR_ALREADY_LOGGED_IN, "this connection is already logged in")
            return True

        user_id = data.get("user_id")
        display_name = data.get("display_name")
        if not isinstance(user_id, str) or not (1 <= len(user_id) <= 32) or not user_id.isprintable() or not user_id.strip():
            session.send_error(req_id, proto.ERR_BAD_REQUEST, "user_id must be 1-32 printable characters and not blank")
            return False
        if not isinstance(display_name, str) or not (1 <= len(display_name) <= MAX_NAME_LENGTH) or not display_name.isprintable() or not display_name.strip():
            session.send_error(req_id, proto.ERR_BAD_REQUEST, f"display_name must be 1-{MAX_NAME_LENGTH} printable characters and not blank")
            return False

        with self._state_lock:
            error = None
            if user_id in self._sessions:
                # Deterministic duplicate handling: reject the newcomer.
                error = (proto.ERR_DUPLICATE_ID, f"user id {user_id!r} is already active")
            elif any(s.display_name == display_name for s in self._sessions.values()):
                error = (proto.ERR_BAD_REQUEST, f"display name {display_name!r} is already active")
            else:
                session.user_id = user_id
                session.display_name = display_name
                self._sessions[user_id] = session
        if error is not None:
            session.send_error(req_id, *error)
            return False

        LOG.info("login: %s (%s)", user_id, display_name)
        session.send_response(req_id, proto.REQ_LOGIN, {"user_id": user_id, "display_name": display_name})
        return True

    def _req_ping(self, session: ClientSession, req_id: Optional[int], data: Dict[str, Any]) -> bool:
        session.send_response(req_id, proto.REQ_PING, {"time": time.time()})
        return True

    def _req_list_quizzes(self, session: ClientSession, req_id: Optional[int], data: Dict[str, Any]) -> bool:
        if not self._require_login(session, req_id):
            return True
        quizzes = [quiz_summary(quiz) for quiz in self.bank.quizzes]
        session.send_response(req_id, proto.REQ_LIST_QUIZZES, {"quizzes": quizzes})
        return True

    def _req_create_room(self, session: ClientSession, req_id: Optional[int], data: Dict[str, Any]) -> bool:
        if not self._require_login(session, req_id):
            return True
        if session.room_id is not None:
            session.send_error(req_id, proto.ERR_ALREADY_IN_ROOM, "leave your current room first")
            return True

        quiz_id = data.get("quiz_id")
        quiz = self.bank.find_quiz(quiz_id) if isinstance(quiz_id, str) else None
        if quiz is None:
            session.send_error(req_id, proto.ERR_UNKNOWN_QUIZ, f"unknown quiz_id {quiz_id!r}")
            return True

        with self._state_lock:
            room_id = f"R-{uuid.uuid4().hex[:6]}"
            while room_id in self._rooms:
                room_id = f"R-{uuid.uuid4().hex[:6]}"
            room = Room(room_id, quiz, host_id=session.user_id)
            session.room_id = room_id
            room.add_member(session)
            self._rooms[room_id] = room
        LOG.info("room %s created by %s for quiz %s", room_id, session.user_id, quiz.quiz_id)
        session.send_response(req_id, proto.REQ_CREATE_ROOM, room.snapshot())
        self._broadcast_room_state(room)
        return True

    def _req_list_rooms(self, session: ClientSession, req_id: Optional[int], data: Dict[str, Any]) -> bool:
        if not self._require_login(session, req_id):
            return True
        with self._state_lock:
            rooms = list(self._rooms.values())
        joinable = []
        for room in rooms:
            snapshot = room.snapshot()
            if snapshot["state"] != ROOM_LOBBY or snapshot["participant_count"] >= MAX_PARTICIPANTS:
                continue
            joinable.append(
                {
                    "room_id": snapshot["room_id"],
                    "quiz_id": snapshot["quiz_id"],
                    "title": snapshot["title"],
                    "host_id": snapshot["host_id"],
                    "participants": snapshot["participant_count"],
                    "capacity": MAX_PARTICIPANTS,
                }
            )
        session.send_response(req_id, proto.REQ_LIST_ROOMS, {"rooms": joinable})
        return True

    def _req_join_room(self, session: ClientSession, req_id: Optional[int], data: Dict[str, Any]) -> bool:
        if not self._require_login(session, req_id):
            return True
        if session.room_id is not None:
            session.send_error(req_id, proto.ERR_ALREADY_IN_ROOM, "leave your current room first")
            return True

        room_id = data.get("room_id")
        with self._state_lock:
            room = self._rooms.get(room_id) if isinstance(room_id, str) else None
            error = None
            if room is None:
                error = (proto.ERR_UNKNOWN_ROOM, f"unknown room_id {room_id!r}")
            else:
                with room.cond:
                    if room.state != ROOM_LOBBY:
                        error = (proto.ERR_ROOM_NOT_JOINABLE, "that room has already started")
                    elif len(room.members) >= MAX_PARTICIPANTS:
                        error = (proto.ERR_ROOM_FULL, "that room is full")
                    else:
                        session.room_id = room_id
                        room.add_member(session)
        if error is not None:
            session.send_error(req_id, *error)
            return True
        LOG.info("%s joined room %s", session.user_id, room_id)
        session.send_response(req_id, proto.REQ_JOIN_ROOM, room.snapshot())
        self._broadcast_room_state(room)
        return True

    def _req_leave_room(self, session: ClientSession, req_id: Optional[int], data: Dict[str, Any]) -> bool:
        if not self._require_login(session, req_id):
            return True
        if session.room_id is None:
            session.send_error(req_id, proto.ERR_NOT_IN_ROOM, "you are not in a room")
            return True
        self._leave_room(session)
        session.send_response(req_id, proto.REQ_LEAVE_ROOM, {})
        return True

    def _req_room_info(self, session: ClientSession, req_id: Optional[int], data: Dict[str, Any]) -> bool:
        if not self._require_login(session, req_id):
            return True
        if session.room_id is None:
            session.send_error(req_id, proto.ERR_NOT_IN_ROOM, "you are not in a room")
            return True
        with self._state_lock:
            room = self._rooms.get(session.room_id)
        if room is None:
            session.send_error(req_id, proto.ERR_UNKNOWN_ROOM, "your room no longer exists")
            return True
        session.send_response(req_id, proto.REQ_ROOM_INFO, room.snapshot())
        return True

    def _req_set_ready(self, session: ClientSession, req_id: Optional[int], data: Dict[str, Any]) -> bool:
        if not self._require_login(session, req_id):
            return True
        if session.room_id is None:
            session.send_error(req_id, proto.ERR_NOT_IN_ROOM, "you are not in a room")
            return True
        ready = data.get("ready")
        if not isinstance(ready, bool):
            session.send_error(req_id, proto.ERR_BAD_REQUEST, "ready must be true or false")
            return True
        with self._state_lock:
            room = self._rooms.get(session.room_id)
        if room is None:
            session.send_error(req_id, proto.ERR_UNKNOWN_ROOM, "your room no longer exists")
            return True
        with room.cond:
            in_lobby = room.state == ROOM_LOBBY
            if in_lobby:
                room.set_ready(session.user_id, ready)
        if not in_lobby:
            session.send_error(req_id, proto.ERR_QUIZ_IN_PROGRESS, "readiness can only change in the lobby")
            return True
        session.send_response(req_id, proto.REQ_SET_READY, {"ready": ready})
        self._broadcast_room_state(room)
        return True

    def _req_start_quiz(self, session: ClientSession, req_id: Optional[int], data: Dict[str, Any]) -> bool:
        if not self._require_login(session, req_id):
            return True
        if session.room_id is None:
            session.send_error(req_id, proto.ERR_NOT_IN_ROOM, "you are not in a room")
            return True
        with self._state_lock:
            room = self._rooms.get(session.room_id)
        if room is None:
            session.send_error(req_id, proto.ERR_UNKNOWN_ROOM, "your room no longer exists")
            return True
        with room.cond:
            if room.state != ROOM_LOBBY:
                error = (proto.ERR_QUIZ_IN_PROGRESS, "the quiz has already started")
            elif session.user_id != room.host_id:
                error = (proto.ERR_NOT_HOST, "only the room host can start the quiz")
            elif not room.all_ready():
                error = (proto.ERR_NOT_READY, "every participant must be ready before starting")
            else:
                session.send_response(req_id, proto.REQ_START_QUIZ, {"room_id": room.room_id, "state": ROOM_PLAYING})
                room.start()
                return True
        session.send_error(req_id, *error)
        return True

    def _req_answer(self, session: ClientSession, req_id: Optional[int], data: Dict[str, Any]) -> bool:
        if not self._require_login(session, req_id):
            return True
        if session.room_id is None:
            session.send_error(req_id, proto.ERR_NOT_IN_ROOM, "you are not in a room")
            return True
        question_id = data.get("question_id")
        choice = data.get("choice")
        if not isinstance(question_id, str) or not isinstance(choice, str):
            session.send_error(req_id, proto.ERR_BAD_REQUEST, "answer needs question_id and choice strings")
            return True
        with self._state_lock:
            room = self._rooms.get(session.room_id)
        if room is None:
            session.send_error(req_id, proto.ERR_UNKNOWN_ROOM, "your room no longer exists")
            return True

        status, detail = room.submit_answer(session.user_id, question_id, choice.upper())
        if status != "accepted":
            session.send_error(req_id, status, detail or "answer rejected")
            return True
        session.send_response(req_id, proto.REQ_ANSWER, {"question_id": question_id, "choice": choice.upper()})
        return True


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CSI201 Networked Quiz Tournament server")
    parser.add_argument("--host", required=True, help="interface/address to bind")
    parser.add_argument("--port", required=True, type=int, help="TCP port to bind")
    parser.add_argument("--questions", required=True, help="path to the question bank JSON file")
    parser.add_argument("--log-level", default="INFO", help="logging level (default: INFO)")
    return parser.parse_args(argv)


def _install_signal_handlers() -> None:
    """Install stop handlers for SIGINT (Ctrl+C) and SIGTERM.

    A non-interactive shell starts background jobs with SIGINT set to
    ``SIG_IGN``; installing the handler explicitly guarantees that Ctrl+C
    always releases the listening socket and open connections, whether the
    server was launched in the foreground or from a script.
    """

    def _raise_keyboard_interrupt(signum, frame):  # noqa: ARG001 - signal signature
        raise KeyboardInterrupt

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _raise_keyboard_interrupt)
        except (ValueError, OSError):  # pragma: no cover - non-main thread / unsupported
            pass


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    _install_signal_handlers()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )

    if not 1 <= args.port <= 65535:
        print(f"ERROR: port {args.port} is out of range 1..65535", file=sys.stderr)
        return 2

    try:
        bank = load_question_bank(args.questions)
    except QuestionBankError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    print(
        f"Question bank OK: {len(bank.quizzes)} quiz(zes), {bank.total_questions} questions "
        f"({os.path.basename(bank.source_path)})",
        flush=True,
    )

    server = QuizServer(args.host, args.port, bank)
    try:
        server.serve_forever()
    except OSError as exc:
        print(f"ERROR: cannot listen on {args.host}:{args.port}: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("Shutting down...", file=sys.stderr)
    finally:
        server.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
