#!/usr/bin/env python3
"""CSI201 Networked Quiz Tournament - command-line client.

Launch (inside the examiner Docker image)::

    python3 client.py --server <server_IP> --port <port> --id <user_id> --name <display_name>

The client is a plain terminal program: it reads commands from standard input,
prints results to standard output and diagnostics to standard error.  Server
events (room updates, questions, results, leaderboard) are printed
asynchronously by a reader thread while the command prompt stays responsive.
"""

from __future__ import annotations

import argparse
import socket
import sys
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent))

import protocol as proto
from protocol import MessageReader, ProtocolError

HELP_TEXT = """\
Commands:
  help                show this help
  quizzes             list available quizzes
  rooms               list joinable rooms
  create <quiz_id>    create a room for a quiz (you become host)
  join <room_id>      join an existing lobby
  info                show the current room and participants
  ready [on|off]      indicate readiness (default: on)
  start               host only: start the quiz when everyone is ready
  answer <A|B|C|D>    answer the open question
  leave               leave the current room
  status              show your identity and room
  quit                disconnect and exit
"""


class QuizClient:
    def __init__(self, host: str, port: int, user_id: str, display_name: str, timeout: float = 10.0) -> None:
        self.host = host
        self.port = port
        self.user_id = user_id
        self.display_name = display_name
        self.timeout = timeout

        self.sock: Optional[socket.socket] = None
        self._reader_thread: Optional[threading.Thread] = None
        self._send_lock = threading.Lock()
        self._pending_lock = threading.Lock()
        self._pending: Dict[int, Dict[str, Any]] = {}
        self._next_id = 1
        self._stop = threading.Event()

        self.room_id: Optional[str] = None
        self.current_question_id: Optional[str] = None

    # ------------------------------------------------------------------
    # connection
    # ------------------------------------------------------------------
    def connect(self) -> None:
        self.sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        self.sock.settimeout(None)
        self._reader_thread = threading.Thread(target=self._read_loop, name="reader", daemon=True)
        self._reader_thread.start()

    def close(self) -> None:
        self._stop.set()
        if self.sock is not None:
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                self.sock.close()
            except OSError:
                pass

    # ------------------------------------------------------------------
    # outbound / request-response
    # ------------------------------------------------------------------
    def _send(self, frame: bytes) -> None:
        if self.sock is None:
            raise ConnectionError("not connected")
        with self._send_lock:
            self.sock.sendall(frame)

    def request(self, type_: str, data: Optional[Dict[str, Any]] = None) -> Tuple[bool, Dict[str, Any]]:
        """Send a request and block until the matching response or error arrives."""
        with self._pending_lock:
            req_id = self._next_id
            self._next_id += 1
            slot: Dict[str, Any] = {"event": threading.Event(), "ok": False, "data": {}, "code": None, "message": None}
            self._pending[req_id] = slot
        try:
            self._send(proto.encode_message(proto.KIND_REQUEST, type_, data, req_id))
        except OSError as exc:
            with self._pending_lock:
                self._pending.pop(req_id, None)
            return False, {"code": "connection_error", "message": str(exc)}

        if not slot["event"].wait(self.timeout):
            with self._pending_lock:
                self._pending.pop(req_id, None)
            return False, {"code": "timeout", "message": f"no reply to {type_} within {self.timeout:g}s"}
        if slot["ok"]:
            return True, slot["data"]
        return False, {"code": slot["code"], "message": slot["message"]}

    # ------------------------------------------------------------------
    # inbound
    # ------------------------------------------------------------------
    def _read_loop(self) -> None:
        assert self.sock is not None
        reader = MessageReader()
        try:
            while not self._stop.is_set():
                chunk = self.sock.recv(4096)
                if not chunk:
                    break
                for message in reader.feed(chunk):
                    self._handle_message(message)
        except (OSError, ProtocolError):
            pass
        finally:
            if not self._stop.is_set():
                print("\n[disconnected by server]", file=sys.stderr)
                self._stop.set()
                # Release any waiter so the command loop can exit.
                with self._pending_lock:
                    for slot in self._pending.values():
                        slot["code"] = "disconnected"
                        slot["message"] = "connection closed"
                        slot["event"].set()

    def _handle_message(self, message: Dict[str, Any]) -> None:
        kind = message.get("kind")
        if kind in (proto.KIND_RESPONSE, proto.KIND_ERROR):
            req_id = message.get("id")
            if isinstance(req_id, int):
                with self._pending_lock:
                    slot = self._pending.pop(req_id, None)
                if slot is not None:
                    if kind == proto.KIND_RESPONSE:
                        slot["ok"] = True
                        slot["data"] = message.get("data", {})
                    else:
                        data = message.get("data", {})
                        slot["code"] = data.get("code")
                        slot["message"] = data.get("message")
                    slot["event"].set()
                    return
            # Fatal or unsolicited error.
            data = message.get("data", {})
            print(f"\n[error] {data.get('code')}: {data.get('message')}", file=sys.stderr)
            return
        if kind == proto.KIND_EVENT:
            self._handle_event(message.get("type"), message.get("data", {}))
            return
        print(f"\n[unexpected message] {message}", file=sys.stderr)

    def _handle_event(self, type_: str, data: Dict[str, Any]) -> None:
        if type_ == proto.EV_ROOM_STATE:
            self.room_id = data.get("room_id")
            print(f"\n[room {data.get('room_id')} | {data.get('title')} | {data.get('state')}]")
            self._print_participants(data)
        elif type_ == proto.EV_QUESTION_STARTED:
            self.current_question_id = data.get("question_id")
            index = data.get("index", 0) + 1
            total = data.get("total", "?")
            print(f"\n[question {index}/{total}] ({data.get('points')} pts, {data.get('time_limit')}s)")
            print(f"  {data.get('text')}")
            for key in ("A", "B", "C", "D"):
                options = data.get("options", {})
                if key in options:
                    print(f"    {key}. {options[key]}")
            print("  answer with: answer <A|B|C|D>")
        elif type_ == proto.EV_QUESTION_CLOSED:
            self.current_question_id = None
            print(f"\n[question closed] correct answer: {data.get('correct_answer')}")
            for result in data.get("results", []):
                mark = "correct" if result.get("correct") else "wrong"
                print(
                    f"  {result.get('user_id')}: {result.get('choice')} ({mark}, +{result.get('points_awarded')})"
                )
            if data.get("missing"):
                print(f"  no answer: {', '.join(data['missing'])}")
            self._print_scores(data.get("scores", []))
        elif type_ == proto.EV_QUIZ_FINISHED:
            print("\n[quiz finished] leaderboard:")
            for row in data.get("leaderboard", []):
                print(
                    f"  {row.get('rank')}. {row.get('display_name')} ({row.get('user_id')}) "
                    f"- {row.get('score')} points"
                )
        else:
            print(f"\n[event] {type_}: {data}")

    @staticmethod
    def _print_participants(data: Dict[str, Any]) -> None:
        for participant in data.get("participants", []):
            role = "host" if participant.get("is_host") else "player"
            ready = "ready" if participant.get("ready") else "not ready"
            print(
                f"  - {participant.get('display_name')} ({participant.get('user_id')}) "
                f"[{role}, {ready}, {participant.get('score')} pts]"
            )

    @staticmethod
    def _print_scores(scores: List[Dict[str, Any]]) -> None:
        if scores:
            rendered = ", ".join(f"{row['user_id']}={row['score']}" for row in scores)
            print(f"  scores: {rendered}")

    # ------------------------------------------------------------------
    # command loop
    # ------------------------------------------------------------------
    def run(self) -> int:
        interactive = sys.stdin.isatty()
        while not self._stop.is_set():
            try:
                line = input("> " if interactive else "")
            except EOFError:
                break
            except KeyboardInterrupt:
                print()
                break
            except ValueError:
                break
            command = line.strip()
            if not command:
                continue
            if command in ("quit", "exit"):
                break
            try:
                self._execute(command)
            except (OSError, ConnectionError) as exc:
                print(f"[connection error] {exc}", file=sys.stderr)
                break
        self.close()
        return 0

    def _execute(self, command: str) -> None:
        parts = command.split()
        verb = parts[0].lower()
        args = parts[1:]

        if verb == "help":
            print(HELP_TEXT)
        elif verb == "quizzes":
            ok, data = self.request(proto.REQ_LIST_QUIZZES)
            if ok:
                for quiz in data.get("quizzes", []):
                    print(
                        f"  {quiz['quiz_id']}: {quiz['title']} "
                        f"({quiz['question_count']} questions, {quiz['total_points']} points) - {quiz['description']}"
                    )
            else:
                self._report_error(data)
        elif verb == "rooms":
            ok, data = self.request(proto.REQ_LIST_ROOMS)
            if ok:
                rooms = data.get("rooms", [])
                if not rooms:
                    print("  (no joinable rooms)")
                for room in rooms:
                    print(
                        f"  {room['room_id']}: {room['title']} "
                        f"[{room['participants']}/{room['capacity']}] host={room['host_id']}"
                    )
            else:
                self._report_error(data)
        elif verb == "create" and args:
            ok, data = self.request(proto.REQ_CREATE_ROOM, {"quiz_id": args[0]})
            if ok:
                self.room_id = data.get("room_id")
                print(f"  created room {self.room_id} for quiz {data.get('quiz_id')}")
            else:
                self._report_error(data)
        elif verb == "join" and args:
            ok, data = self.request(proto.REQ_JOIN_ROOM, {"room_id": args[0]})
            if ok:
                self.room_id = data.get("room_id")
                print(f"  joined room {self.room_id}")
            else:
                self._report_error(data)
        elif verb == "info":
            ok, data = self.request(proto.REQ_ROOM_INFO)
            if ok:
                print(f"  room {data.get('room_id')} | {data.get('title')} | {data.get('state')}")
                self._print_participants(data)
            else:
                self._report_error(data)
        elif verb == "ready":
            ready = True
            if args and args[0].lower() in ("off", "false", "0", "no"):
                ready = False
            ok, data = self.request(proto.REQ_SET_READY, {"ready": ready})
            if ok:
                print(f"  ready = {data.get('ready')}")
            else:
                self._report_error(data)
        elif verb == "start":
            ok, data = self.request(proto.REQ_START_QUIZ)
            if ok:
                print(f"  quiz starting ({data.get('room_id')})")
            else:
                self._report_error(data)
        elif verb == "answer" and args:
            if self.current_question_id is None:
                print("  no question is currently open")
            else:
                ok, data = self.request(
                    proto.REQ_ANSWER,
                    {"question_id": self.current_question_id, "choice": args[0].upper()},
                )
                if ok:
                    print(f"  answer {data.get('choice')} accepted")
                else:
                    self._report_error(data)
        elif verb == "leave":
            ok, data = self.request(proto.REQ_LEAVE_ROOM)
            if ok:
                self.room_id = None
                print("  left the room")
            else:
                self._report_error(data)
        elif verb == "status":
            print(f"  user: {self.display_name} ({self.user_id})")
            print(f"  room: {self.room_id or '(none)'}")
            print(f"  open question: {self.current_question_id or '(none)'}")
        else:
            print("  unknown command; type 'help'")

    @staticmethod
    def _report_error(data: Dict[str, Any]) -> None:
        print(f"  error: {data.get('code')}: {data.get('message')}", file=sys.stderr)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CSI201 Networked Quiz Tournament client")
    parser.add_argument("--server", required=True, help="server IP address or hostname")
    parser.add_argument("--port", required=True, type=int, help="server TCP port")
    parser.add_argument("--id", dest="user_id", required=True, help="unique user id")
    parser.add_argument("--name", dest="display_name", required=True, help="display name")
    parser.add_argument("--timeout", type=float, default=10.0, help="per-request timeout in seconds")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    client = QuizClient(args.server, args.port, args.user_id, args.display_name, timeout=args.timeout)
    try:
        client.connect()
    except OSError as exc:
        print(f"ERROR: cannot connect to {args.server}:{args.port}: {exc}", file=sys.stderr)
        return 2

    ok, data = client.request(proto.REQ_LOGIN, {"user_id": args.user_id, "display_name": args.display_name})
    if not ok:
        print(f"ERROR: login failed: {data.get('code')}: {data.get('message')}", file=sys.stderr)
        client.close()
        return 2
    print(f"Logged in as {args.display_name} ({args.user_id}) on {args.server}:{args.port}")
    print("Type 'help' for commands.")
    return client.run()


if __name__ == "__main__":
    raise SystemExit(main())
