"""Smoke tests for the CSI201 Networked Quiz Tournament framework.

Run from the ``Codes`` directory::

    python3 -m unittest discover -s tests -v

The tests are intentionally dependency-free (standard library only) so they can
run inside the examiner Docker image.
"""

from __future__ import annotations

import json
import os
import queue
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

CODES_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(CODES_DIR))

import protocol as proto  # noqa: E402
from protocol import MessageReader  # noqa: E402
from questionbank import QuestionBankError, load_question_bank, validate_question_bank  # noqa: E402

SHIPPED_BANK = CODES_DIR / "questions.json"

# Correct answers for the shipped "network-basics" quiz.
CORRECT_ANSWERS = {"NET001": "B", "NET002": "A", "NET003": "C", "NET004": "B", "NET005": "D"}


# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------
def free_port() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


class ServerProcess:
    """A running server subprocess with a non-blocking line reader."""

    def __init__(self, proc: subprocess.Popen) -> None:
        self.proc = proc
        self.lines = []
        self._queue: "queue.Queue[str]" = queue.Queue()
        self._thread = threading.Thread(target=self._pump, daemon=True)
        self._thread.start()

    def _pump(self) -> None:
        try:
            if self.proc.stdout is None:
                return
            for line in self.proc.stdout:
                stripped = line.rstrip("\n")
                self.lines.append(stripped)
                self._queue.put(stripped)
        except (ValueError, OSError):
            pass

    def wait_for(self, text: str, timeout: float = 15.0) -> bool:
        deadline = time.monotonic() + timeout
        while True:
            if any(text in line for line in self.lines):
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return any(text in line for line in self.lines)
            try:
                self._queue.get(timeout=remaining)
            except queue.Empty:
                return any(text in line for line in self.lines)

    def terminate(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        if self.proc.stdout is not None:
            try:
                self.proc.stdout.close()
            except OSError:
                pass


def start_server(bank_path: Path, port: int) -> ServerProcess:
    proc = subprocess.Popen(
        [
            sys.executable,
            "server.py",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--questions",
            str(bank_path),
        ],
        cwd=str(CODES_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    server = ServerProcess(proc)
    if not server.wait_for("SERVER_READY"):
        server.terminate()
        raise AssertionError(f"server did not become ready; output={server.lines!r}")
    return server


def run_server_once(bank_path: Path, port: int) -> tuple:
    proc = subprocess.Popen(
        [
            sys.executable,
            "server.py",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--questions",
            str(bank_path),
        ],
        cwd=str(CODES_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    out, _ = proc.communicate(timeout=15)
    return proc.returncode, out


class TestClient:
    """Minimal protocol client used to drive the server from tests."""

    def __init__(
        self,
        host: str,
        port: int,
        user_id: str,
        display_name: str | None = None,
        auto_answers: dict | None = None,
    ) -> None:
        self.user_id = user_id
        self.display_name = display_name or user_id.title()
        self.auto_answers = auto_answers or {}
        self.sock = socket.create_connection((host, port), timeout=5)
        self.sock.settimeout(None)
        self._reader = MessageReader()
        self._send_lock = threading.Lock()
        self._pending_lock = threading.Lock()
        self._pending: dict = {}
        self._next_id = 1
        self.events: "queue.Queue[dict]" = queue.Queue()
        self._stop = threading.Event()
        self.room_id = None
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()

    # -- low level -----------------------------------------------------
    def _send(self, frame: bytes) -> None:
        with self._send_lock:
            self.sock.sendall(frame)

    def _loop(self) -> None:
        try:
            while not self._stop.is_set():
                chunk = self.sock.recv(4096)
                if not chunk:
                    break
                for message in self._reader.feed(chunk):
                    self._dispatch(message)
        except Exception:
            pass

    def _dispatch(self, message: dict) -> None:
        kind = message.get("kind")
        if kind == proto.KIND_EVENT:
            self.events.put(message)
            if message.get("type") == proto.EV_QUESTION_STARTED:
                question_id = message["data"]["question_id"]
                choice = self.auto_answers.get(question_id)
                if choice:
                    # Fire-and-forget so the reader thread never blocks.
                    self._fire(proto.REQ_ANSWER, {"question_id": question_id, "choice": choice})
            return
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

    def _fire(self, type_: str, data: dict | None = None) -> None:
        with self._pending_lock:
            req_id = self._next_id
            self._next_id += 1
        self._send(proto.encode_message(proto.KIND_REQUEST, type_, data, req_id))

    # -- API -----------------------------------------------------------
    def request(self, type_: str, data: dict | None = None, timeout: float = 5.0):
        with self._pending_lock:
            req_id = self._next_id
            self._next_id += 1
            slot = {"event": threading.Event(), "ok": False, "data": {}, "code": None, "message": None}
            self._pending[req_id] = slot
        self._send(proto.encode_message(proto.KIND_REQUEST, type_, data, req_id))
        if not slot["event"].wait(timeout):
            raise AssertionError(f"no reply to {type_} within {timeout}s")
        if slot["ok"]:
            return True, slot["data"]
        return False, {"code": slot["code"], "message": slot["message"]}

    def login(self):
        return self.request(proto.REQ_LOGIN, {"user_id": self.user_id, "display_name": self.display_name})

    def wait_event(self, type_: str, timeout: float = 30.0) -> dict:
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AssertionError(f"event {type_} not received")
            try:
                message = self.events.get(timeout=remaining)
            except queue.Empty:
                raise AssertionError(f"event {type_} not received")
            if message.get("type") == type_:
                return message

    def close(self) -> None:
        self._stop.set()
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass


def read_one_frame(sock: socket.socket) -> dict:
    header = b""
    while len(header) < proto.HEADER_SIZE:
        chunk = sock.recv(proto.HEADER_SIZE - len(header))
        if not chunk:
            raise AssertionError("connection closed before a full frame arrived")
        header += chunk
    (length,) = proto.HEADER.unpack(header)
    payload = b""
    while len(payload) < length:
        chunk = sock.recv(length - len(payload))
        if not chunk:
            raise AssertionError("connection closed mid-frame")
        payload += chunk
    return json.loads(payload.decode("utf-8"))


# ----------------------------------------------------------------------
# framing unit tests
# ----------------------------------------------------------------------
class FramingTests(unittest.TestCase):
    def test_fragmented_stream_is_reassembled(self):
        frame = proto.encode_message(proto.KIND_REQUEST, proto.REQ_PING, {}, 1)
        reader = MessageReader()
        messages = []
        for i in range(len(frame)):
            messages.extend(reader.feed(frame[i : i + 1]))
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]["type"], proto.REQ_PING)

    def test_coalesced_stream_is_split(self):
        frame_a = proto.encode_message(proto.KIND_REQUEST, proto.REQ_PING, {}, 1)
        frame_b = proto.encode_message(proto.KIND_REQUEST, proto.REQ_PING, {}, 2)
        messages = MessageReader().feed(frame_a + frame_b)
        self.assertEqual([m["id"] for m in messages], [1, 2])

    def test_invalid_frame_length_is_rejected(self):
        reader = MessageReader()
        with self.assertRaises(proto.ProtocolError):
            reader.feed((0).to_bytes(4, "big"))


# ----------------------------------------------------------------------
# question-bank validation tests
# ----------------------------------------------------------------------
class ValidationTests(unittest.TestCase):
    def test_shipped_bank_is_valid(self):
        bank = load_question_bank(str(SHIPPED_BANK))
        self.assertGreaterEqual(len(bank.quizzes), 2)
        for quiz in bank.quizzes:
            self.assertGreaterEqual(len(quiz.questions), 5)

    def test_schema_version_must_be_one(self):
        with self.assertRaises(QuestionBankError):
            validate_question_bank({"schema_version": 2, "quizzes": []})

    def test_options_must_be_exactly_abcd(self):
        bank = json.loads(SHIPPED_BANK.read_text())
        bank["quizzes"][0]["questions"][0]["options"] = {"A": "x", "B": "y", "C": "z"}
        with self.assertRaises(QuestionBankError):
            validate_question_bank(bank)

    def test_duplicate_question_id_is_rejected(self):
        bank = json.loads(SHIPPED_BANK.read_text())
        bank["quizzes"][1]["questions"][0]["question_id"] = bank["quizzes"][0]["questions"][0]["question_id"]
        with self.assertRaises(QuestionBankError):
            validate_question_bank(bank)

    def test_time_limit_range_is_enforced(self):
        bank = json.loads(SHIPPED_BANK.read_text())
        bank["quizzes"][0]["questions"][0]["time_limit"] = 4
        with self.assertRaises(QuestionBankError):
            validate_question_bank(bank)

    def test_empty_file_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "empty.json"
            path.write_text("")
            with self.assertRaises(QuestionBankError):
                load_question_bank(str(path))

    def test_malformed_json_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bad.json"
            path.write_text("{not json")
            with self.assertRaises(QuestionBankError):
                load_question_bank(str(path))


# ----------------------------------------------------------------------
# server start-up tests
# ----------------------------------------------------------------------
class ServerStartupTests(unittest.TestCase):
    def test_invalid_bank_exits_non_zero_without_ready(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bad.json"
            path.write_text(json.dumps({"schema_version": 1, "quizzes": []}))
            code, out = run_server_once(path, free_port())
        self.assertNotEqual(code, 0)
        self.assertNotIn("SERVER_READY", out)
        self.assertIn("ERROR", out)

    def test_missing_file_exits_non_zero(self):
        code, out = run_server_once(Path("/nonexistent/bank.json"), free_port())
        self.assertNotEqual(code, 0)
        self.assertNotIn("SERVER_READY", out)

    def test_ctrl_c_shuts_down_cleanly(self):
        port = free_port()
        server = start_server(SHIPPED_BANK, port)
        try:
            time.sleep(0.3)
            server.proc.send_signal(signal.SIGINT)
            try:
                code = server.proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.fail("server did not release resources and exit after Ctrl+C")
        finally:
            server.terminate()
        self.assertEqual(code, 0)


# ----------------------------------------------------------------------
# protocol robustness tests
# ----------------------------------------------------------------------
class ProtocolRobustnessTests(unittest.TestCase):
    def setUp(self):
        self.port = free_port()
        self.server = start_server(SHIPPED_BANK, self.port)

    def tearDown(self):
        self.server.terminate()

    def test_unknown_request_type_returns_error_and_keeps_connection(self):
        client = TestClient("127.0.0.1", self.port, "carol", "Carol")
        try:
            self.assertTrue(client.login()[0])
            ok, data = client.request("does_not_exist")
            self.assertFalse(ok)
            self.assertEqual(data["code"], proto.ERR_BAD_REQUEST)
            # Connection still usable.
            self.assertTrue(client.request(proto.REQ_PING)[0])
        finally:
            client.close()

    def test_malformed_frame_is_rejected(self):
        sock = socket.create_connection(("127.0.0.1", self.port), timeout=5)
        try:
            # Valid 4-byte length header, but the payload is not JSON.
            sock.sendall((5).to_bytes(4, "big") + b"hello")
            message = read_one_frame(sock)
            self.assertEqual(message["kind"], proto.KIND_ERROR)
            self.assertEqual(message["data"]["code"], proto.ERR_PROTOCOL_ERROR)
            self.assertEqual(sock.recv(16), b"")
        finally:
            sock.close()

    def test_duplicate_active_id_is_rejected(self):
        first = TestClient("127.0.0.1", self.port, "dave", "Dave")
        second = TestClient("127.0.0.1", self.port, "dave", "Dave")
        try:
            self.assertTrue(first.login()[0])
            ok, data = second.login()
            self.assertFalse(ok)
            self.assertEqual(data["code"], proto.ERR_DUPLICATE_ID)
        finally:
            first.close()
            second.close()


# ----------------------------------------------------------------------
# end-to-end tests
# ----------------------------------------------------------------------
class EndToEndTests(unittest.TestCase):
    def setUp(self):
        self.port = free_port()
        self.server = start_server(SHIPPED_BANK, self.port)

    def tearDown(self):
        self.server.terminate()

    def test_full_two_player_quiz(self):
        alice = TestClient("127.0.0.1", self.port, "alice", auto_answers=dict(CORRECT_ANSWERS))
        bob = TestClient("127.0.0.1", self.port, "bob", auto_answers={qid: "A" for qid in CORRECT_ANSWERS})
        try:
            self.assertTrue(alice.login()[0])
            self.assertTrue(bob.login()[0])

            ok, data = alice.request(proto.REQ_LIST_QUIZZES)
            self.assertTrue(ok)
            quiz_ids = [quiz["quiz_id"] for quiz in data["quizzes"]]
            self.assertIn("network-basics", quiz_ids)
            # The catalogue must expose only these keys: no question text,
            # options or answers may leak before a quiz starts.
            allowed_keys = {"quiz_id", "title", "description", "question_count", "total_points"}
            for quiz in data["quizzes"]:
                self.assertEqual(set(quiz), allowed_keys)
            self.assertNotIn("NET001", json.dumps(data))
            self.assertNotIn("answer", json.dumps(data))

            ok, data = alice.request(proto.REQ_CREATE_ROOM, {"quiz_id": "network-basics"})
            self.assertTrue(ok)
            room_id = data["room_id"]

            ok, data = bob.request(proto.REQ_LIST_ROOMS)
            self.assertTrue(ok)
            self.assertIn(room_id, [room["room_id"] for room in data["rooms"]])

            self.assertTrue(bob.request(proto.REQ_JOIN_ROOM, {"room_id": room_id})[0])
            self.assertTrue(alice.request(proto.REQ_SET_READY, {"ready": True})[0])
            self.assertTrue(bob.request(proto.REQ_SET_READY, {"ready": True})[0])

            ok, info = alice.request(proto.REQ_ROOM_INFO)
            self.assertTrue(ok)
            self.assertEqual(info["ready_count"], 2)

            # The client must not be able to start before everyone is ready.
            self.assertTrue(bob.request(proto.REQ_SET_READY, {"ready": False})[0])
            ok, data = alice.request(proto.REQ_START_QUIZ)
            self.assertFalse(ok)
            self.assertEqual(data["code"], proto.ERR_NOT_READY)
            self.assertTrue(bob.request(proto.REQ_SET_READY, {"ready": True})[0])

            self.assertTrue(alice.request(proto.REQ_START_QUIZ)[0])

            finished = alice.wait_event(proto.EV_QUIZ_FINISHED, timeout=40)
            bob.wait_event(proto.EV_QUIZ_FINISHED, timeout=40)

            leaderboard = finished["data"]["leaderboard"]
            self.assertEqual(leaderboard[0]["user_id"], "alice")
            self.assertEqual(leaderboard[0]["score"], 500)
            self.assertEqual(leaderboard[1]["user_id"], "bob")
        finally:
            alice.close()
            bob.close()

    def test_ten_clients_in_two_isolated_rooms(self):
        # Ten clients connect at once; two rooms run quizzes independently.
        clients = [
            TestClient("127.0.0.1", self.port, f"p{i}", auto_answers=dict(CORRECT_ANSWERS))
            for i in range(10)
        ]
        try:
            for client in clients:
                self.assertTrue(client.login()[0])

            # Room A: p0 (host) + p1..p4.  Room B: p5 (host) + p6..p9.
            ok, data = clients[0].request(proto.REQ_CREATE_ROOM, {"quiz_id": "network-basics"})
            self.assertTrue(ok)
            room_a = data["room_id"]
            for client in clients[1:5]:
                self.assertTrue(client.request(proto.REQ_JOIN_ROOM, {"room_id": room_a})[0])

            ok, data = clients[5].request(proto.REQ_CREATE_ROOM, {"quiz_id": "network-basics"})
            self.assertTrue(ok)
            room_b = data["room_id"]
            for client in clients[6:10]:
                self.assertTrue(client.request(proto.REQ_JOIN_ROOM, {"room_id": room_b})[0])
            self.assertNotEqual(room_a, room_b)

            # Room B has one player who answers incorrectly.
            clients[9].auto_answers = {qid: "A" for qid in CORRECT_ANSWERS}

            for client in clients:
                self.assertTrue(client.request(proto.REQ_SET_READY, {"ready": True})[0])

            ok, info = clients[0].request(proto.REQ_ROOM_INFO)
            self.assertTrue(ok)
            self.assertEqual(info["participant_count"], 5)

            self.assertTrue(clients[0].request(proto.REQ_START_QUIZ)[0])
            self.assertTrue(clients[5].request(proto.REQ_START_QUIZ)[0])

            finished_a = clients[0].wait_event(proto.EV_QUIZ_FINISHED, timeout=40)
            finished_b = clients[5].wait_event(proto.EV_QUIZ_FINISHED, timeout=40)

            self.assertEqual(finished_a["data"]["room_id"], room_a)
            self.assertEqual(finished_b["data"]["room_id"], room_b)

            board_a = finished_a["data"]["leaderboard"]
            board_b = finished_b["data"]["leaderboard"]
            # Neither room's leaderboard contains the other room's players.
            self.assertEqual({row["user_id"] for row in board_a}, {f"p{i}" for i in range(5)})
            self.assertEqual({row["user_id"] for row in board_b}, {f"p{i}" for i in range(5, 10)})
            # Everyone in room A answered correctly; room B's p9 answered "A"
            # to everything, which happens to be correct for NET002 only.
            self.assertTrue(all(row["score"] == 500 for row in board_a))
            self.assertEqual(board_b[-1]["user_id"], "p9")
            self.assertEqual(board_b[-1]["score"], 100)
        finally:
            for client in clients:
                client.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
