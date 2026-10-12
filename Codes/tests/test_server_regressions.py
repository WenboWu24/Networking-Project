"""Regression tests for server timing, validation, room races and cleanup."""

from __future__ import annotations

import copy
import json
import socket
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

CODES_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(CODES_DIR))

import protocol as proto
from questionbank import QuestionBankError, load_question_bank, validate_question_bank
from server import ClientSession, QuizServer, Room, ROOM_PLAYING
from tests.test_smoke import SHIPPED_BANK, TestClient, free_port, start_server


class SessionStub:
    def __init__(self, user_id):
        self.user_id = user_id
        self.display_name = user_id
        self.room_id = None
        self.messages = []

    def send_response(self, id, type_, data=None):
        self.messages.append((proto.KIND_RESPONSE, data))

    def send_error(self, id, code, message):
        self.messages.append((proto.KIND_ERROR, code))

    def send_event(self, type_, data=None):
        pass


class TimingTests(unittest.TestCase):
    def setUp(self):
        quiz = load_question_bank(str(SHIPPED_BANK)).quizzes[0]
        self.room = Room("timing", quiz, "player")
        self.player = SessionStub("player")
        self.room.add_member(self.player)
        self.room.state = ROOM_PLAYING
        self.question = quiz.questions[0]
        self.room.current_question = self.question
        self.room.current_players = [self.player.user_id]
        self.room.question_deadline = time.monotonic() + 10

    def test_expired_answer_is_rejected_without_waiting_for_timer_thread(self):
        self.room.question_deadline = time.monotonic() - 0.01
        status, _ = self.room.submit_answer("player", self.question.question_id, self.question.answer)
        self.assertEqual(status, proto.ERR_LATE_ANSWER)
        closed = self.room._close_question(self.question)
        self.assertEqual(closed["scores"][0]["score"], 0)

    def test_answer_exactly_at_deadline_is_rejected(self):
        self.room.question_deadline = 100.0
        with patch("server.time.monotonic", return_value=100.0):
            status, _ = self.room.submit_answer("player", self.question.question_id, self.question.answer)
        self.assertEqual(status, proto.ERR_LATE_ANSWER)

    def test_question_close_rejects_answer_during_intermission(self):
        self.room._close_question(self.question)
        status, _ = self.room.submit_answer("player", self.question.question_id, self.question.answer)
        self.assertEqual(status, proto.ERR_LATE_ANSWER)

    def test_duplicate_answer_cannot_change_score(self):
        self.assertEqual(self.room.submit_answer("player", self.question.question_id, self.question.answer)[0], "accepted")
        self.assertEqual(self.room.submit_answer("player", self.question.question_id, "A")[0], proto.ERR_ALREADY_ANSWERED)
        closed = self.room._close_question(self.question)
        self.assertEqual(closed["scores"][0]["score"], self.question.points)
        self.assertEqual(len(closed["results"]), 1)

    def test_nonparticipant_cannot_submit_answer(self):
        status, _ = self.room.submit_answer("outsider", self.question.question_id, self.question.answer)
        self.assertEqual(status, proto.ERR_NOT_IN_ROOM)

    def test_departing_player_answer_and_score_are_removed(self):
        self.room.submit_answer("player", self.question.question_id, self.question.answer)
        self.room.remove_member("player")
        closed = self.room._close_question(self.question)
        self.assertEqual(closed["results"], [])
        self.assertNotIn("player", self.room.scores)


class QuestionBankRegressionTests(unittest.TestCase):
    def setUp(self):
        self.bank = json.loads(SHIPPED_BANK.read_text(encoding="utf-8"))

    def test_ids_with_newline_are_rejected(self):
        for field in ("quiz_id", "question_id"):
            with self.subTest(field=field):
                bank = copy.deepcopy(self.bank)
                target = bank["quizzes"][0] if field == "quiz_id" else bank["quizzes"][0]["questions"][0]
                target[field] += "\n"
                with self.assertRaises(QuestionBankError):
                    validate_question_bank(bank)

    def test_options_accept_characters_not_restricted_to_visible_text(self):
        self.bank["quizzes"][0]["questions"][0]["options"]["A"] = "first line\nsecond line"
        self.assertEqual(validate_question_bank(self.bank).quizzes[0].questions[0].options["A"], "first line\nsecond line")

    def test_additional_members_remain_accepted(self):
        self.bank["metadata"] = {"author": "test"}
        self.bank["quizzes"][0]["extra"] = True
        self.bank["quizzes"][0]["questions"][0]["extra"] = [1, 2]
        self.assertEqual(len(validate_question_bank(self.bank).quizzes), 2)

    def test_nonstandard_json_constant_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bank.json"
            text = json.dumps(self.bank)[:-1] + ', "extra": NaN}'
            path.write_text(text, encoding="utf-8")
            with self.assertRaises(QuestionBankError):
                load_question_bank(str(path))


class RoomConcurrencyTests(unittest.TestCase):
    def setUp(self):
        bank = load_question_bank(str(SHIPPED_BANK))
        self.server = QuizServer("127.0.0.1", 12345, bank)
        self.host = SessionStub("host")
        self.room = Room("race", bank.quizzes[0], self.host.user_id)
        self.host.room_id = self.room.room_id
        self.room.add_member(self.host)
        self.server._rooms[self.room.room_id] = self.room

    def test_two_simultaneous_joins_cannot_exceed_capacity(self):
        for i in range(18):
            self.room.add_member(SessionStub(f"existing{i}"))
        newcomers = [SessionStub("new-a"), SessionStub("new-b")]
        reached_add = threading.Condition()
        add_count = 0
        original_add = self.room.add_member

        def observe_add(session):
            nonlocal add_count
            with reached_add:
                add_count += 1
                reached_add.notify_all()
            original_add(session)

        with patch.object(self.room, "add_member", side_effect=observe_add):
            with self.room.cond:
                threads = [threading.Thread(target=self.server._req_join_room, args=(s, 1, {"room_id": "race"})) for s in newcomers]
                for thread in threads:
                    thread.start()
                # Force both old check-before-lock callers to wait in add_member.
                # Correct callers wait on room.cond before checking capacity.
                with reached_add:
                    reached_add.wait_for(lambda: add_count == 2, timeout=0.3)
            for thread in threads:
                thread.join(timeout=2)
                self.assertFalse(thread.is_alive())
        self.assertEqual(len(self.room.members), 20)
        self.assertEqual(sum(s.room_id == "race" for s in newcomers), 1)

    def test_start_cannot_race_with_an_unready_join(self):
        self.room.set_ready("host", True)
        response_started = threading.Event()
        release_response = threading.Event()
        newcomer = SessionStub("new-player")
        original_response = self.host.send_response

        def pause_response(*args):
            response_started.set()
            release_response.wait(timeout=2)
            original_response(*args)

        try:
            with patch.object(self.host, "send_response", side_effect=pause_response):
                starter = threading.Thread(target=self.server._req_start_quiz, args=(self.host, 1, {}))
                starter.start()
                self.assertTrue(response_started.wait(timeout=2))
                joiner = threading.Thread(target=self.server._req_join_room, args=(newcomer, 2, {"room_id": "race"}))
                joiner.start()
                joiner.join(timeout=0.2)
                release_response.set()
                starter.join(timeout=2)
                joiner.join(timeout=2)
                self.assertFalse(starter.is_alive())
                self.assertFalse(joiner.is_alive())
            self.assertIsNone(newcomer.room_id)
            self.assertEqual(newcomer.messages[0][1], proto.ERR_ROOM_NOT_JOINABLE)
        finally:
            release_response.set()
            self.room.stop()


class ConnectionCleanupTests(unittest.TestCase):
    def test_failed_send_closes_socket_and_unblocks_reader(self):
        class BrokenSocket:
            closed = False
            def sendall(self, frame):
                raise OSError("disconnected")
            def shutdown(self, how):
                pass
            def close(self):
                self.closed = True
        sock = BrokenSocket()
        session = ClientSession(sock, ("127.0.0.1", 12345), None)
        session.send_response(1, "ping")
        self.assertTrue(sock.closed)

    def test_shutdown_closes_clients_that_have_not_logged_in(self):
        bank = load_question_bank(str(SHIPPED_BANK))
        server = QuizServer("127.0.0.1", free_port(), bank)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        sock = None
        try:
            deadline = time.monotonic() + 2
            while server._listen_socket is None and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertIsNotNone(server._listen_socket)
            sock = socket.create_connection(("127.0.0.1", server.port), timeout=2)
            deadline = time.monotonic() + 2
            while not server._connections and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(server._connections)
            server.shutdown()
            thread.join(timeout=2)
            self.assertFalse(thread.is_alive())
            self.assertEqual(sock.recv(1), b"")
            self.assertFalse(server._connections)
        finally:
            server.shutdown()
            thread.join(timeout=2)
            if sock is not None:
                sock.close()


class NetworkServerRegressionTests(unittest.TestCase):
    def setUp(self):
        self.port = free_port()
        self.server = start_server(SHIPPED_BANK, self.port)
        self.clients = []

    def tearDown(self):
        for client in self.clients:
            client.close()
        self.server.terminate()

    def client(self, id, name):
        client = TestClient("127.0.0.1", self.port, id, name)
        self.clients.append(client)
        return client

    def test_duplicate_display_name_is_rejected(self):
        first = self.client("first-id", "Shared Name")
        second = self.client("second-id", "Shared Name")
        self.assertTrue(first.login()[0])
        self.assertFalse(second.login()[0])
        self.assertTrue(first.request(proto.REQ_PING)[0])

    def test_unsupported_version_and_boolean_id_return_request_errors(self):
        client = self.client("version-user", "Version User")
        self.assertTrue(client.login()[0])
        for change in ({"v": 2}, {"id": True}):
            envelope = proto.make_request(proto.REQ_PING, {}, 100)
            envelope.update(change)
            data = json.dumps(envelope).encode("utf-8")
            sock = socket.create_connection(("127.0.0.1", self.port), timeout=2)
            try:
                sock.sendall(proto.HEADER.pack(len(data)) + data)
                from tests.test_smoke import read_one_frame
                message = read_one_frame(sock)
                self.assertEqual(message["kind"], proto.KIND_ERROR)
                self.assertEqual(message["data"]["code"], proto.ERR_BAD_REQUEST)
            finally:
                sock.close()
        self.assertTrue(client.request(proto.REQ_PING)[0])

    def test_disconnect_does_not_block_remaining_player_or_another_room(self):
        answers = {"NET001": "B", "NET002": "A", "NET003": "C", "NET004": "B", "NET005": "D"}
        survivor = self.client("survivor", "Survivor")
        departing = self.client("departing", "Departing")
        observer = self.client("observer", "Observer")
        survivor.auto_answers = answers
        for client in (survivor, departing, observer):
            self.assertTrue(client.login()[0])
        ok, room = survivor.request(proto.REQ_CREATE_ROOM, {"quiz_id": "network-basics"})
        self.assertTrue(ok)
        self.assertTrue(departing.request(proto.REQ_JOIN_ROOM, {"room_id": room["room_id"]})[0])
        self.assertTrue(observer.request(proto.REQ_CREATE_ROOM, {"quiz_id": "transport-layer"})[0])
        for client in (survivor, departing):
            self.assertTrue(client.request(proto.REQ_SET_READY, {"ready": True})[0])
        self.assertTrue(survivor.request(proto.REQ_START_QUIZ)[0])
        departing.wait_event(proto.EV_QUESTION_STARTED, timeout=2)
        departing.close()
        closed = survivor.wait_event(proto.EV_QUESTION_CLOSED, timeout=2)
        self.assertEqual(closed["data"]["scores"][0]["score"], 100)
        self.assertTrue(observer.request(proto.REQ_ROOM_INFO)[0])
        finished = survivor.wait_event(proto.EV_QUIZ_FINISHED, timeout=8)
        self.assertEqual([(row["user_id"], row["score"]) for row in finished["data"]["leaderboard"]], [("survivor", 500)])


if __name__ == "__main__":
    unittest.main(verbosity=2)
