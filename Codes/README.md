# CSI201 Networked Quiz Tournament

A command-line, multi-room quiz tournament built with Python 3.12 and raw TCP
sockets (standard library only).  One server owns the question bank; any number
of clients connect over TCP, create or join rooms, and play quizzes that the
server times and scores.

## Exact start-up commands

The commands below are the assessed commands and are run from inside the
`Codes` directory of the examiner Docker image.

```bash
# Terminal 1 - server (validates the bank, then prints SERVER_READY)
python3 server.py --host 0.0.0.0 --port 9000 --questions questions.json

# Terminal 2..N - one client per player
python3 client.py --server 127.0.0.1 --port 9000 --id alice --name Alice
python3 client.py --server 127.0.0.1 --port 9000 --id bob   --name Bob
```

The server prints `SERVER_READY` on standard output only after the question
bank has been validated and the listening socket is open.  If the bank is
missing, malformed or schema-invalid the server prints a diagnostic to standard
error and exits with a non-zero status without listening for clients.

Pressing Ctrl+C stops the server cleanly: it stops room timers, closes the
listening socket and every open connection, then exits with status 0.  `SIGTERM`
is handled the same way, which makes scripted shutdowns reliable.

## Repository layout

| File | Responsibility |
| --- | --- |
| `server.py` | Argument parsing, question-bank validation, TCP server, rooms, timing, scoring |
| `client.py` | CLI client: command loop, asynchronous event display, request/response handling |
| `protocol.py` | Length-prefixed JSON framing, envelope kinds, message and error constants |
| `questionbank.py` | Loading and validation of the compulsory question-bank schema |
| `questions.json` | Development question bank (2 quizzes, 5 questions each) |
| `tests/test_smoke.py` | Repeatable smoke tests (validation, start-up, end-to-end play) |

## CLI command summary

Client commands (typed at the `>` prompt):

| Command | Meaning |
| --- | --- |
| `help` | Show the command list |
| `quizzes` | List quizzes (id, title, counts) - no questions or answers revealed |
| `rooms` | List joinable rooms in the lobby state |
| `create <quiz_id>` | Create a room for a quiz; you become the host |
| `join <room_id>` | Join an existing lobby |
| `info` | Show the current room and its participants |
| `ready [on\|off]` | Indicate readiness (default `on`) |
| `start` | Host only: start when every participant is ready |
| `answer <A\|B\|C\|D>` | Answer the question that is currently open |
| `leave` | Leave the current room |
| `status` | Show your identity and room |
| `quit` | Disconnect and exit |

Server arguments: `--host`, `--port`, `--questions`, optional `--log-level`.

## Application-layer protocol (summary)

Every message is a UTF-8 JSON object prefixed by a 4-byte unsigned big-endian
length header.  Receivers must cope with TCP fragmentation and coalescing; the
`MessageReader` class does this with an internal buffer.

Envelope: `{"v": 1, "kind": ..., "type": ..., "id": ..., "data": {...}}`

* `kind = request` - client to server, `id` is a client request identifier.
* `kind = response` - server to client, `id` echoes the request.
* `kind = event` - asynchronous server push (room state, questions, results).
* `kind = error` - `id` set for a request error, `null` for a fatal error.

Request types: `login`, `list_quizzes`, `create_room`, `list_rooms`,
`join_room`, `leave_room`, `room_info`, `set_ready`, `start_quiz`, `answer`,
`ping`.  Event types: `room_state`, `question_started`, `question_closed`,
`quiz_finished`.  See `protocol.py` for the authoritative catalogue.

## Game rules implemented

* A room is created by its host and lives in the `lobby` state until it starts.
  Once a quiz starts the room is `playing`; when the last question closes it
  becomes `finished` and is no longer joinable.
* Only the host may start the quiz, and only when every participant is ready.
* The server controls question order (file order) and timing.  Each question is
  open for its own `time_limit` seconds, and closes early once every active
  player has answered.
* At most one answer per active player per question.  A correct accepted answer
  scores the question's `points`; incorrect, duplicate, missing or late answers
  score zero.  There is no time bonus.
* The correct answer is only sent in the `question_closed` event, never in the
  question itself.
* Final leaderboard order: score descending, then correct answers descending,
  then user id ascending.

## Assumptions and known limitations

* Framing is a 4-byte length prefix with JSON payloads, and the maximum message
  size is 1 MB.
* Duplicate active user ids are rejected deterministically: the newcomer is
  refused with `duplicate_id`.  Ids become free again when the client disconnects.
* A user may be in at most one room at a time.  Leaving a room during play
  removes the player and their score from that room.
* If the host leaves, the host role is handed to the remaining member with the
  lowest user id.
* This is a working framework for smoke testing Tasks 1-6.  Not yet implemented:
  reconnect/resume of an interrupted session, per-question statistics for the
  documentation, and any persistence beyond process lifetime.

## Smoke tests

From the `Codes` directory:

```bash
python3 -m unittest discover -s tests -v
```

The tests cover question-bank validation, the `SERVER_READY` contract, invalid
start-up exit codes, an end-to-end two-player quiz, duplicate-id rejection,
malformed-frame handling and Ctrl+C shutdown.
