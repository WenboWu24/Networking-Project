"""Loading and validation of the compulsory question-bank format.

The schema implemented here is the one prescribed by the CSI201 Coursework I
specification.  The whole file is validated before the server opens its
listening socket: any problem raises :class:`QuestionBankError` with a
meaningful diagnostic, and the caller exits with a non-zero status.

Only the server process is allowed to read this file.  Clients obtain quiz and
question information exclusively through the application-layer protocol.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

#: Maximum accepted question-bank file size (1 MB).
MAX_FILE_BYTES = 1024 * 1024

#: quiz_id / question_id character set and length.
_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")

#: options must contain exactly these keys.
_OPTION_KEYS = ("A", "B", "C", "D")


class QuestionBankError(Exception):
    """Raised when the question bank is missing, unreadable or invalid."""


@dataclass(frozen=True)
class Question:
    question_id: str
    text: str
    options: Dict[str, str]
    answer: str
    time_limit: int
    points: int


@dataclass(frozen=True)
class Quiz:
    quiz_id: str
    title: str
    description: str
    questions: List[Question]


@dataclass(frozen=True)
class QuestionBank:
    schema_version: int
    quizzes: List[Quiz]
    source_path: str

    @property
    def total_questions(self) -> int:
        return sum(len(quiz.questions) for quiz in self.quizzes)

    def find_quiz(self, quiz_id: str) -> Optional[Quiz]:
        for quiz in self.quizzes:
            if quiz.quiz_id == quiz_id:
                return quiz
        return None


# ----------------------------------------------------------------------
# primitive checks
# ----------------------------------------------------------------------
def _is_int(value: Any) -> bool:
    # ``bool`` is a subclass of ``int`` and must not pass as an integer.
    return isinstance(value, int) and not isinstance(value, bool)


def _visible(value: Any, min_len: int, max_len: int) -> bool:
    """Return True when *value* is a string of visible characters in range."""
    if not isinstance(value, str):
        return False
    if not (min_len <= len(value) <= max_len):
        return False
    return all(ch.isprintable() for ch in value)


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise QuestionBankError(message)


# ----------------------------------------------------------------------
# validation
# ----------------------------------------------------------------------
def _parse_question(raw: Any, quiz_id: str, index: int, seen_question_ids: set) -> Question:
    where = f"quizzes[{quiz_id}].questions[{index}]"
    _require(isinstance(raw, dict), f"{where} must be an object")

    question_id = raw.get("question_id")
    _require(
        isinstance(question_id, str) and bool(_ID_RE.fullmatch(question_id)),
        f"{where}.question_id must be 1-32 characters from [A-Za-z0-9_-]",
    )
    _require(question_id not in seen_question_ids, f"duplicate question_id {question_id!r} (must be unique in the file)")
    seen_question_ids.add(question_id)

    _require(_visible(raw.get("text"), 1, 500), f"{where}.text must be 1-500 visible characters")

    options = raw.get("options")
    _require(isinstance(options, dict), f"{where}.options must be an object")
    _require(
        set(options.keys()) == set(_OPTION_KEYS),
        f"{where}.options must contain exactly the keys A, B, C and D",
    )
    for key in _OPTION_KEYS:
        _require(
            isinstance(options[key], str) and 1 <= len(options[key]) <= 200,
            f"{where}.options[{key}] must be 1-200 characters",
        )

    answer = raw.get("answer")
    _require(answer in _OPTION_KEYS, f"{where}.answer must be exactly A, B, C or D")

    time_limit = raw.get("time_limit")
    _require(_is_int(time_limit) and 5 <= time_limit <= 60, f"{where}.time_limit must be an integer in 5..60")

    points = raw.get("points")
    _require(_is_int(points) and 1 <= points <= 1000, f"{where}.points must be an integer in 1..1000")

    return Question(
        question_id=question_id,
        text=raw["text"],
        options={key: options[key] for key in _OPTION_KEYS},
        answer=answer,
        time_limit=time_limit,
        points=points,
    )


def _parse_quiz(raw: Any, index: int, seen_quiz_ids: set, seen_question_ids: set) -> Quiz:
    where = f"quizzes[{index}]"
    _require(isinstance(raw, dict), f"{where} must be an object")

    quiz_id = raw.get("quiz_id")
    _require(
        isinstance(quiz_id, str) and bool(_ID_RE.fullmatch(quiz_id)),
        f"{where}.quiz_id must be 1-32 characters from [A-Za-z0-9_-]",
    )
    _require(quiz_id not in seen_quiz_ids, f"duplicate quiz_id {quiz_id!r} (must be unique)")
    seen_quiz_ids.add(quiz_id)
    where = f"quiz {quiz_id!r}"

    _require(_visible(raw.get("title"), 1, 100), f"{where}.title must be 1-100 visible characters")
    _require(_visible(raw.get("description"), 0, 300), f"{where}.description must be 0-300 visible characters")

    questions = raw.get("questions")
    _require(isinstance(questions, list), f"{where}.questions must be an array")
    _require(5 <= len(questions) <= 50, f"{where}.questions must contain 5 to 50 questions")

    parsed = [_parse_question(q, quiz_id, i, seen_question_ids) for i, q in enumerate(questions)]
    return Quiz(
        quiz_id=quiz_id,
        title=raw["title"],
        description=raw["description"],
        questions=parsed,
    )


def validate_question_bank(raw: Any, source_path: str = "<memory>") -> QuestionBank:
    """Validate an already-decoded object and return a :class:`QuestionBank`."""
    _require(isinstance(raw, dict), "top-level value must be a JSON object")
    _require("schema_version" in raw, "top-level object must contain schema_version")
    _require("quizzes" in raw, "top-level object must contain quizzes")

    schema_version = raw["schema_version"]
    _require(_is_int(schema_version) and schema_version == 1, "schema_version must be the integer 1")

    quizzes = raw["quizzes"]
    _require(isinstance(quizzes, list), "quizzes must be an array")
    _require(1 <= len(quizzes) <= 20, "quizzes must contain 1 to 20 quiz objects")

    seen_quiz_ids: set = set()
    seen_question_ids: set = set()
    parsed_quizzes = [
        _parse_quiz(quiz, index, seen_quiz_ids, seen_question_ids)
        for index, quiz in enumerate(quizzes)
    ]
    return QuestionBank(schema_version=schema_version, quizzes=parsed_quizzes, source_path=source_path)


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-standard JSON constant {value}")


def load_question_bank(path: str) -> QuestionBank:
    """Read *path* from disk and validate it.

    Raises :class:`QuestionBankError` with a human-readable diagnostic for a
    missing file, an empty file, an oversized file, malformed JSON or any
    schema violation.
    """
    try:
        with open(path, "rb") as handle:
            blob = handle.read(MAX_FILE_BYTES + 1)
    except OSError as exc:
        raise QuestionBankError(f"cannot read question bank {path!r}: {exc}") from exc

    if not blob:
        raise QuestionBankError(f"question bank {path!r} is empty")
    if len(blob) > MAX_FILE_BYTES:
        raise QuestionBankError(f"question bank {path!r} exceeds the maximum size of {MAX_FILE_BYTES} bytes")

    try:
        text = blob.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise QuestionBankError(f"question bank {path!r} is not valid UTF-8: {exc}") from exc

    try:
        raw = json.loads(text, parse_constant=_reject_json_constant)
    except (ValueError, RecursionError) as exc:
        raise QuestionBankError(f"question bank {path!r} is not valid JSON: {exc}") from exc

    return validate_question_bank(raw, source_path=path)


def quiz_summary(quiz: Quiz) -> Dict[str, Any]:
    """Public catalogue view of a quiz (no question text, options or answers)."""
    return {
        "quiz_id": quiz.quiz_id,
        "title": quiz.title,
        "description": quiz.description,
        "question_count": len(quiz.questions),
        "total_points": sum(q.points for q in quiz.questions),
    }


def question_view(question: Question, index: int, total: int) -> Dict[str, Any]:
    """Wire view of a live question.  The ``answer`` is deliberately omitted."""
    return {
        "question_id": question.question_id,
        "index": index,
        "total": total,
        "text": question.text,
        "options": dict(question.options),
        "time_limit": question.time_limit,
        "points": question.points,
    }
