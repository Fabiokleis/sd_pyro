from dataclasses import dataclass
from enum import Enum
from typing import TypedDict


class NodeRole(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    term: int
    index: int
    command: str


@dataclass
class VoteRequest:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass
class VoteReply:
    term: int
    vote_granted: bool


@dataclass
class AppendEntries:
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: list[LogEntry]
    leader_commit: int


@dataclass
class AppendReply:
    term: int
    success: bool
    match_index: int


# Wire format TypedDicts — Pyro5 RPC payloads are plain dicts at the boundary.
# Use these in RaftNodeRPC; use dataclasses for internal RaftNode logic.


class LogEntryWire(TypedDict):
    term: int
    index: int
    command: str


class VoteRequestWire(TypedDict):
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


class VoteReplyWire(TypedDict):
    term: int
    vote_granted: bool


class AppendEntriesWire(TypedDict):
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: list[LogEntryWire]
    leader_commit: int


class AppendReplyWire(TypedDict):
    term: int
    success: bool
    match_index: int
