"""
A from-scratch, single-process educational implementation of the Raft
consensus algorithm (leader election + log replication).

Design choice: the "network" is simulated with direct, synchronous calls
(see Cluster below) rather than real sockets/threads. Real Raft nodes talk
over an async, lossy network -- but modeling that with actual sleeps and
threads makes tests slow and flaky. Here, a virtual "tick" is the unit of
time, and a Cluster object explicitly controls which messages get
delivered. This keeps every scenario -- including partitions, dropped
messages, and stale leaders -- fully deterministic and easy to test,
while the *safety logic* inside RaftNode (the part that actually matters)
is the same logic a real networked implementation would use.

Log indices are 1-based, matching the Raft paper. self.log[i] holds the
entry for index i+1.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class Role(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    term: int
    command: object


@dataclass
class RequestVoteArgs:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass
class RequestVoteReply:
    term: int
    vote_granted: bool


@dataclass
class AppendEntriesArgs:
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: list
    leader_commit: int


@dataclass
class AppendEntriesReply:
    term: int
    success: bool


class RaftNode:
    def __init__(self, node_id, peer_ids, election_timeout_ticks=10):
        self.id = node_id
        self.peers = list(peer_ids)

        # --- Persistent state (would survive a crash in a real system) ---
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: list[LogEntry] = []

        # --- Volatile state, all servers ---
        self.commit_index = 0
        self.last_applied = 0
        self.role = Role.FOLLOWER
        self.leader_id: Optional[str] = None
        self.applied: list = []  # stand-in "state machine": committed commands in order

        # --- Volatile state, leaders only (reset on election) ---
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}

        # --- Election bookkeeping ---
        self.votes_received: set[str] = set()
        self.election_timeout_ticks = election_timeout_ticks
        self.election_elapsed = 0

    # ------------------------------------------------------------------
    # Log helpers (1-based indexing on top of a 0-based Python list)
    # ------------------------------------------------------------------
    def last_log_index(self) -> int:
        return len(self.log)

    def last_log_term(self) -> int:
        return self.log[-1].term if self.log else 0

    def term_at(self, index: int) -> int:
        """Term of the entry at 1-based `index`, or 0 if index is 0 (the
        conceptual empty-log sentinel used by the consistency check)."""
        if index <= 0:
            return 0
        return self.log[index - 1].term

    def entries_from(self, index: int) -> list:
        """Entries starting at 1-based `index`, to the end of the log."""
        if index <= 0:
            return list(self.log)
        return list(self.log[index - 1:])

    # ------------------------------------------------------------------
    # Timers / role transitions
    # ------------------------------------------------------------------
    def reset_election_timer(self):
        self.election_elapsed = 0

    def tick(self) -> Optional[str]:
        """Advance this node's clock by one simulated step. Returns
        "timeout" if a follower/candidate's election timer just expired."""
        if self.role == Role.LEADER:
            return None
        self.election_elapsed += 1
        if self.election_elapsed >= self.election_timeout_ticks:
            return "timeout"
        return None

    def become_follower(self, term: int):
        """Step down. Called whenever we see a higher term from anyone --
        this is the mechanical, no-judgment-call reflex described in the
        paper: a bigger term number always wins, full stop."""
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
        self.role = Role.FOLLOWER
        self.reset_election_timer()

    # ------------------------------------------------------------------
    # Leader election
    # ------------------------------------------------------------------
    def start_election(self) -> RequestVoteArgs:
        self.role = Role.CANDIDATE
        self.current_term += 1
        self.voted_for = self.id
        self.votes_received = {self.id}
        self.reset_election_timer()
        return RequestVoteArgs(
            term=self.current_term,
            candidate_id=self.id,
            last_log_index=self.last_log_index(),
            last_log_term=self.last_log_term(),
        )

    def handle_request_vote(self, args: RequestVoteArgs) -> RequestVoteReply:
        # Rule 1: never vote for a stale term.
        if args.term < self.current_term:
            return RequestVoteReply(term=self.current_term, vote_granted=False)

        # A higher term always wins, unconditionally.
        if args.term > self.current_term:
            self.become_follower(args.term)

        # Up-to-date check: candidate's log must be at least as complete
        # as ours. Compare last-entry term first, break ties on length.
        log_ok = (args.last_log_term > self.last_log_term()) or (
            args.last_log_term == self.last_log_term()
            and args.last_log_index >= self.last_log_index()
        )

        can_vote = self.voted_for in (None, args.candidate_id)

        if can_vote and log_ok:
            self.voted_for = args.candidate_id
            self.reset_election_timer()  # granting a vote counts as "leader-ish" activity
            return RequestVoteReply(term=self.current_term, vote_granted=True)

        return RequestVoteReply(term=self.current_term, vote_granted=False)

    def handle_request_vote_reply(self, peer_id: str, reply: RequestVoteReply, total_nodes: int):
        if reply.term > self.current_term:
            self.become_follower(reply.term)
            return
        if self.role != Role.CANDIDATE or reply.term < self.current_term:
            return  # stale reply from an earlier election attempt; ignore
        if reply.vote_granted:
            self.votes_received.add(peer_id)
            if len(self.votes_received) > total_nodes // 2:
                self.become_leader()

    def become_leader(self):
        self.role = Role.LEADER
        self.leader_id = self.id
        for p in self.peers:
            self.next_index[p] = self.last_log_index() + 1
            self.match_index[p] = 0

    # ------------------------------------------------------------------
    # Log replication
    # ------------------------------------------------------------------
    def client_append(self, command):
        """Only meaningful on the leader: append a new entry to be
        replicated. Returns the new entry's index, or None if not leader."""
        if self.role != Role.LEADER:
            return None
        entry = LogEntry(term=self.current_term, command=command)
        self.log.append(entry)
        return self.last_log_index()

    def make_append_entries(self, peer_id: str) -> AppendEntriesArgs:
        next_idx = self.next_index.get(peer_id, self.last_log_index() + 1)
        prev_index = next_idx - 1
        prev_term = self.term_at(prev_index)
        return AppendEntriesArgs(
            term=self.current_term,
            leader_id=self.id,
            prev_log_index=prev_index,
            prev_log_term=prev_term,
            entries=self.entries_from(next_idx),
            leader_commit=self.commit_index,
        )

    def handle_append_entries(self, args: AppendEntriesArgs) -> AppendEntriesReply:
        # Rule 1: reject anything from a stale leader.
        if args.term < self.current_term:
            return AppendEntriesReply(term=self.current_term, success=False)

        # A valid (current-or-newer term) leader means: I am a follower,
        # my timer resets, and I recognize them as leader. This also
        # covers the case where I was a candidate -- discovering a
        # legitimate leader makes me stand down immediately.
        self.current_term = args.term
        self.role = Role.FOLLOWER
        self.leader_id = args.leader_id
        self.voted_for = args.leader_id
        self.reset_election_timer()

        # Log consistency check: my entry at prev_log_index must match
        # the leader's term there, or I can't safely append after it.
        if args.prev_log_index > 0:
            if args.prev_log_index > self.last_log_index():
                return AppendEntriesReply(term=self.current_term, success=False)
            if self.term_at(args.prev_log_index) != args.prev_log_term:
                return AppendEntriesReply(term=self.current_term, success=False)

        # Append new entries, truncating on the first real conflict
        # (same index, different term = the old entry is not safe to keep).
        insert_at = args.prev_log_index
        for i, entry in enumerate(args.entries):
            idx = insert_at + i + 1
            if idx <= self.last_log_index():
                if self.term_at(idx) != entry.term:
                    self.log = self.log[: idx - 1]
                    self.log.append(entry)
                # else: identical entry already present, nothing to do
            else:
                self.log.append(entry)

        if args.leader_commit > self.commit_index:
            self.commit_index = min(args.leader_commit, self.last_log_index())
            self.apply_committed()

        return AppendEntriesReply(term=self.current_term, success=True)

    def handle_append_entries_reply(
        self, peer_id: str, reply: AppendEntriesReply, args_sent: AppendEntriesArgs, total_nodes: int
    ):
        if reply.term > self.current_term:
            self.become_follower(reply.term)
            return
        if self.role != Role.LEADER:
            return

        if reply.success:
            self.match_index[peer_id] = args_sent.prev_log_index + len(args_sent.entries)
            self.next_index[peer_id] = self.match_index[peer_id] + 1
            self.advance_commit_index(total_nodes)
        else:
            # Simple back-off: try one entry earlier next time.
            # (Real implementations often send a conflict term/index hint
            # so the leader can skip back a whole conflicting term in one
            # round trip instead of one entry at a time -- a nice
            # extension, omitted here for clarity.)
            self.next_index[peer_id] = max(1, self.next_index.get(peer_id, 1) - 1)

    def advance_commit_index(self, total_nodes: int):
        """Find the highest index replicated to a majority and, if that
        entry is from *our own current term*, mark it committed.

        That "own current term" restriction is a subtle but essential
        safety rule (Figure 8 in the Raft paper): a leader must never
        directly commit an entry from an earlier term just because a
        majority happens to hold it, because a future leader election
        could still legally overwrite it. Once a majority holds an entry
        from the *current* term, though, all earlier entries become
        committed transitively (they can no longer be overwritten by any
        future leader, by the log-matching + up-to-date-vote guarantees).
        """
        majority = total_nodes // 2 + 1
        for n in range(self.last_log_index(), self.commit_index, -1):
            if self.term_at(n) != self.current_term:
                continue
            count = 1 + sum(1 for p in self.peers if self.match_index.get(p, 0) >= n)
            if count >= majority:
                self.commit_index = n
                self.apply_committed()
                break

    def apply_committed(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            self.applied.append(self.log[self.last_applied - 1].command)


class SimulatedNetwork:
    """Controls which nodes can currently reach each other. No delays or
    queues -- messages are either delivered this tick or dropped, which
    is all that's needed to simulate partitions and lossy links."""

    def __init__(self, node_ids):
        self.node_ids = set(node_ids)
        self._partitions: Optional[list[set]] = None  # None = fully connected
        self._blocked_links: set[tuple] = set()  # explicit (a, b) drops, direction-sensitive

    def partition(self, *groups):
        """Split the cluster into isolated groups, e.g. partition({'n0'}, {'n1','n2'})."""
        self._partitions = [set(g) for g in groups]

    def heal(self):
        self._partitions = None
        self._blocked_links.clear()

    def drop_link(self, a: str, b: str):
        """Drop messages from a to b specifically (one direction)."""
        self._blocked_links.add((a, b))

    def restore_link(self, a: str, b: str):
        self._blocked_links.discard((a, b))

    def can_deliver(self, sender: str, receiver: str) -> bool:
        if sender == receiver:
            return True
        if (sender, receiver) in self._blocked_links:
            return False
        if self._partitions is None:
            return True
        return any(sender in g and receiver in g for g in self._partitions)


class Cluster:
    """Test/demo harness that owns a set of RaftNodes and a SimulatedNetwork,
    and drives them forward one tick at a time."""

    def __init__(self, n=3, election_timeouts=None):
        self.node_ids = [f"n{i}" for i in range(n)]
        self.network = SimulatedNetwork(self.node_ids)
        self.nodes: dict[str, RaftNode] = {}
        for i, nid in enumerate(self.node_ids):
            timeout = 10 if election_timeouts is None else election_timeouts[i]
            peers = [p for p in self.node_ids if p != nid]
            self.nodes[nid] = RaftNode(nid, peers, election_timeout_ticks=timeout)

    def total_nodes(self) -> int:
        return len(self.node_ids)

    def tick(self):
        # 1. Advance clocks; timed-out nodes start an election.
        for node in self.nodes.values():
            if node.tick() == "timeout":
                self._run_election(node)

        # 2. Any current leader replicates its log / sends heartbeats.
        for node in self.nodes.values():
            if node.role == Role.LEADER:
                self._replicate(node)

    def tick_n(self, k: int):
        for _ in range(k):
            self.tick()

    def _run_election(self, candidate: RaftNode):
        args = candidate.start_election()
        for peer_id in candidate.peers:
            if not self.network.can_deliver(candidate.id, peer_id):
                continue
            peer = self.nodes[peer_id]
            reply = peer.handle_request_vote(args)
            if not self.network.can_deliver(peer_id, candidate.id):
                continue  # reply lost on the way back
            candidate.handle_request_vote_reply(peer_id, reply, self.total_nodes())

    def _replicate(self, leader: RaftNode):
        for peer_id in leader.peers:
            if not self.network.can_deliver(leader.id, peer_id):
                continue
            args = leader.make_append_entries(peer_id)
            peer = self.nodes[peer_id]
            reply = peer.handle_append_entries(args)
            if not self.network.can_deliver(peer_id, leader.id):
                continue  # reply lost
            leader.handle_append_entries_reply(peer_id, reply, args, self.total_nodes())

    def client_write(self, command):
        leader = self.get_leader()
        if leader is None:
            raise RuntimeError("no leader available to accept a write")
        return leader.client_append(command)

    def get_leader(self) -> Optional[RaftNode]:
        leaders = [n for n in self.nodes.values() if n.role == Role.LEADER]
        return leaders[0] if leaders else None

    def all_leaders(self) -> list:
        """Every node currently claiming to be leader -- should never have
        length > 1 for a single term, and in practice never > 1 at all
        once terms are compared, but useful for asserting no split brain."""
        return [n for n in self.nodes.values() if n.role == Role.LEADER]

    def run_until_leader(self, max_ticks=50) -> Optional[RaftNode]:
        for _ in range(max_ticks):
            self.tick()
            leader = self.get_leader()
            if leader is not None:
                return leader
        return None
