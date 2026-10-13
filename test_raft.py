"""
Test suite for the Raft implementation in raft.py.

Each test is written to demonstrate one specific correctness property or
edge case from the Raft paper -- read the docstrings even if you don't
read the assertions, they're the "why does this matter" for each test.

Run with: pytest test_raft.py -v
"""

import pytest
from raft import Cluster, Role, LogEntry, RaftNode, RequestVoteArgs


# ----------------------------------------------------------------------
# Leader election
# ----------------------------------------------------------------------

def test_single_leader_elected():
    """The most basic property: given enough ticks, exactly one node
    becomes leader and the rest remain followers."""
    c = Cluster(3, election_timeouts=[5, 20, 20])
    leader = c.run_until_leader()

    assert leader is not None
    leaders = c.all_leaders()
    assert len(leaders) == 1
    followers = [n for n in c.nodes.values() if n.role == Role.FOLLOWER]
    assert len(followers) == 2


def test_votes_are_one_per_term():
    """A server must cast at most one vote per term. If a second
    candidate asks for a vote in the same term after the first already
    got it, the request must be rejected."""
    n = RaftNode("voter", ["a", "b"])
    args_a = RequestVoteArgs(term=1, candidate_id="a", last_log_index=0, last_log_term=0)
    args_b = RequestVoteArgs(term=1, candidate_id="b", last_log_index=0, last_log_term=0)

    reply_a = n.handle_request_vote(args_a)
    reply_b = n.handle_request_vote(args_b)

    assert reply_a.vote_granted is True
    assert reply_b.vote_granted is False  # already committed this term's vote to "a"
    assert n.voted_for == "a"


def test_candidate_with_stale_log_loses_election():
    """The up-to-date check: a voter must refuse a candidate whose log is
    behind its own, even if the candidate's term number is fine.
    Otherwise a leader could be elected that would silently lose
    already-committed entries."""
    voter = RaftNode("voter", ["candidate"])
    voter.log = [LogEntry(term=1, command="x=1"), LogEntry(term=1, command="x=2")]
    voter.current_term = 1

    # Candidate's log is shorter at the same last-log-term -> not up to date.
    stale_candidate_args = RequestVoteArgs(
        term=2, candidate_id="candidate", last_log_index=1, last_log_term=1
    )
    reply = voter.handle_request_vote(stale_candidate_args)

    assert reply.vote_granted is False


def test_election_requires_true_majority():
    """With 5 nodes, a candidate that can only reach 2 peers (himself +
    1 other = 2 votes total) must NOT become leader -- 2 is not a
    majority of 5 (needs 3)."""
    c = Cluster(5, election_timeouts=[5, 20, 20, 20, 20])
    candidate = c.nodes["n0"]

    # Only n0 and n1 can hear each other; n2, n3, n4 are unreachable from n0.
    c.network.partition({"n0", "n1"}, {"n2"}, {"n3"}, {"n4"})
    c.tick_n(6)  # let n0's timer fire and run one election attempt

    assert candidate.role != Role.LEADER
    assert c.get_leader() is None


# ----------------------------------------------------------------------
# Log replication
# ----------------------------------------------------------------------

def test_client_write_replicates_to_all_and_commits():
    """After a leader is elected, a client write should end up
    replicated in every node's log, committed, and applied -- with
    identical logs across the cluster."""
    c = Cluster(3, election_timeouts=[5, 20, 20])
    c.run_until_leader()

    c.client_write("x=1")
    c.client_write("y=2")
    c.tick_n(5)

    logs = [[(e.term, e.command) for e in n.log] for n in c.nodes.values()]
    assert all(log == logs[0] for log in logs)
    assert logs[0] == [(1, "x=1"), (1, "y=2")]
    assert all(n.commit_index == 2 for n in c.nodes.values())
    assert all(n.applied == ["x=1", "y=2"] for n in c.nodes.values())


def test_write_rejected_without_a_leader():
    """A client write can't go anywhere if no leader currently exists --
    this should fail loudly, not silently drop the command."""
    c = Cluster(3)  # no ticks yet: everyone is still a follower
    with pytest.raises(RuntimeError):
        c.client_write("x=1")


def test_conflicting_follower_entry_gets_overwritten():
    """Simulates a follower that has a stale, uncommitted, conflicting
    entry at some index (e.g. from a leader that crashed before
    replicating it anywhere else). Once a legitimate new leader with a
    different entry at that index replicates to this follower, the
    follower's conflicting entry -- and everything after it -- must be
    discarded and replaced, not merged or kept."""
    c = Cluster(3, election_timeouts=[5, 20, 20])
    leader = c.run_until_leader()

    # Manually seed n1 with a bogus, conflicting entry at index 1
    # (term 0, i.e. "older" than anything the real leader will write).
    follower = c.nodes["n1"]
    follower.log = [LogEntry(term=0, command="BOGUS")]

    c.client_write("real-command")
    c.tick_n(5)

    assert [ (e.term, e.command) for e in follower.log ] == [(1, "real-command")]
    assert follower.applied == ["real-command"]


# ----------------------------------------------------------------------
# Split-brain prevention / partitions / stale leaders
# ----------------------------------------------------------------------

def test_partition_minority_leader_cannot_progress():
    """Classic split-brain scenario: the current leader gets isolated
    into a minority partition. It should be unable to commit any new
    writes (no majority to acknowledge them), while the majority
    partition is free to elect a new leader and keep making progress."""
    c = Cluster(3, election_timeouts=[5, 20, 20])
    old_leader = c.run_until_leader()
    assert old_leader.id == "n0"

    others = [nid for nid in c.node_ids if nid != old_leader.id]
    c.network.partition({old_leader.id}, set(others))

    # Old leader (alone) tries to accept a write -- it will append
    # locally but can never reach a majority to commit it.
    old_leader.client_append("orphaned-write")
    c.tick_n(3)
    assert old_leader.commit_index == 0

    # Meanwhile, force one of the majority-side followers to time out
    # and win an election within its own partition.
    c.nodes[others[0]].election_elapsed = c.nodes[others[0]].election_timeout_ticks
    c.tick_n(15)

    new_leader = [c.nodes[nid] for nid in others if c.nodes[nid].role == Role.LEADER]
    assert len(new_leader) == 1
    assert new_leader[0].current_term > old_leader.current_term


def test_stale_leader_steps_down_after_partition_heals():
    """Continuation of the above: once the network heals, the old
    leader (still on a lower term, still sending heartbeats) must
    discover the higher term and immediately step down to follower --
    this is the term-comparison reflex, not a negotiation."""
    c = Cluster(3, election_timeouts=[5, 20, 20])
    old_leader = c.run_until_leader()
    old_term = old_leader.current_term

    others = [nid for nid in c.node_ids if nid != old_leader.id]
    c.network.partition({old_leader.id}, set(others))
    c.nodes[others[0]].election_elapsed = c.nodes[others[0]].election_timeout_ticks
    c.tick_n(15)
    new_leader = next(c.nodes[nid] for nid in others if c.nodes[nid].role == Role.LEADER)
    assert new_leader.current_term > old_term

    c.network.heal()
    c.tick_n(5)  # old leader's next heartbeat reaches someone on the new term

    assert old_leader.role == Role.FOLLOWER
    assert old_leader.current_term >= new_leader.current_term
    # And there is exactly one leader cluster-wide again.
    assert len(c.all_leaders()) == 1


def test_no_two_leaders_in_same_term():
    """A direct check of the safety property split-brain prevention
    exists to guarantee: scan every node's history of terms it has been
    leader for (approximated here by re-running elections under a
    partition/heal cycle) and confirm no term ever has two leaders."""
    c = Cluster(3, election_timeouts=[5, 20, 20])
    c.run_until_leader()

    # Force a leadership change under partition, then heal, several times.
    for _ in range(3):
        leader = c.get_leader()
        if leader is None:
            c.tick_n(5)
            continue
        others = [nid for nid in c.node_ids if nid != leader.id]
        c.network.partition({leader.id}, set(others))
        c.nodes[others[0]].election_elapsed = c.nodes[others[0]].election_timeout_ticks
        c.tick_n(15)
        c.network.heal()
        c.tick_n(10)

    # However things shook out, group current leaders by term.
    leaders_by_term = {}
    for n in c.nodes.values():
        if n.role == Role.LEADER:
            leaders_by_term.setdefault(n.current_term, []).append(n.id)
    assert all(len(ids) == 1 for ids in leaders_by_term.values())


# ----------------------------------------------------------------------
# Follower catch-up
# ----------------------------------------------------------------------

def test_lagging_follower_catches_up_after_reconnecting():
    """A follower that was cut off while several entries were committed
    should, once reconnected, receive all the entries it missed via
    repeated AppendEntries (next_index backing up / advancing), ending
    with an identical log to everyone else."""
    c = Cluster(3, election_timeouts=[5, 20, 20])
    c.run_until_leader()

    lagging = "n2"
    c.network.partition({"n0", "n1"}, {lagging})

    c.client_write("a")
    c.client_write("b")
    c.client_write("c")
    c.tick_n(5)

    assert len(c.nodes[lagging].log) == 0  # cut off, received nothing
    assert c.nodes["n0"].commit_index == 3  # majority (n0+n1) still committed fine

    c.network.heal()
    c.tick_n(10)

    caught_up_log = [(e.term, e.command) for e in c.nodes[lagging].log]
    assert caught_up_log == [(1, "a"), (1, "b"), (1, "c")]
    assert c.nodes[lagging].applied == ["a", "b", "c"]
