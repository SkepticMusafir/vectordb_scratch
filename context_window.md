# AI assistant instructions — Raft implementation project

Drop this file in the project root. For Claude Code, copy/rename it to
`CLAUDE.md`; for Cursor, `.cursorrules`; for others, point the assistant
at it directly at the start of a session. Read this file in full before
doing any work in this repo.

## Context

This project is a Raft consensus implementation. It started as an
educational, in-memory simulation (single process, synchronous fake
network) to learn the algorithm. It is now moving toward a real,
deployable implementation. Treat these as two different codebases that
happen to share an ancestor — don't assume the simulation's shortcuts
carry forward.

## 1. Ship in small increments — wait for the go-ahead

Do not generate a large batch of code in one response. Propose and
build **one module, one file, or one coherent unit of work at a time**,
then stop and wait for explicit confirmation ("continue", "looks good",
"next") before starting the next unit.

- When starting a task, state the plan as a numbered list of increments
  before writing any code.
- After completing an increment, stop. Summarize what changed, what you
  did *not* yet do, and what you'd tackle next. Do not proceed on your
  own judgment that the rest is "obvious" or "quick."
- If a single increment is unavoidably large (e.g. it's genuinely one
  atomic change), say so explicitly and explain why it can't be split,
  rather than splitting arbitrarily or silently going big.
- "Hold as long as possible" means: default to the smallest useful
  increment, not the largest one you can justify.

## 2. Build for production, not for a classroom

No implementation should be accepted as "done" just because it
demonstrates the algorithm correctly in a happy-path test. Concretely,
production-grade means:

- **Persistence**: `currentTerm`, `votedFor`, and the log must survive a
  process crash and restart (write-ahead to disk before responding to
  RPCs that depend on them, per the Raft paper's durability
  requirements).
- **Real networking**: actual RPC over a real transport (gRPC, HTTP,
  raw TCP framing — pick one and justify it), with real timeouts,
  retries, and handling of partial/duplicate delivery — not
  direct in-process function calls.
- **Concurrency correctness**: real concurrent access (goroutines,
  threads, or async tasks) with explicit reasoning about what's
  protected by what lock/actor boundary — not single-threaded stepping.
- **Log compaction / snapshotting**: an unbounded log is not
  acceptable for anything long-running.
- **Cluster membership changes**: joint consensus or single-server
  changes for adding/removing nodes, if the project's scope includes
  reconfiguration.
- **Observability**: structured logging of role transitions, elections,
  and commit progress; metrics for leader stability, commit latency,
  and election frequency.
- **Error handling**: no bare `except: pass` / swallowed errors; every
  failure mode (disk write failure, malformed RPC, clock skew) has a
  defined, tested behavior.
- **Testing**: unit tests plus scenario tests for partitions, crashes,
  and message loss — including tests that inject faults *during*
  replication and recovery, not just before/after.
- If a shortcut is taken for expedience, flag it explicitly in code
  comments and in the progress log (see below) — don't let a
  simulation-era shortcut quietly persist uncommented.

## 3. Orient before you touch anything

At the start of every session, before writing or editing code:

- List and skim the current project directory structure.
- Re-read `PROGRESS.md` (see below) to recall where things stand.
- Ask the user what they want to focus on this session, rather than
  assuming the next item on the backlog is what they want worked on.
  A short check-in question is expected, not optional: e.g. "Last
  session ended with log compaction half-implemented and networking
  untouched — what's the priority today?"

## 4. Track progress and end with a picture

Maintain a `PROGRESS.md` file in the project root, updated at the end
of every session with an entry in this format:

```markdown
## Session N — YYYY-MM-DD

**Done:**
- ...

**Next iteration should tackle:**
- ...

**Open questions / decisions needed:**
- ...
```

Additionally, end every session that changed the architecture or state
machine with a visualization (a diagram of current component
structure, or a state/sequence diagram of whatever subsystem changed)
so progress is visible at a glance, not just described in prose.
