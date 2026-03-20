# Raft Consensus Protocol

A Go implementation of the [Raft distributed consensus algorithm](https://raft.github.io/raft.pdf), built as part of a distributed systems course (based on MIT 6.824). Raft enables a cluster of servers to agree on a shared log of commands, even in the presence of network partitions and server crashes.

## What it does

This implementation covers the core Raft protocol:

**Leader Election** — Servers elect a single leader through randomized election timeouts and majority voting. If a leader fails, the remaining servers detect the failure and elect a new one. Split votes are resolved by randomized retry.

**Log Replication** — The leader accepts client commands, appends them to its log, and replicates them to followers via AppendEntries RPCs. Once a majority of servers have stored an entry, the leader commits it and notifies followers. Followers that fall behind are brought up to date through log backtracking.

**Persistence** — Each server persists its current term, vote, and log to stable storage. After a crash and restart, a server recovers its state and rejoins the cluster without data loss.

**Safety** — The implementation enforces the election restriction from Figure 2 of the Raft paper: a candidate's log must be at least as up-to-date as a majority of the cluster before it can win an election. This guarantees that committed entries are never lost.

## Project structure

```
raft-project/
├── go.mod
├── raft/
│   ├── raft.go          # Core implementation: election, replication, persistence
│   ├── config.go        # Test harness: cluster setup, network simulation
│   ├── persister.go     # In-memory persistence layer
│   ├── util.go          # Debug logging utility
│   └── test_test.go     # Full test suite
├── labrpc/
│   ├── labrpc.go        # Simulated RPC framework with network partitioning
│   └── test_test.go
└── labgob/
    ├── labgob.go        # GOB encoding wrapper with safety checks
    └── test_test.go
```

## Running the tests

```bash
cd raft
go test -v
```

The test suite covers initial elections, re-elections after network failure, basic and concurrent log agreement, leader partitioning and rejoin, log backup over incorrect follower logs, persistence through crashes, the Figure 8 scenario from the Raft paper, and behavior under unreliable networks with message loss and reordering.

To run a specific test:

```bash
go test -v -run TestInitialElection4A
go test -v -run TestBasicAgree4B
go test -v -run TestFigure84B
```

## Design notes

The implementation uses a single `ticker()` goroutine per server that manages state transitions between follower, candidate, and leader roles. Key design choices:

- **Heartbeat interval**: 100ms, with election timeouts randomized between 300–500ms
- **Log backtracking**: On AppendEntries rejection, the leader decrements `nextIndex` and retries, converging on the correct match point
- **Commit advancement**: The leader tracks `matchIndex` across followers and advances `commitIndex` once a majority has replicated an entry for the current term

The simulated RPC layer (`labrpc`) supports toggling network partitions, message loss, delays, and reordering — making it possible to test edge cases that are hard to reproduce on a real network.

## Built with

Go, `encoding/gob` for serialization, and the `labrpc` simulated network framework.
