# *VernKV* (Version v0.1)

[![Go Reference](https://pkg.go.dev/badge/github.com/ver1619/VernKV.svg?style=flat-square)](https://pkg.go.dev/github.com/ver1619/VernKV)
![Go Version](https://img.shields.io/badge/Go-1.22%2B-1f2937?logo=go&logoColor=00ADD8&style=flat-square)
![License](https://img.shields.io/github/license/ver1619/VERN_v0.1?label=license&color=yellow)

<p align="center">
  <img 
    src="https://github.com/user-attachments/assets/43ce00ec-3c74-4f7e-a5ce-0f5b4019f1f7"
    alt="VernKV logo"
    width="300"
  >
</p>

***VernKV*** is a correctness-first, log-structured key-value storage engine written in Go.

It is designed as an **embedded key-value engine**, not a database server, with a focus on fundamental storage guarantees required by modern storage systems: durability, crash safety, and deterministic recovery.


## Motivation

Modern databases depend on storage engines that preserve correctness in the
presence of crashes, partial writes, and system failures.

VernKV was built to:
- study these guarantees in depth
- implement them from first principles
- avoid premature optimization
- prioritize clarity and correctness over performance

The project follows **LSM-tree design principles**, using append-only writes,
immutable on-disk files, and explicit ordering to make crash recovery
predictable and verifiable.

## What VernKV Is NOT?

- Not a database server
- Not distributed
- Not a CLI tool
- Not optimized for throughput or latency
- Not production-ready

VernKV intentionally trades peak performance for simpler, easier-to-understand
design rules and predictable recovery behavior. Early versions avoid complex
features that make correctness harder to reason about, focusing instead on
clear, reliable behavior.

## Core Guarantees (v0.1)

VernKV v0.1 provides the following guarantees:

- **Durability**  
  All writes are appended to a write-ahead log (WAL) and fsynced before becoming visible.

- **Crash Safety**  
  The engine can recover to a consistent state after crashes at any point during
  writes or flushes.

- **Deterministic Recovery**  
  WAL replay is idempotent and deterministic; the same WAL always produces the same state.

- **Correct Read Semantics**  
  Reads resolve conflicts using sequence numbers and respect tombstones(Deletes), ensuring
  correct handling of overwrites and deletions.

- **Immutable On-Disk State**  
  SSTables are written once and never modified.

## Explicit Non-Goals (v0.1)

The following are intentionally out of scope for v0.1:

- Compaction
- WAL truncation
- Bloom filters
- Range scans / iterators
- Concurrency beyond a single writer
- Transactions
- Replication
- CLI interface

These features may be explored in later versions.




