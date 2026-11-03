# VaultStore implementation status

Started 2026-11-02. Empty workspace; no existing files to preserve.

## Plan
1. Foundation: architecture, configuration, SQL migrations.
2. Streaming storage: fixed chunks, hashes, MinIO, unit tests.
3. PostgreSQL metadata and concurrent deduplication.
4. Full/incremental immutable snapshots.
5. Kafka outbox, fenced job leases, retry/recovery.
6. Safe, verified restores.
7. Verification, retention, garbage collection.
8. React dashboard using actual API data.
9. Metrics, containers, Kubernetes, CI.
10. Reliability tests, benchmarks, documentation and demo.

## Environment
Windows / PowerShell; Go and Node installed. Docker not found on PATH.
Infrastructure-dependent checks must be reported separately from executed tests.

## Current work
Phase 1 in progress. Implementing a modular backend with API, worker and scheduler
executables. PostgreSQL is the sole production metadata authority. Kafka provides
at-least-once notifications; PostgreSQL leases fence duplicate execution. Immutable
complete manifests make each recovery point independently restorable.

## Next steps
Implement configuration, migrations and streaming storage; run foundation tests.
