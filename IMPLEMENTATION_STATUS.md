# VaultStore implementation status

Updated 2026-11-03 (Asia/Calcutta). Initially empty workspace. Functional implementation;
Docker deployment and production certification are not claimed.

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

## Phase ledger

| Phase | Files / completed behavior | Verification |
|---|---|---|
| 1 | config, migrations, architecture | Migration executed on PostgreSQL 17.6 |
| 2 | chunking, MinIO adapter, safepath | Empty/binary/boundary/cancel/hash tests pass |
| 3 | pgx repository, chunk locks/references | Real DB rollback/claims/fencing and concurrent dedup pass |
| 4 | scanner, full/complete incremental manifests | Full/incremental/deletion and previous recovery point tests pass |
| 5 | Kafka/outbox, leases, retry, scheduling | Actual publication/consumption; killed backup worker recovers on attempt 2 |
| 6 | restore selection, root confinement, hashes, conflicts | Original bytes and skip/rename/overwrite tests pass |
| 7 | verification, retention, orphan/reference GC | Corruption/repair, parent deletion, concurrent GC pass |
| 8 | React routes/forms/charts/live API | Build/typecheck/lint/2 tests pass; browser check in progress |
| 9 | Docker, Compose, Kubernetes, metrics, Grafana, CI | 10 K8s resources pass strict schemas; no Docker runtime |
| 10 | reliability tests/demo/docs | Extended real-service suite passes; restore process-crash test running |

## Executed results

- `go test ./...`, `go vet ./...`, `go build ./cmd/...`: PASS.
- `go test -race ./...`: PASS including latest consumer-startup changes, using a
  workspace-local 64-bit LLVM MinGW compiler. Installed 32-bit GCC initially failed.
- PostgreSQL suite: PASS (transaction rollback, exclusive claims, backoff, stale and
  expired lease tokens, durable retry outbox).
- PostgreSQL/Kafka/MinIO system suite: PASS, 12 subtests, latest package time 2.981 s.
  Includes full/incremental/deletion, duplicate messages, original-data restore,
  expired leases, concurrent workers/GC, upload failure before/after object persistence,
  failed manifest transaction with successful retry, all conflict policies,
  corruption/repair and parent deletion.
- HTTP demo against compiled API/scheduler/worker: PASS for full/unchanged/modified/
  deleted snapshots, earlier restore, verification, and actual backup worker termination.
  Replacement completed on attempt 2. Restore-worker crash check is in progress.
- Frontend production build, typecheck, lint: PASS. Vitest: 2 PASS. Full npm audit: 0 vulnerabilities.
- Kubernetes: 10 resources validate against strict v1.32 JSON schemas. Compose and
  monitoring YAML parse; Grafana JSON parses. No cluster rollout was executed.
- Benchmark: 728.80 MB/s, 14,387,749 ns/op, 4,340,675 B/op, 11 allocs/op.
  Windows amd64, i5-1335U, Go 1.26.10, 10 MiB in-memory input, 4 MiB chunk size.

## Failures found and fixed

- Root path `.` incorrectly rejected; fixed and path tests pass.
- Incomplete initial dependency checksums; go mod tidy resolved them.
- Kafka auto-topic metadata race: services now create/wait for topics before consumers
  join, watch partition changes, and scope consumer groups to the topic. Publisher
  retries leave the outbox durable.
- Consumer exit previously left a heartbeat without a reader; fatal reader errors now
  terminate the process for restart.
- Retry time is persisted in jobs as well as the outbox, so duplicates cannot bypass backoff.
- GC checks live leases as well as advisory locks, covering loss of a lock connection.
- Action request bodies are consumed/validated. An early HTTP demo hit a reset; the
  subsequent full run passed after body handling and consumer startup fixes.
- Frontend dependency advisories resolved by updates and a patched minimatch override.
  Bundle split into application, React and chart chunks.

## Environment and deployment limits

Docker is absent; WSL cannot start. Native PostgreSQL 17.6, Kafka 3.9.1 (Java 17),
and MinIO release 2025-04-22 built from source were used. The MinIO Windows binary
download returned HTTP 410; source build succeeded. Local Node 24 and LLVM compiler
are under ignored `.runtime`; system installs were not replaced. Secrets remain in
ignored `.env` and were not printed.

## Exact next steps

1. Collect native demo restore-crash result and browser smoke; inspect screenshots.
2. Finalize results and documentation; stop owned preview/native services.
3. On a Docker-capable host, run Compose, `scripts/integration.py` and `scripts/demo.py --crash`.
4. Run GitHub Actions and a Kubernetes rollout with external services, secrets and RWX storage.

## Scope boundaries

Single operator; mounted local sources; fixed chunking; complete manifests capped at
100,000 combined file/chunk entries. No atomic live-filesystem snapshots, compression,
cloud connectors, RBAC, encryption layer, Kafka TLS/SASL configuration, Kafka lag
exporter, streaming manifest persistence or HA topology. GC is globally serialized.
Restore is atomic per file where supported and can leave temporary files after crashes.
Real infrastructure outages beyond worker crashes remain to be exercised; injected
storage/broker faults are labeled accordingly. See README and docs for details.
