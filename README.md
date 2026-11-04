# VaultStore

Distributed backup, deduplication and recovery engine built with Go, PostgreSQL,
Kafka, MinIO and React. The API schedules work; workers stream files into SHA-256
content-addressed chunks and publish complete, immutable recovery manifests.

**Verification status:** see [IMPLEMENTATION_STATUS.md](IMPLEMENTATION_STATUS.md).
This is a working implementation under validation, not a claim of production
certification or a deployed service.

## Run locally

Requires Docker Compose v2 and Python 3. Host development uses Go 1.26+ and Node 24.

```sh
python scripts/setup.py
docker compose up -d --build
```

Open `http://localhost:3000`, go to Settings, and enter `API_TOKEN` from your local
`.env`. Register `demo` as a source path. Create a full backup, inspect its completed
recovery point, and start a restore to a relative destination such as `recovered`.

API: `http://localhost:8080`; MinIO console: `http://localhost:9001`; Prometheus:
`http://localhost:9090`; Grafana: `http://localhost:3001` (user `admin`, password in
`.env`). All host ports bind to loopback. No credentials are checked into source.

```sh
python scripts/demo.py
python scripts/demo.py --crash
python scripts/integration.py
```

The demo proves unchanged-content reuse, modified/deleted-file snapshots, restoration
of an earlier snapshot, and verification. `--crash` kills and restarts the Compose
worker and checks that its attempt count increases. It fails if the backup completes
before the crash can be injected; increase sample size in that case.

## Implemented behavior

- Full and incremental snapshots of regular files and directories, including empty files.
- Configurable streaming fixed-size chunks, SHA-256 chunk/file/manifest verification.
- Cross-file and cross-snapshot deduplication with per-hash PostgreSQL locks.
- PostgreSQL normalized file/chunk references and transactional job/outbox creation.
- Kafka consumer groups, explicit offset commits, fenced leases, retries and dead letters.
- Expired worker recovery, cancellation, manual retry, interval schedules.
- Restore selection by file/directory; skip, deterministic rename, explicit overwrite.
- Explicit verification jobs, retention by snapshot count, orphan reconciliation.
- Authenticated versioned API, restricted filesystem roots and traversal-resistant I/O.
- Live React console, Prometheus metrics and provisioned Grafana dashboard.

## Repository

```text
cmd/                  api, worker, scheduler; devdb test database helper
internal/
  api/                authenticated HTTP routes
  app/ config/        service lifecycle and environment validation
  chunking/           fixed chunking, SHA-256 and object keys
  engine/             scanning, deduplication, backup, restore, verification, GC
  jobs/               Kafka, outbox, leases, retry and scheduling
  metadata/           pgx transactions and immutable manifest publication
  model/ safepath/    schemas, checksums and restricted filesystem access
  storage/            MinIO object adapter
migrations/           embedded, versioned SQL schema
web/                  React / TypeScript / Vite console
tests/integration/    real PostgreSQL/Kafka/MinIO tests
deployments/          Kubernetes, Prometheus, Grafana
scripts/              setup, demo, integration and local verification helpers
docs/                 detailed architecture and operational contracts
```

## Architecture and consistency

The API and scheduler are the control plane. Workers are the data plane. PostgreSQL
is authoritative for metadata; MinIO holds chunk bytes. Kafka is an at-least-once
notification transport. There is **no distributed atomic transaction** between
PostgreSQL and MinIO, and execution is **not exactly once**.

An upload is read back and hashed before its metadata is committed. All snapshot
references and the completed state commit together behind a lease fencing check.
Collection is excluded while workers publish snapshots and while live leases exist.
Completed snapshots are self-contained: deleting a parent does not destroy the
child's ability to restore.

See [architecture](docs/architecture.md), [storage design](docs/storage-design.md),
[database ER diagram](docs/database-design.md), [backup and restore](docs/backup-and-restore.md),
and [failure recovery](docs/failure-recovery.md).

## API and development

All data endpoints require `Authorization: Bearer <API_TOKEN>`. Responses use
`{"data":...}` or `{"error":{"message":"..."}}`. Lists accept `limit` (1–500) and
`offset`. See the [endpoint reference](docs/api-reference.md).

```sh
go test ./...
go test -race ./...
go vet ./...
go build ./cmd/...
cd web
npm ci
npm run lint
npm run typecheck
npm test
npm run build
```

For host development, set the environment values described in `.env.example`, start
the infrastructure services, and run each of `go run ./cmd/api`, `go run ./cmd/worker`
and `go run ./cmd/scheduler` in separate terminals. Give worker and scheduler unique
`LISTEN_ADDR` ports. `npm run dev` proxies `/api` to `localhost:8080`.

See [test strategy](docs/testing-strategy.md), [performance](docs/performance.md),
and [Docker/Kubernetes deployment](docs/deployment.md).

## Boundaries and future work

- Single trusted operator using a bearer token; no users, passwords, RBAC or tenant isolation.
- Local mounted sources only; workers must see identical source and restore mounts.
- No compression, encryption-at-rest layer, cloud source integrations, CDC or filesystem snapshot provider.
- A live directory scan cannot guarantee application-consistent point-in-time contents.
  Quiesce writers or back up a filesystem snapshot. File size/mtime changes during reads fail the attempt.
- Fixed-size chunks may deduplicate poorly after insertions. Incrementals rehash all files for correctness.
- Manifests are in memory with a hard 100,000 combined file/chunk-entry cap. Split larger sources.
- Collection uses a conservative global lock and may wait for long backups/restores.
- Restore preserves permission bits but not ACLs, xattrs, ownership, sparse extents or original timestamps.
  Symlinks and special files are rejected. Hard links restore as independent files.
- Restore publication is atomic per file on supported filesystems, not across a directory tree.
  Abrupt termination may leave `.vaultstore-*` temporary files; inspect and clean them while workers are stopped.
- Dashboard storage totals describe verified metadata, not an instantaneous bucket billing inventory.
- Kubernetes YAML requires external stateful services, RWX storage and a supplied Secret.
- Kafka lag, full distributed traces, multi-broker failover and object-version recovery are future work.

Production hardening priorities are a snapshot-provider integration, tenant authorization,
TLS/SASL Kafka transport, managed secrets, distributed load/failure testing, streaming
manifest persistence, and object storage versioning/replication. Protect PostgreSQL
metadata separately: chunks alone do not contain the recovery manifests.
