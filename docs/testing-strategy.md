# Testing strategy

`go test ./...` runs infrastructure-independent tests for chunk boundaries, binary/empty
data, cancellation, callback failures, manifest corruption, portable path validation,
file reconstruction, interrupted downloads with successful retries, corruption/repair,
concurrent reconstruction, scanning exclusions, authentication and request decoding.
In-memory objects are test doubles only; production metadata is always PostgreSQL.

`go test -race ./...` requires a 64-bit C toolchain on Windows. The implementation
session used a workspace-local LLVM MinGW compiler after the installed GCC rejected
64-bit mode. Record both that initial failure and the subsequent successful run.

## Real service tests

```sh
python scripts/setup.py
docker compose up -d --wait postgres kafka minio
python scripts/integration.py
```

The script recreates only the explicitly named `vaultstore_test` database. Never point
it at production infrastructure. Test data uses a separate bucket and Kafka topic.
Tests are build-tagged; requesting them without their environment fails rather than
silently skipping. Compilation alone (`-run '^$'`) is not integration execution.

The system suite covers PostgreSQL persistence, actual Kafka publication/consumption,
outbox broker failure, duplicate processing, full/incremental snapshots, deleted paths,
restore of original bytes, expired lease fencing, concurrent backups/GC, corruption
detection/repair, and deletion of a parent without breaking child recovery.
The PostgreSQL suite separately verifies rollback, backoff, claims and expired tokens.

The Compose demo `--crash` kills a real worker process and requires successful retry.
Fault tests must check the recovered data/state, not only an error return. Real network
partition/DB restart/MinIO process outage testing still needs broader infrastructure
fault orchestration; existing unit faults do not replace those exercises.

Frontend checks: `npm run lint`, `npm run typecheck`, `npm test`, `npm run build`.
Tests exercise analytics definitions and empty-storage cases. Automated browser
workflow coverage should be expanded before production rollout.
