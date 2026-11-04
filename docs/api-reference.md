# REST API v1

Except health/readiness/metrics, every request needs a bearer token matching API_TOKEN.
Same-origin deployment is expected; no wildcard CORS headers are emitted. Bodies are
limited to 1 MiB, unknown fields are rejected, and IDs must be UUIDs. Request timeout
is 30 seconds. Long operations return job IDs and run asynchronously.

Responses: `{"data": value}`; errors: `{"error":{"message":"reason"}}`.
Current detail GETs return a zero/one-element array, except file lists and storage overview.
Status codes: 200 accepted/read, 400 invalid input, 401 unauthorized, 404 unknown route,
409 invalid state/storage conflict, 503 readiness unavailable. No data is mocked.

| Method | Path | Behavior |
|---|---|---|
| GET | /health, /ready | Process health / database readiness |
| POST, GET | /sources | Register / list sources |
| GET, DELETE | /sources/{id} | Inspect / delete unreferenced source |
| POST, GET | /backups | Enqueue backup / list history |
| GET, DELETE | /backups/{id} | Inspect / delete inactive snapshot |
| GET | /backups/{id}/files, /backups/{id}/status | Files / snapshot state |
| POST | /backups/{id}/verify | Enqueue integrity scan |
| POST, GET | /restores | Enqueue / list restores |
| GET | /restores/{id}, /restores/{id}/status | Restore job details |
| GET | /recovery-points | Completed retained snapshots |
| GET | /recovery-points/{id}, /recovery-points/{id}/files | Snapshot / files |
| GET | /jobs, /jobs/{id} | Jobs / details including errors and report |
| POST | /jobs/{id}/cancel, /jobs/{id}/retry | Cancel / retry |
| GET | /storage/overview, /storage/deduplication | Logical/verified physical bytes and counts |
| GET | /storage/chunks | Verified chunk metadata with reference counts |
| GET | /analytics/overview | Operational totals |
| GET | /analytics/backups | Daily snapshot counts and bytes |
| GET | /analytics/workers | Worker heartbeat/active status |

All table paths are prefixed `/api/v1`. `/metrics` is outside that prefix and intended
for a private monitoring network. Health endpoints reveal no credentials.

Lists accept `limit` (default 50, maximum 500), `offset` (default 0). Backups/jobs accept
`status`; files accept `prefix`. Return lists are newest first except manifest file order.

Register source:
```json
{"name":"Documents","path":"demo","exclusions":["*.tmp","cache"],"retention":10,"schedule_seconds":0}
```
`schedule_seconds` is zero or at least 60; retention is 1–10000. Paths are relative to
BACKUP_ROOT. The source must already exist. `.` selects the configured root.

Create backup:
```json
{"source_id":"<UUID>","name":"Daily","type":"incremental"}
```
Returns snapshot `id` and `job_id`; type is `full` or `incremental`.

Restore:
```json
{"snapshot_id":"<UUID>","destination":"recovered","path":"documents","conflict":"skip"}
```
`path` may be empty to restore all. Conflict is `skip` (default), `rename`, or
`overwrite`. Destination is relative to RESTORE_ROOT. Returns restore job `id`.

Cancel/retry/verify accept an empty body. A verification report appears on its job.
