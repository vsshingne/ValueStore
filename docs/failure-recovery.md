# Failure and delivery contract

| Failure | Recovery behavior |
|---|---|
| Crash before job transaction commits | Neither job nor outbox becomes visible |
| Kafka unavailable | Outbox stays unpublished; scheduler retries |
| Publish succeeds, DB acknowledgment fails | Event may be published again; claims are idempotent |
| Duplicate event | Only a queued/due retry job can be claimed; terminal/running duplicates are no-ops |
| Worker exits during backup | Lease expires; scheduler fences it and queues another attempt |
| PostgreSQL unavailable | Metadata operations fail; no final manifest commits; expired jobs recover after DB returns |
| MinIO unavailable/interrupted upload | Job retries with backoff; deterministic keys tolerate leftover objects |
| Upload succeeds before metadata commit | Object is reused after verification or swept as an orphan |
| Final DB transaction fails | File rows and snapshot completion roll back together |
| Corrupted chunk | Verification/restore fails with file/hash; corrupt bytes are not published |
| Restore crash | Completed files remain; temporary files may remain; retry applies the selected conflict policy |
| Concurrent GC and backup | Shared/exclusive advisory lock plus live-lease check prevents deleting publishing chunks |

Job state machine: queued → running → completed, or running → retrying → running.
Permanent errors or exhausted attempts become failed and enqueue a `.dead` topic
notification. A user can cancel nonterminal jobs and retry failed/cancelled jobs.
Retries use `2^attempt` seconds, capped at 256 seconds. Both persisted eligibility and
outbox delivery times enforce backoff. Defaults: five attempts, 60-second lease,
20-second renewal, six-hour attempt timeout, two concurrent jobs per worker process.

Kafka offset commit follows the durable outcome (completion or a persisted retry).
Lease recovery also covers a consumer whose partition moved during long processing.
There is no exactly-once side-effect guarantee. An old worker cannot commit metadata
with its fencing token, but a filesystem write already in progress cannot be rolled
back by PostgreSQL. Restrict destination ownership and use deterministic conflict rules.

Unknown event schemas are retained for operator intervention rather than discarded.
Dead-letter events contain a job ID; detailed errors remain in PostgreSQL. A production
Kafka deployment should configure replication, authentication, ACLs and retention.

See the executed-results section of IMPLEMENTATION_STATUS.md. Do not interpret the
failure-design table as proof that every infrastructure outage has been exercised.
