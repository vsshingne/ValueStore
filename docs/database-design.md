# PostgreSQL schema

`migrations/001_initial.sql` is applied once under a migration advisory lock and an
explicit transaction. `schema_migrations` tracks the version. Normal operations use
READ COMMITTED with row locks and advisory locks for the documented invariants.

```mermaid
erDiagram
 sources ||--o{ snapshots : captures
 snapshots ||--o{ files : contains
 snapshots ||--o{ jobs : operates_on
 files ||--o{ file_chunks : orders
 chunks ||--o{ file_chunks : references
 jobs ||--o{ outbox : publishes
 sources {
 uuid id PK
 text path
 jsonb exclusions
 int retention
 int schedule_seconds
 }
 snapshots {
 uuid id PK
 uuid source_id FK
 uuid parent_id FK
 text status
 bytea manifest
 text checksum
 bigint logical_bytes
 }
 jobs {
 uuid id PK
 uuid snapshot_id FK
 text kind
 text status
 uuid token
 timestamptz lease_until
 timestamptz next_attempt_at
 jsonb payload
 jsonb report
 }
 files {
 uuid id PK
 uuid snapshot_id FK
 text path
 bigint size
 text hash
 }
 file_chunks {
 uuid file_id PK,FK
 int ordinal PK
 text chunk_hash FK
 }
 chunks {
 text hash PK
 bigint size
 text storage_key UK
 }
 outbox {
 uuid id PK
 uuid job_id FK
 text topic
 jsonb payload
 timestamptz published_at
 }
```

Additional tables: `workers` (heartbeat registry), `audit_logs` (sensitive operation
history). There are no users because the implementation is single-operator.

Foreign keys prevent deleting referenced chunks. `(snapshot_id,path)` is unique;
`(file_id,ordinal)` preserves order. Completed snapshots require non-null manifest,
checksum and completion time. Parent deletion clears the relational parent pointer;
the original parent ID remains in the child's immutable manifest for provenance.

The application database identity must not be exposed to end users. Direct SQL can
bypass application-level lifecycle rules; limit operational access and back up the
database independently. API deletes record audit entries and use reference-safe GC.
