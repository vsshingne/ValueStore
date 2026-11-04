CREATE TABLE IF NOT EXISTS sources (
 id uuid PRIMARY KEY, name text NOT NULL CHECK(length(name) BETWEEN 1 AND 200), path text NOT NULL,
 exclusions jsonb NOT NULL DEFAULT '[]', retention integer NOT NULL DEFAULT 10 CHECK(retention>0),
 schedule_seconds integer NOT NULL DEFAULT 0 CHECK(schedule_seconds=0 OR schedule_seconds>=60),
 next_run timestamptz, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE snapshots (
 id uuid PRIMARY KEY, source_id uuid NOT NULL REFERENCES sources(id), name text NOT NULL,
 type text NOT NULL CHECK(type IN ('full','incremental')), parent_id uuid REFERENCES snapshots(id) ON DELETE SET NULL,
 status text NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','completed','failed','cancelled')),
 created_at timestamptz NOT NULL DEFAULT now(), completed_at timestamptz,
 manifest bytea, checksum text, logical_bytes bigint NOT NULL DEFAULT 0,
 CHECK(status<>'completed' OR (manifest IS NOT NULL AND checksum IS NOT NULL AND completed_at IS NOT NULL))
);
CREATE TABLE jobs (
 id uuid PRIMARY KEY, kind text NOT NULL CHECK(kind IN ('backup','restore','verify')),
 snapshot_id uuid NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
 status text NOT NULL DEFAULT 'queued' CHECK(status IN ('queued','running','retrying','completed','failed','cancelled')),
 payload jsonb NOT NULL DEFAULT '{}', attempt integer NOT NULL DEFAULT 0, token uuid,
 lease_until timestamptz, next_attempt_at timestamptz NOT NULL DEFAULT now(), worker_id text, progress bigint NOT NULL DEFAULT 0, error text,
 report jsonb, created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX jobs_recovery ON jobs(status,lease_until);
CREATE INDEX jobs_snapshot ON jobs(snapshot_id);
CREATE INDEX snapshots_source ON snapshots(source_id,created_at DESC);
CREATE TABLE outbox (
 id uuid PRIMARY KEY, job_id uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
 topic text NOT NULL, payload jsonb NOT NULL, available_at timestamptz NOT NULL DEFAULT now(), published_at timestamptz
);
CREATE INDEX outbox_pending ON outbox(available_at) WHERE published_at IS NULL;
CREATE TABLE chunks (hash text PRIMARY KEY CHECK(hash ~ '^[0-9a-f]{64}$'), size bigint NOT NULL CHECK(size>0), storage_key text NOT NULL UNIQUE, verified_at timestamptz NOT NULL DEFAULT now(), created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE files (
 id uuid PRIMARY KEY, snapshot_id uuid NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
 path text NOT NULL, size bigint NOT NULL CHECK(size>=0), hash text NOT NULL, directory boolean NOT NULL DEFAULT false,
 UNIQUE(snapshot_id,path)
);
CREATE TABLE file_chunks (
 file_id uuid NOT NULL REFERENCES files(id) ON DELETE CASCADE, ordinal integer NOT NULL CHECK(ordinal>=0),
 chunk_hash text NOT NULL REFERENCES chunks(hash) ON DELETE RESTRICT, PRIMARY KEY(file_id,ordinal)
);
CREATE INDEX chunk_references ON file_chunks(chunk_hash);
CREATE TABLE workers (id text PRIMARY KEY, heartbeat timestamptz NOT NULL, active integer NOT NULL DEFAULT 0);
CREATE TABLE audit_logs (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, action text NOT NULL, resource_id text NOT NULL, created_at timestamptz NOT NULL DEFAULT now());
CREATE FUNCTION immutable_manifest() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF OLD.status='completed' AND (NEW.manifest IS DISTINCT FROM OLD.manifest OR NEW.checksum IS DISTINCT FROM OLD.checksum OR NEW.status IS DISTINCT FROM OLD.status) THEN RAISE EXCEPTION 'completed manifest is immutable'; END IF; RETURN NEW; END $$;
CREATE TRIGGER immutable_snapshot BEFORE UPDATE ON snapshots FOR EACH ROW EXECUTE FUNCTION immutable_manifest();
