# Backup and restore

Sources specify name, path relative to BACKUP_ROOT, glob exclusions, retention count
and optional schedule interval in seconds. Globs follow Go path.Match semantics;
`**` is not a recursive glob extension. Matching directories prune their subtree.

A full backup scans every regular file and directory. An incremental also scans and
hashes current contents, records the previous completed snapshot, and reuses verified
chunks. The resulting manifest is complete rather than a delta chain. Deleted source
paths are omitted. SHA-256, not mtime, determines content identity.

Manifest v1 contains snapshot/source/parent IDs, snapshot creation time, and ordered
file entries with size, SHA-256, mode, mtime, directory flag and ordered chunks. Its
checksum covers the exact stored JSON bytes; normalized rows are a query/reference
index. A completed manifest is protected by a PostgreSQL update trigger.

## Restore sequence

```mermaid
sequenceDiagram
 participant U as Client
 participant A as API
 participant D as PostgreSQL
 participant W as Worker
 participant O as MinIO
 participant F as Restricted destination
 U->>A: Snapshot, selection, destination, conflict policy
 A->>D: Validate complete snapshot; job + outbox
 W->>D: Claim lease, load and checksum manifest
 loop Selected files
 W->>O: Read ordered chunks
 W->>W: Check chunk hashes and full-file hash
 W->>F: Write and fsync temporary file
 W->>D: Confirm lease
 W->>F: Publish file atomically where supported
 end
 W->>D: Commit completed report
```

Restore selection matches the exact path or its descendants and retains the source
relative path in the destination. `skip` never overwrites existing entries. `rename`
uses `<path>.restored-<job UUID>`; a retry accepts that file only if its hash matches.
`overwrite` is explicit and replaces each completed file with a rename. Non-overwrite
publication uses a hard link to make the existence check atomic. Unsupported filesystems
return an error rather than silently reverting to an unsafe overwrite.

Root handles use Go os.Root to prevent traversal and symlink escapes. Paths reject
absolute paths, dot-dot, backslashes, Windows drive/ADS forms and reserved device names.
Existing destination symlinks and nonregular file targets are rejected. Restore roots
should be owned by the service and not writable by untrusted local users.

Verification jobs read and hash the immutable manifest and all its referenced bytes.
Corruption fails the job with the affected file/chunk in its error. No repair is
attempted automatically. The corruption exercise exists only in integration tests.
