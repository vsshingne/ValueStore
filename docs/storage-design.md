# Storage design

Chunk identity is a lowercase SHA-256 hex digest. Key layout:
`chunks/<first-two>/<next-two>/<full-hash>`. Fixed chunk size defaults to 4 MiB;
`CHUNK_SIZE` permits 1 KiB through 64 MiB. Reads are streaming. The callback owns
the chunk buffer only until it returns. The full-file hash is updated in read order.
Empty files have the SHA-256 empty digest and no chunk references.

For each hash, a PostgreSQL transaction advisory lock serializes lookup/upload.
The worker HEADs the object, reads it, and checks its size/hash before reuse. Missing
objects are uploaded and downloaded for verification, then chunk metadata commits.
A row alone is never treated as proof that an object exists. Same-content competing
workers converge on one object key and one uniquely constrained row.

Snapshots hold a global shared advisory lock throughout scanning and publication.
Collection and snapshot deletion take the exclusive lock. GC additionally checks
for live job leases to cover a lost advisory-lock connection. Referenced objects are
never intentionally deleted. GC deletes an unreferenced object before deleting its
metadata; a crash between these operations leaves harmless unreferenced metadata.
The next sweep removes it. Objects with no metadata are discovered by bucket listing.
No incomplete file references are persisted: final file/chunk rows commit together.

The design assumes all object mutation flows through VaultStore. Operators can
corrupt/delete objects independently; explicit verification and restore detect this,
but VaultStore cannot reconstruct lost unique bytes without an external replica.

## Analytics

Logical bytes = sum of file sizes in retained completed snapshots.
Physical bytes = sum of verified chunk metadata sizes (including pending orphan cleanup).
Savings = logical minus physical bytes; ratio = logical / physical (undefined at zero).
Percent savings = savings / logical × 100 (zero for empty logical data).
Values can temporarily be negative during incomplete backups. Compression is not enabled.

The UI's reused references are `max(0, file_chunk_reference_count - unique_chunk_count)`;
the runtime `chunks_reused_total` counter counts actual verified reuse operations,
including retried attempts. These are different measurements.
