# Performance

Reproduce chunking/hash throughput:
```sh
go test ./internal/chunking -run '^$' -bench BenchmarkChunking -benchmem
```

Observed on 2026-11-03: Windows amd64, Intel Core i5-1335U (12 logical processors),
Go 1.26.10. Ten MiB in-memory input, four MiB chunk buffer:

```text
BenchmarkChunking-12  72  14387749 ns/op  728.80 MB/s  4340675 B/op  11 allocs/op
```

This measures chunk hashing plus whole-file hashing and callback dispatch. It does
not measure disks, PostgreSQL, Kafka, S3, backup completion or restore throughput.
Results vary with CPU load and hardware. No end-to-end numbers are inferred from it.

Chunk buffers are bounded (1 KiB–64 MiB). Manifests have a 100,000 combined entry/reference
cap. Default worker concurrency is two and pgx pool max is 48 connections. Existing
objects are downloaded and checked before reuse; this favors integrity over network
efficiency. Final snapshot verification reads all referenced bytes again.

For end-to-end measurement, record service versions, host CPU/RAM, filesystem/storage,
network RTT, chunk size, file distribution and concurrency. Measure cold full backup,
unchanged incremental, small modifications, restore and concurrent deduplication
separately; use job creation/completion timestamps and actual object bytes. Capture
Prometheus upload and attempt-duration histograms. Do not compare synthetic chunk
throughput to a remote S3 backup workload.
