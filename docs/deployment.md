# Deployment

## Docker Compose

Run `python scripts/setup.py`, then `docker compose up -d --build`. The setup creates
random local secrets and deterministic demo files without overwriting an existing
`.env`. Infrastructure volumes persist PostgreSQL, Kafka, MinIO, Prometheus and Grafana.
Backup mounts are read-only; restore mounts are writable. Local demo permissions are
intentionally permissive for the non-root worker; use owned volumes in production.

API, worker and scheduler use the same Dockerfile with SERVICE build arguments. The
frontend uses its own Node build/nginx image. Services retry startup connections.
Compose waits on PostgreSQL/Kafka/MinIO checks; application health and readiness are
available at `/api/v1/health` and `/api/v1/ready`. Readiness currently checks PostgreSQL.

Scale workers with `docker compose up -d --scale worker=3`. Default Kafka topic partition
count is eight; total consumers beyond partitions may be idle. Job concurrency is
bounded per process. Long jobs may be redelivered after a consumer rebalance; leases
and durable state make those deliveries safe for metadata.

The MinIO image is pinned to an available community release. Evaluate current vendor
support/security and pin deployment image digests before production. Compose uses
one broker and one database: this is local development topology, not an HA cluster.

## Kubernetes

Provide a namespace, external PostgreSQL/Kafka/MinIO services, and an RWX storage class.
Update endpoints in `deployments/kubernetes/config.yaml`. Build/push API, worker,
scheduler and frontend images and replace their image names in the manifests.

Supply Secret `vaultstore-secrets` using your secret manager, with keys DATABASE_URL,
S3_ACCESS_KEY, S3_SECRET_KEY and API_TOKEN. No Secret values or placeholder passwords
are committed. Then:

```sh
kubectl -n vaultstore apply -f deployments/kubernetes/
kubectl -n vaultstore rollout status deployment/vaultstore-api
kubectl -n vaultstore rollout status deployment/vaultstore-worker
kubectl -n vaultstore port-forward service/vaultstore-frontend 3000:80
```

Provision source/restore volumes before workloads become ready. Non-root UID/GID is
65532. Set filesystem ownership accordingly. Frontend nginx routes `/api` to service
`api:8080`. Add a TLS ingress and restrict direct access to API/metrics and infrastructure.
The manifests do not provision stateful dependencies or claim a successful deployment.

## Observability and security

JSON logs include job IDs, attempts and error context. HTTP responses carry generated
request IDs. Metrics derive from actual worker events: processed bytes, uploaded/reused
chunks, upload duration, active jobs, attempts by kind/result and attempt duration.
The provided Grafana dashboard graphs those series. Kafka lag is not fabricated.

Bearer tokens live in backend environment and tab session storage, never in frontend
build arguments. Host ports bind loopback. Use TLS, secret rotation, network policies
and restricted database/bucket permissions outside the local demo. Keep filesystem
roots controlled by trusted administrators. Never expose MinIO root credentials to
browser code. Preserve PostgreSQL metadata and object bytes in a separate disaster
recovery plan.
