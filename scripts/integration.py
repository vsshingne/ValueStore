"""Run integration tests against a separate database and unique object bucket."""
from pathlib import Path
import os, subprocess, secrets
root = Path(__file__).resolve().parents[1]
env = dict(os.environ)
values = dict(line.split('=',1) for line in (root/'.env').read_text().splitlines() if line and not line.startswith('#'))
subprocess.run(['docker','compose','exec','-T','postgres','psql','-U','vaultstore','-d','postgres','-c','DROP DATABASE IF EXISTS vaultstore_test WITH (FORCE)'],cwd=root,check=True)
subprocess.run(['docker','compose','exec','-T','postgres','createdb','-U','vaultstore','vaultstore_test'],cwd=root,check=True)
env.update(DATABASE_URL=f"postgres://vaultstore:{values['POSTGRES_PASSWORD']}@localhost:5432/vaultstore_test?sslmode=disable",S3_ENDPOINT='localhost:9000',S3_ACCESS_KEY=values['MINIO_ROOT_USER'],S3_SECRET_KEY=values['MINIO_ROOT_PASSWORD'],API_TOKEN=values['API_TOKEN'],KAFKA_BROKERS='localhost:19092',KAFKA_TOPIC='test-'+secrets.token_hex(8),VAULTSTORE_INTEGRATION='1')
subprocess.run(['go','test','-tags=integration','./tests/integration','-v','-count=1'],cwd=root,env=env,check=True)
