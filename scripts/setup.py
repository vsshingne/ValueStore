"""Generate local secrets and deterministic demo data without overwriting secrets."""
from pathlib import Path
import secrets
root = Path(__file__).resolve().parents[1]
env = root / '.env'
if not env.exists():
    values = {k: secrets.token_hex(24) for k in ['POSTGRES_PASSWORD','MINIO_ROOT_PASSWORD','API_TOKEN','GRAFANA_PASSWORD']}
    values['MINIO_ROOT_USER'] = 'vaultstore-' + secrets.token_hex(6)
    env.write_text(''.join(f'{k}={v}\n' for k,v in values.items()))
    env.chmod(0o600)
source = root / 'data/sources/demo'
source.mkdir(parents=True, exist_ok=True)
(root / 'data/restores').mkdir(parents=True, exist_ok=True)
(root / 'data/restores').chmod(0o777)  # Local non-root Compose worker write access.
for name, content in {'notes.txt':b'VaultStore deterministic fixture\n','duplicate.txt':b'VaultStore deterministic fixture\n','binary.bin':bytes(range(256))*256}.items():
    if not (source/name).exists(): (source/name).write_bytes(content)
large = source/'large.bin'
if not large.exists():
    with large.open('wb') as f:
        for i in range(32): f.write(bytes(range(256))*4096)
print('Prepared .env and deterministic demo source. Secrets were not printed.')
