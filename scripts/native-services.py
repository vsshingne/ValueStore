"""Optional Windows verification helpers: download isolated local service binaries.
Not required for Docker deployment. Artifacts stay under ignored .runtime.
"""
from pathlib import Path
import urllib.request, zipfile, tarfile, concurrent.futures, subprocess, os
root=Path(__file__).resolve().parents[1]/'.runtime'
root.mkdir(exist_ok=True)
downloads={
 'postgres.jar':'https://repo.maven.apache.org/maven2/io/zonky/test/postgres/embedded-postgres-binaries-windows-amd64/17.6.0/embedded-postgres-binaries-windows-amd64-17.6.0.jar',
 'kafka.tgz':'https://archive.apache.org/dist/kafka/3.9.1/kafka_2.13-3.9.1.tgz',
}
def fetch(item):
    name,url=item
    target=root/name
    if not target.exists():
        with urllib.request.urlopen(url,timeout=60) as response, target.with_suffix('.part').open('wb') as out:
            while chunk:=response.read(1024*1024):out.write(chunk)
        target.with_suffix('.part').replace(target)
    print(name,'downloaded',flush=True)
    if name=='postgres.jar':
        with zipfile.ZipFile(target) as z:
            for member in z.namelist():
                if member.endswith('.txz'):
                    archive=root/'postgres.txz';archive.write_bytes(z.read(member))
                    with tarfile.open(archive) as t:t.extractall(root/'postgres',filter='data')
    if name=='kafka.tgz':
        with tarfile.open(target) as t:t.extractall(root,filter='data')
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    for task in executor.map(fetch,downloads.items()):pass
if not (root/'minio.exe').exists():
    subprocess.run(['go','install','github.com/minio/minio@RELEASE.2025-04-22T22-12-26Z'],env=dict(os.environ,GOBIN=str(root)),check=True)
