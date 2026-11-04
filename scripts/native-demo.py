"""Run compiled API/scheduler/worker, API demo, and actual worker kill/recovery.
Uses only the isolated vaultstore_test database and loopback native services.
"""
from pathlib import Path
import os, subprocess, secrets, urllib.request, json, time, sys, hashlib
root=Path(__file__).resolve().parents[1]
values=dict(line.split('=',1) for line in (root/'.env').read_text().splitlines() if line and not line.startswith('#'))
env=dict(os.environ,DATABASE_URL='postgres://vaultstore@localhost:15432/vaultstore_test?sslmode=disable',S3_ENDPOINT='localhost:19000',S3_ACCESS_KEY=values['MINIO_ROOT_USER'],S3_SECRET_KEY=values['MINIO_ROOT_PASSWORD'],API_TOKEN=values['API_TOKEN'],KAFKA_BROKERS='localhost:19092',KAFKA_TOPIC='demo-'+secrets.token_hex(8),S3_BUCKET='demo-'+secrets.token_hex(8),BACKUP_ROOT=str(root/'data/sources'),RESTORE_ROOT=str(root/'data/restores'),VAULTSTORE_URL='http://127.0.0.1:18080')
subprocess.run(['go','run','./cmd/devdb','-reset'],cwd=root,env=dict(env,DATABASE_URL='postgres://vaultstore@localhost:15432/postgres?sslmode=disable'),check=True)
processes={}
def start(role,port):
    log=(root/f'.runtime/{role}.log').open('a')
    proc=subprocess.Popen([str(root/f'.runtime/{role}.exe')],cwd=root,env=dict(env,LISTEN_ADDR=f'127.0.0.1:{port}'),stdout=log,stderr=log,creationflags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0)
    log.close();processes[role]=proc;return proc
for role in ['api','worker','scheduler']:
    subprocess.run(['go','build','-o',str(root/f'.runtime/{role}.exe'),f'./cmd/{role}'],cwd=root,check=True)
def api(path,body=None):
    req=urllib.request.Request(env['VAULTSTORE_URL']+'/api/v1/'+path,data=None if body is None else json.dumps(body).encode(),headers={'Authorization':'Bearer '+values['API_TOKEN'],'Content-Type':'application/json'})
    with urllib.request.urlopen(req,timeout=35) as response:return json.load(response)['data']
try:
    start('api',18080);start('worker',18081);start('scheduler',18082)
    for _ in range(60):
        try:api('ready');break
        except Exception:time.sleep(1)
    else:raise RuntimeError('API startup failed; inspect .runtime/api.log')
    subprocess.run([sys.executable,'scripts/demo.py'],cwd=root,env=env,check=True)
    if os.environ.get('VAULTSTORE_SKIP_CRASH')=='1':
        subprocess.run(['node','browser-smoke.mjs'],cwd=root/'web',env=env,check=True)
        sys.exit(0)
    source=api('sources')[0]['id']
    job=api('backups',{'source_id':source,'name':'Actual process crash recovery','type':'full'})['job_id']
    for _ in range(1000):
        status=api('jobs/'+job)[0]['status']
        if status=='running':break
        if status=='completed':raise RuntimeError('Job completed before crash injection')
        time.sleep(.01)
    else:raise RuntimeError('Job was not claimed')
    processes['worker'].kill();processes['worker'].wait(timeout=10)
    start('worker',18081)
    print('Killed worker during running backup; waiting for lease expiry and retry.',flush=True)
    for _ in range(150):
        row=api('jobs/'+job)[0]
        if row['status']=='completed':
            assert row['attempt']>=2,'Lease reassignment was not exercised'
            print(json.dumps({'crash_recovered':True,'attempts':row['attempt'],'job_id':job}),flush=True);break
        if row['status']=='failed':raise RuntimeError(row)
        time.sleep(1)
    else:raise TimeoutError('Crash recovery did not complete')
    restore=api('restores',{'snapshot_id':row['snapshot_id'],'destination':'crash-restore-'+job,'conflict':'skip'})['id']
    for _ in range(1000):
        restore_row=api('jobs/'+restore)[0]
        if restore_row['status']=='running':break
        if restore_row['status']=='completed':raise RuntimeError('Restore completed before crash injection')
        time.sleep(.01)
    else:raise RuntimeError('Restore was not claimed')
    processes['worker'].kill();processes['worker'].wait(timeout=10);start('worker',18081)
    print('Killed worker during restore; waiting for lease expiry and retry.',flush=True)
    for _ in range(150):
        restore_row=api('jobs/'+restore)[0]
        if restore_row['status']=='completed':
            assert restore_row['attempt']>=2
            restored=root/'data/restores'/('crash-restore-'+job)/'large.bin'
            assert hashlib.file_digest(restored.open('rb'),'sha256').digest()==hashlib.file_digest((root/'data/sources/demo/large.bin').open('rb'),'sha256').digest()
            print(json.dumps({'restore_crash_recovered':True,'attempts':restore_row['attempt'],'job_id':restore}),flush=True);break
        if restore_row['status']=='failed':raise RuntimeError(restore_row)
        time.sleep(1)
    else:raise TimeoutError('Restore crash recovery did not complete')
    if os.environ.get('VAULTSTORE_BROWSER_CHECK')=='1':
        subprocess.run(['node','browser-smoke.mjs'],cwd=root/'web',env=env,check=True)
finally:
    for proc in processes.values():
        if proc.poll() is None:proc.terminate()
    for proc in processes.values():
        try:proc.wait(timeout=20)
        except subprocess.TimeoutExpired:proc.kill()
