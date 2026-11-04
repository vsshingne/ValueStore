"""End-to-end API demo. Start Docker Compose first; reads token from local .env."""
from pathlib import Path
import json, time, urllib.request, subprocess, hashlib, argparse, os
root=Path(__file__).resolve().parents[1]
values=dict(line.split('=',1) for line in (root/'.env').read_text().splitlines() if line and not line.startswith('#'))
parser=argparse.ArgumentParser();parser.add_argument('--crash',action='store_true',help='Kill/restart the demo worker container');args=parser.parse_args()
def api(path,body=None):
    request=urllib.request.Request(os.environ.get('VAULTSTORE_URL','http://localhost:8080')+'/api/v1/'+path,data=None if body is None else json.dumps(body).encode(),headers={'Authorization':'Bearer '+values['API_TOKEN'],'Content-Type':'application/json'})
    with urllib.request.urlopen(request,timeout=35) as response:return json.load(response)['data']
def wait(job):
    for _ in range(300):
        row=api('jobs/'+job)[0]
        if row['status']=='completed':return row
        if row['status'] in ['failed','cancelled']:raise RuntimeError(row)
        time.sleep(1)
    raise TimeoutError(job)
fixture=root/'data/sources/demo'
fixture.mkdir(parents=True,exist_ok=True)
for name in ['notes.txt','duplicate.txt']:(fixture/name).write_bytes(b'VaultStore deterministic fixture\n')
source=api('sources',{'name':'Deterministic demo','path':'demo','retention':10,'schedule_seconds':0,'exclusions':[]})['id']
def backup(name,kind='incremental'):
    result=api('backups',{'source_id':source,'name':name,'type':kind});wait(result['job_id']);return result['id']
first=backup('Initial full','full');before=api('storage/overview');backup('Unchanged');after=api('storage/overview')
assert before['physical_bytes']==after['physical_bytes'],'Unchanged data uploaded new chunks'
original=(root/'data/sources/demo/notes.txt').read_bytes()
(root/'data/sources/demo/notes.txt').write_bytes(b'Modified deterministic content\n');backup('Modified file')
(root/'data/sources/demo/duplicate.txt').unlink(missing_ok=True);backup('Deleted file')
destination='demo-'+str(time.time_ns())
restore=api('restores',{'snapshot_id':first,'destination':destination,'conflict':'skip'});wait(restore['id'])
assert (root/'data/restores'/destination/'notes.txt').read_bytes()==original
assert (root/'data/restores'/destination/'duplicate.txt').exists()
verify=api('backups/'+first+'/verify',{});wait(verify['job_id'])
if args.crash:
    job=api('backups',{'source_id':source,'name':'Crash recovery','type':'full'})['job_id']
    for _ in range(100):
        row=api('jobs/'+job)[0]
        if row['status']=='running':break
        if row['status']=='completed':raise RuntimeError('Backup finished before fault; increase sample size and rerun')
        time.sleep(.1)
    else:raise RuntimeError('Worker did not claim job')
    subprocess.run(['docker','compose','kill','-s','SIGKILL','worker'],cwd=root,check=True)
    subprocess.run(['docker','compose','up','-d','worker'],cwd=root,check=True)
    recovered=wait(job)
    assert recovered['attempt']>=2,'Crash did not exercise reassignment'
print(json.dumps({'first_snapshot':first,'deduplication':after,'restore_destination':destination,'restored_sha256':hashlib.sha256(original).hexdigest(),'crash_test':args.crash},indent=2))
