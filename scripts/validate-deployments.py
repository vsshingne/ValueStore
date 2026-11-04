"""Validate YAML and Kubernetes resources against upstream JSON schemas."""
from pathlib import Path
import json, urllib.request, yaml, jsonschema
root=Path(__file__).resolve().parents[1]
count=0
for p in (root/'deployments/kubernetes').glob('*.yaml'):
    for document in yaml.safe_load_all(p.read_text()):
        kind=document['kind'].lower()
        api=document['apiVersion'].replace('/','-')
        suffix={'apps-v1':'apps-v1','v1':'v1'}[api]
        url=f'https://raw.githubusercontent.com/yannh/kubernetes-json-schema/master/v1.32.0-standalone-strict/{kind}-{suffix}.json'
        schema=json.load(urllib.request.urlopen(url,timeout=30))
        jsonschema.validate(document,schema)
        count+=1
compose=yaml.safe_load((root/'docker-compose.yml').read_text())
compose_schema=json.load(urllib.request.urlopen('https://raw.githubusercontent.com/compose-spec/compose-spec/main/schema/compose-spec.json',timeout=30))
jsonschema.validate(compose,compose_schema)
assert set(['api','worker','scheduler','frontend','postgres','kafka','minio','prometheus','grafana'])<=compose['services'].keys()
for p in (root/'deployments').rglob('*.yml'):list(yaml.safe_load_all(p.read_text()))
json.loads((root/'deployments/grafana/dashboards/vaultstore.json').read_text())
print(f'Validated {count} Kubernetes resources against v1.32 strict schemas; Compose/monitoring syntax valid.')
