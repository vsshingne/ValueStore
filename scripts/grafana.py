"""Regenerate the provisioned dashboard from the actual exported metrics."""
from pathlib import Path
import json
specs=[('Backup performance','rate(backup_bytes_processed_total[5m])'),('Worker health','worker_active_jobs'),('Storage utilization','vaultstore_physical_bytes'),('Kafka outbox processing','rate(outbox_published_total[5m])'),('Failures and retries','rate(job_attempts_total{result="failed"}[5m])'),('Pending outbox','outbox_pending_events'),('Chunk reuse','rate(chunks_reused_total[5m])'),('Upload latency','rate(chunk_upload_duration_seconds_sum[5m])/rate(chunk_upload_duration_seconds_count[5m])')]
panels=[{'id':i+1,'title':title,'type':'timeseries','datasource':{'type':'prometheus','uid':'prometheus'},'gridPos':{'x':i%2*12,'y':i//2*8,'w':12,'h':8},'targets':[{'expr':expr,'refId':'A'}]} for i,(title,expr) in enumerate(specs)]
p=Path(__file__).resolve().parents[1]/'deployments/grafana/dashboards'
p.mkdir(parents=True,exist_ok=True)
(p/'vaultstore.json').write_text(json.dumps({'uid':'vaultstore','title':'VaultStore Operations','schemaVersion':39,'version':1,'refresh':'15s','panels':panels},indent=2)+'\n')
