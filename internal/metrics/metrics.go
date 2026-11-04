package metrics

import (
 "context"
 "log/slog"
 "math"
 "time"
 "github.com/prometheus/client_golang/prometheus"
 "vaultstore/internal/metadata"
)

// Register exports database-derived inventories. NaN indicates an unavailable
// measurement; database outages are never converted into fabricated zero values.
func Register(r *metadata.Repository){
 for _,item:=range []struct{name,help,query string}{
  {"vaultstore_logical_bytes","Logical bytes in retained completed snapshots","SELECT COALESCE(sum(logical_bytes),0)::float8 FROM snapshots WHERE status='completed'"},
  {"vaultstore_physical_bytes","Verified chunk metadata bytes","SELECT COALESCE(sum(size),0)::float8 FROM chunks"},
  {"vaultstore_unique_chunks","Unique chunk rows","SELECT count(*)::float8 FROM chunks"},
  {"outbox_pending_events","Unpublished durable outbox records","SELECT count(*)::float8 FROM outbox WHERE published_at IS NULL"},
  {"vaultstore_active_workers","Workers with a recent heartbeat","SELECT count(*)::float8 FROM workers WHERE heartbeat>now()-interval '30 seconds'"},
 }{
  prometheus.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name:item.name,Help:item.help},func()float64{ctx,cancel:=context.WithTimeout(context.Background(),3*time.Second);defer cancel();var n float64;if err:=r.Pool.QueryRow(ctx,item.query).Scan(&n);err!=nil{slog.Error("metric query failed","metric",item.name,"error",err);return math.NaN()};return n}))
 }
}
