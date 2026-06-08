package metrics

import "github.com/prometheus/client_golang/prometheus"

// Counters and gauges exposed on the /metrics endpoint (Prometheus text format).
// Any Grafana/Prometheus setup can scrape this without dashboard changes.
var (
	EventsPublished = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "slotwatch_events_published_total",
		Help: "Total WAL events successfully published to Kafka, by table.",
	}, []string{"table"})

	KafkaErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "slotwatch_kafka_errors_total",
		Help: "Total Kafka publish errors.",
	})

	// WalLagBytes is pg_current_wal_lsn() - confirmed_flush_lsn from pg_replication_slots.
	// Zero means fully caught up with Postgres.
	WalLagBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "slotwatch_wal_lag_bytes",
		Help: "Bytes of WAL produced by Postgres but not yet confirmed by Slotwatch.",
	})

	PipelineUp = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "slotwatch_pipeline_up",
		Help: "1 if the pipeline is actively processing, 0 if paused or errored.",
	})
)

func init() {
	prometheus.MustRegister(EventsPublished, KafkaErrors, WalLagBytes, PipelineUp)
}
