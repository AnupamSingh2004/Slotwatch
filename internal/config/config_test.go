package config_test

import (
	"os"
	"testing"

	"github.com/anupam/slotwatch/internal/config"
)

func TestLoad(t *testing.T) {
	os.Setenv("TEST_PG_PASS", "hunter2")
	defer os.Unsetenv("TEST_PG_PASS")

	yaml := `
postgres:
  host: localhost
  port: 5432
  user: slotwatch
  password: ${TEST_PG_PASS}
  database: myapp
  replication_slot: slotwatch_slot
  publication: slotwatch_pub
kafka:
  brokers:
    - kafka:9092
  topic_prefix: postgres
pipeline:
  tables:
    - public.orders
  wal_lag_warn_threshold_bytes: 10485760
api:
  port: 8080
`
	f, _ := os.CreateTemp("", "slotwatch-*.yml")
	f.WriteString(yaml)
	f.Close()
	defer os.Remove(f.Name())

	cfg, err := config.Load(f.Name())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Postgres.Password != "hunter2" {
		t.Errorf("env var not expanded: got %q", cfg.Postgres.Password)
	}
	if cfg.Postgres.Host != "localhost" {
		t.Errorf("host wrong: got %q", cfg.Postgres.Host)
	}
	if len(cfg.Kafka.Brokers) != 1 {
		t.Errorf("expected 1 broker, got %d", len(cfg.Kafka.Brokers))
	}
	if cfg.API.Port != 8080 {
		t.Errorf("expected port 8080, got %d", cfg.API.Port)
	}
}
