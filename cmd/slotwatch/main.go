package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/anupam/slotwatch/internal/api"
	"github.com/anupam/slotwatch/internal/checkpoint"
	"github.com/anupam/slotwatch/internal/config"
	"github.com/anupam/slotwatch/internal/pipeline"
	"github.com/anupam/slotwatch/internal/publisher"
	"github.com/anupam/slotwatch/internal/replication"
	"github.com/anupam/slotwatch/internal/ringbuffer"
)

// statusProvider implements api.StatusProvider.
// Lives in main to avoid import cycles — it needs both the pipeline and a Postgres connection.
type statusProvider struct {
	pipe  *pipeline.Pipeline
	cfg   *config.Config
	ring  *ringbuffer.RingBuffer
	pgDSN string
}

func (s *statusProvider) Status() api.StatusResponse {
	resp := api.StatusResponse{
		Pipeline:          "running",
		TablesTracked:     s.cfg.Pipeline.Tables,
		PostgresConnected: true,
		KafkaConnected:    true,
	}
	if s.pipe.IsPaused() {
		resp.Pipeline = "paused"
	}

	// Open a short-lived SQL connection to read WAL lag from pg_replication_slots.
	// We don't keep this connection open permanently because it's only needed on each /status poll.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, s.pgDSN)
	if err != nil {
		resp.PostgresConnected = false
		return resp
	}
	defer conn.Close(ctx)

	// wal_lag_bytes = pg_current_wal_lsn() - confirmed_flush_lsn
	// Zero means Slotwatch has confirmed everything Postgres has produced so far.
	var lagBytes int64
	err = conn.QueryRow(ctx, `
		SELECT COALESCE(
			pg_current_wal_lsn() - confirmed_flush_lsn,
			0
		)
		FROM pg_replication_slots
		WHERE slot_name = $1
	`, s.cfg.Postgres.ReplicationSlot).Scan(&lagBytes)
	if err == nil {
		resp.WalLagBytes = lagBytes
	}

	events := s.ring.All()
	if len(events) > 0 {
		resp.LastEventAt = events[len(events)-1].CommitTime
	}

	return resp
}

func main() {
	cfgPath := flag.String("config", "/etc/slotwatch/config.yml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pgDSN := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		cfg.Postgres.User, cfg.Postgres.Password,
		cfg.Postgres.Host, cfg.Postgres.Port, cfg.Postgres.Database)

	ckpt, err := checkpoint.New(ctx, pgDSN, cfg.Postgres.ReplicationSlot)
	if err != nil {
		log.Fatalf("checkpoint store: %v", err)
	}
	defer ckpt.Close()

	startLSN, err := ckpt.Read(ctx)
	if err != nil {
		log.Fatalf("read checkpoint: %v", err)
	}
	log.Printf("resuming from LSN %d", startLSN)

	reader, err := replication.New(ctx, pgDSN,
		cfg.Postgres.ReplicationSlot, cfg.Postgres.Publication, startLSN)
	if err != nil {
		log.Fatalf("replication reader: %v", err)
	}
	defer reader.Close(ctx)

	pub, err := publisher.New(cfg.Kafka.Brokers, cfg.Kafka.TopicPrefix)
	if err != nil {
		log.Fatalf("kafka publisher: %v", err)
	}
	defer pub.Close()

	ring := ringbuffer.New(50)
	pipe := pipeline.New(reader, pub, ckpt, ring)

	sp := &statusProvider{pipe: pipe, cfg: cfg, ring: ring, pgDSN: pgDSN}
	srv := api.New(pipe, ring, sp)

	go func() {
		addr := fmt.Sprintf(":%d", cfg.API.Port)
		log.Printf("HTTP API listening on %s", addr)
		if err := srv.ListenAndServe(addr); err != nil {
			log.Printf("HTTP server stopped: %v", err)
		}
	}()

	log.Println("pipeline starting")
	if err := pipe.Run(ctx); err != nil {
		log.Printf("pipeline stopped: %v", err)
	}
}
