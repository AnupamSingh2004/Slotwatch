package pipeline

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/anupam/slotwatch/internal/checkpoint"
	"github.com/anupam/slotwatch/internal/decoder"
	"github.com/anupam/slotwatch/internal/metrics"
	"github.com/anupam/slotwatch/internal/publisher"
	"github.com/anupam/slotwatch/internal/replication"
	"github.com/anupam/slotwatch/internal/ringbuffer"
	"github.com/jackc/pglogrepl"
)

// Pipeline orchestrates the WAL reader → decoder → publisher → LSN tracker flow.
type Pipeline struct {
	paused     atomic.Bool
	reader     *replication.Reader
	pub        *publisher.Publisher
	checkpoint *checkpoint.Store
	ring       *ringbuffer.RingBuffer
}

func New(
	reader *replication.Reader,
	pub *publisher.Publisher,
	ckpt *checkpoint.Store,
	ring *ringbuffer.RingBuffer,
) *Pipeline {
	return &Pipeline{reader: reader, pub: pub, checkpoint: ckpt, ring: ring}
}

// Run starts the pipeline and blocks until the context is cancelled or a fatal error occurs.
func (p *Pipeline) Run(ctx context.Context) error {
	ch, err := p.reader.Start(ctx)
	if err != nil {
		return fmt.Errorf("start replication: %w", err)
	}

	// relation map is built up as RELATION messages arrive from Postgres.
	// Postgres sends a RELATION message before the first change to any table in a session,
	// and again whenever a table's schema changes.
	relations := map[uint32]*pglogrepl.RelationMessageV2{}
	metrics.PipelineUp.Set(1)

	for {
		select {
		case <-ctx.Done():
			metrics.PipelineUp.Set(0)
			return ctx.Err()

		case msg, ok := <-ch:
			if !ok {
				metrics.PipelineUp.Set(0)
				return fmt.Errorf("replication channel closed")
			}

			// When paused, drain the channel but don't publish or advance the LSN.
			// We keep the connection open so Postgres doesn't accumulate WAL waiting for us.
			if p.paused.Load() {
				continue
			}

			// RELATION messages update our schema cache — not publishable events.
			if rel, ok := msg.Data.(*pglogrepl.RelationMessageV2); ok {
				relations[rel.RelationID] = rel
				continue
			}

			ev, err := decoder.Decode(msg.Data, relations, uint64(msg.LSN), msg.CommitTime)
			if err != nil || ev == nil {
				continue
			}

			if err := p.pub.Publish(ctx, *ev); err != nil {
				metrics.PipelineUp.Set(0)
				return fmt.Errorf("publish: %w", err)
			}

			p.ring.Push(*ev)

			// Advance the checkpoint only after Kafka has confirmed the message.
			// If we crash here, we re-publish on restart — at-least-once delivery.
			if err := p.checkpoint.Write(ctx, uint64(msg.LSN)); err != nil {
				return fmt.Errorf("checkpoint write: %w", err)
			}
		}
	}
}

// Pause stops publishing but keeps the replication connection open.
// WAL position is preserved — Resume continues from the same point.
func (p *Pipeline) Pause() {
	p.paused.Store(true)
	metrics.PipelineUp.Set(0)
}

func (p *Pipeline) Resume() {
	p.paused.Store(false)
	metrics.PipelineUp.Set(1)
}

func (p *Pipeline) IsPaused() bool {
	return p.paused.Load()
}
