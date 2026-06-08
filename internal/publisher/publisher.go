package publisher

import (
	"context"
	"fmt"

	"github.com/anupam/slotwatch/internal/metrics"
	"github.com/anupam/slotwatch/internal/types"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Publisher sends ChangeEvents to Kafka topics.
// One topic per table: <prefix>.<schema>.<table> (e.g. postgres.public.orders).
type Publisher struct {
	client      *kgo.Client
	topicPrefix string
}

// New creates a Kafka producer with all-ISR acknowledgement (strongest durability guarantee).
func New(brokers []string, topicPrefix string) (*Publisher, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		// AllISRAcks waits for all in-sync replicas to confirm — no data loss on broker failure.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchMaxBytes(1<<20),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return &Publisher{client: client, topicPrefix: topicPrefix}, nil
}

// Publish serializes ev and produces it to Kafka synchronously.
// Returns only after Kafka has acknowledged the message.
func (p *Publisher) Publish(ctx context.Context, ev types.ChangeEvent) error {
	data, err := Serialize(ev)
	if err != nil {
		return err
	}

	topic := fmt.Sprintf("%s.%s.%s", p.topicPrefix, ev.Schema, ev.Table)

	// Use the first column value as the partition key.
	// This ensures all changes for the same row land on the same partition,
	// which preserves ordering for downstream consumers.
	var key []byte
	src := ev.After
	if src == nil {
		src = ev.Before
	}
	for _, v := range src {
		key = []byte(fmt.Sprintf("%v", v))
		break
	}

	results := p.client.ProduceSync(ctx, &kgo.Record{
		Topic: topic,
		Key:   key,
		Value: data,
	})
	if err := results.FirstErr(); err != nil {
		metrics.KafkaErrors.Inc()
		return fmt.Errorf("kafka produce: %w", err)
	}

	metrics.EventsPublished.WithLabelValues(ev.Table).Inc()
	return nil
}

func (p *Publisher) Close() {
	p.client.Close()
}
