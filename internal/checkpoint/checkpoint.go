package checkpoint

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Store persists the last confirmed WAL LSN in a Postgres table.
// Using Postgres (rather than a local file) means the checkpoint survives
// volume deletions and is visible alongside the data it protects.
type Store struct {
	conn     *pgx.Conn
	slotName string
}

// New connects to Postgres and ensures the checkpoints table exists.
func New(ctx context.Context, dsn, slotName string) (*Store, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	s := &Store{conn: conn, slotName: slotName}
	if err := s.migrate(ctx); err != nil {
		conn.Close(ctx)
		return nil, err
	}
	return s, nil
}

func (s *Store) migrate(ctx context.Context) error {
	_, err := s.conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS slotwatch_checkpoints (
			slot_name     TEXT PRIMARY KEY,
			confirmed_lsn PG_LSN NOT NULL,
			updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`)
	return err
}

// Read returns the last confirmed LSN for this slot, or 0 if none exists.
func (s *Store) Read(ctx context.Context) (uint64, error) {
	var lsn uint64
	err := s.conn.QueryRow(ctx,
		`SELECT confirmed_lsn::text::bigint FROM slotwatch_checkpoints WHERE slot_name = $1`,
		s.slotName,
	).Scan(&lsn)
	if err == pgx.ErrNoRows {
		return 0, nil
	}
	return lsn, err
}

// Write saves the confirmed LSN. Called only after Kafka has acknowledged the event.
func (s *Store) Write(ctx context.Context, lsn uint64) error {
	_, err := s.conn.Exec(ctx, `
		INSERT INTO slotwatch_checkpoints (slot_name, confirmed_lsn, updated_at)
		VALUES ($1, $2::bigint::pg_lsn, NOW())
		ON CONFLICT (slot_name) DO UPDATE
		SET confirmed_lsn = EXCLUDED.confirmed_lsn, updated_at = NOW()
	`, s.slotName, int64(lsn))
	return err
}

// Delete removes the checkpoint — used in tests to start from a clean state.
func (s *Store) Delete(ctx context.Context, slotName string) {
	s.conn.Exec(ctx, `DELETE FROM slotwatch_checkpoints WHERE slot_name = $1`, slotName)
}

func (s *Store) Close() {
	s.conn.Close(context.Background())
}
