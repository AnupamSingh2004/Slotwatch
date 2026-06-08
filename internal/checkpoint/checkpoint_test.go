package checkpoint_test

import (
	"context"
	"os"
	"testing"

	"github.com/anupam/slotwatch/internal/checkpoint"
)

// Integration test — requires a real Postgres instance.
// Run with: SLOTWATCH_TEST_PG_DSN=postgres://... go test ./internal/checkpoint/... -v
func TestCheckpoint(t *testing.T) {
	dsn := os.Getenv("SLOTWATCH_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set SLOTWATCH_TEST_PG_DSN to run this test")
	}

	ctx := context.Background()
	store, err := checkpoint.New(ctx, dsn, "test_slot")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer store.Close()

	// Clean up from any previous run
	store.Delete(ctx, "test_slot")

	// First read — no checkpoint yet, should return 0
	lsn, err := store.Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if lsn != 0 {
		t.Errorf("expected 0 for fresh slot, got %d", lsn)
	}

	// Write then read back
	if err := store.Write(ctx, 99999); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got, err := store.Read(ctx)
	if err != nil {
		t.Fatalf("Read after write: %v", err)
	}
	if got != 99999 {
		t.Errorf("expected 99999, got %d", got)
	}
}
