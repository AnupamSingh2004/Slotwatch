package publisher_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/anupam/slotwatch/internal/publisher"
	"github.com/anupam/slotwatch/internal/types"
)

func TestSerializeInsert(t *testing.T) {
	ev := types.ChangeEvent{
		Operation:  types.OperationInsert,
		Schema:     "public",
		Table:      "orders",
		LSN:        12345,
		CommitTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Before:     nil,
		After:      map[string]any{"id": float64(1), "name": "anupam"},
	}

	data, err := publisher.Serialize(ev)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if payload["op"] != "c" {
		t.Errorf("expected op=c, got %v", payload["op"])
	}
	if payload["before"] != nil {
		t.Errorf("expected before=nil for INSERT, got %v", payload["before"])
	}
	after, ok := payload["after"].(map[string]any)
	if !ok {
		t.Fatal("after is not an object")
	}
	if after["name"] != "anupam" {
		t.Errorf("after.name wrong: %v", after["name"])
	}
}

func TestSerializeDelete(t *testing.T) {
	ev := types.ChangeEvent{
		Operation: types.OperationDelete,
		Schema:    "public",
		Table:     "orders",
		Before:    map[string]any{"id": float64(1)},
		After:     nil,
	}
	data, _ := publisher.Serialize(ev)
	var payload map[string]any
	json.Unmarshal(data, &payload)
	if payload["op"] != "d" {
		t.Errorf("expected op=d, got %v", payload["op"])
	}
	if payload["after"] != nil {
		t.Errorf("expected after=nil for DELETE")
	}
}

func TestSerializeUnknownOp(t *testing.T) {
	ev := types.ChangeEvent{Operation: "TRUNCATE"}
	_, err := publisher.Serialize(ev)
	if err == nil {
		t.Error("expected error for unknown operation")
	}
}
