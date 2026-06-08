package ringbuffer_test

import (
	"testing"

	"github.com/anupam/slotwatch/internal/ringbuffer"
	"github.com/anupam/slotwatch/internal/types"
)

func makeEvent(table string) types.ChangeEvent {
	return types.ChangeEvent{Operation: types.OperationInsert, Table: table}
}

func TestCapacity(t *testing.T) {
	rb := ringbuffer.New(3)
	rb.Push(makeEvent("a"))
	rb.Push(makeEvent("b"))
	rb.Push(makeEvent("c"))
	rb.Push(makeEvent("d")) // overwrites "a"

	events := rb.All()
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
	if events[0].Table != "b" {
		t.Errorf("expected oldest=b, got %s", events[0].Table)
	}
	if events[2].Table != "d" {
		t.Errorf("expected newest=d, got %s", events[2].Table)
	}
}

func TestEmpty(t *testing.T) {
	rb := ringbuffer.New(10)
	if len(rb.All()) != 0 {
		t.Error("expected empty buffer")
	}
}

func TestSingleElement(t *testing.T) {
	rb := ringbuffer.New(5)
	rb.Push(makeEvent("only"))
	events := rb.All()
	if len(events) != 1 || events[0].Table != "only" {
		t.Errorf("unexpected result: %v", events)
	}
}
