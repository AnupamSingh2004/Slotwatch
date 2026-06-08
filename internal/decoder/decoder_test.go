package decoder_test

import (
	"testing"
	"time"

	"github.com/anupam/slotwatch/internal/decoder"
	"github.com/anupam/slotwatch/internal/types"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

// relationMap returns a minimal relation map for testing — one table "public.orders"
// with two columns: id (INT4) and name (TEXT).
// The V2 types embed the base types, so fields are accessed through the embedded struct.
func relationMap() map[uint32]*pglogrepl.RelationMessageV2 {
	return map[uint32]*pglogrepl.RelationMessageV2{
		1: {
			RelationMessage: pglogrepl.RelationMessage{
				RelationID:   1,
				Namespace:    "public",
				RelationName: "orders",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: pgtype.Int4OID},
					{Name: "name", DataType: pgtype.TextOID},
				},
			},
		},
	}
}

func TestDecodeInsert(t *testing.T) {
	msg := &pglogrepl.InsertMessageV2{
		InsertMessage: pglogrepl.InsertMessage{
			RelationID: 1,
			Tuple: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{DataType: 't', Data: []byte("42")},
					{DataType: 't', Data: []byte("anupam")},
				},
			},
		},
	}

	ev, err := decoder.Decode(msg, relationMap(), 12345, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.Operation != types.OperationInsert {
		t.Errorf("expected INSERT, got %s", ev.Operation)
	}
	if ev.Table != "orders" {
		t.Errorf("expected table=orders, got %s", ev.Table)
	}
	if ev.After["name"] != "anupam" {
		t.Errorf("expected after.name=anupam, got %v", ev.After["name"])
	}
	if ev.Before != nil {
		t.Error("expected before=nil for INSERT")
	}
}

func TestDecodeDelete(t *testing.T) {
	msg := &pglogrepl.DeleteMessageV2{
		DeleteMessage: pglogrepl.DeleteMessage{
			RelationID: 1,
			OldTuple: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{DataType: 't', Data: []byte("42")},
					{DataType: 't', Data: []byte("anupam")},
				},
			},
		},
	}
	ev, err := decoder.Decode(msg, relationMap(), 12346, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.Operation != types.OperationDelete {
		t.Errorf("expected DELETE, got %s", ev.Operation)
	}
	if ev.After != nil {
		t.Error("expected after=nil for DELETE")
	}
	if ev.Before["name"] != "anupam" {
		t.Errorf("expected before.name=anupam, got %v", ev.Before["name"])
	}
}

func TestDecodeNonChangeMessage(t *testing.T) {
	// BEGIN and COMMIT messages are not change events — decoder returns nil, nil
	msg := &pglogrepl.BeginMessage{}
	ev, err := decoder.Decode(msg, relationMap(), 0, time.Now())
	if err != nil {
		t.Errorf("unexpected error for BEGIN: %v", err)
	}
	if ev != nil {
		t.Error("expected nil event for BEGIN message")
	}
}
