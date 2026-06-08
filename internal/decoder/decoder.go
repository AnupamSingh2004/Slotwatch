package decoder

import (
	"fmt"
	"time"

	"github.com/anupam/slotwatch/internal/types"
	"github.com/jackc/pglogrepl"
)

// Decode converts a pglogrepl protocol message into a ChangeEvent.
// Returns (nil, nil) for non-change messages like BEGIN and COMMIT —
// callers should skip nil events rather than treating them as errors.
func Decode(
	msg pglogrepl.Message,
	relations map[uint32]*pglogrepl.RelationMessageV2,
	lsn uint64,
	commitTime time.Time,
) (*types.ChangeEvent, error) {
	switch m := msg.(type) {
	case *pglogrepl.InsertMessageV2:
		rel, ok := relations[m.RelationID]
		if !ok {
			return nil, fmt.Errorf("unknown relation OID %d — missing RELATION message", m.RelationID)
		}
		return &types.ChangeEvent{
			Operation:  types.OperationInsert,
			Schema:     rel.Namespace,
			Table:      rel.RelationName,
			LSN:        lsn,
			CommitTime: commitTime,
			After:      tupleToMap(m.Tuple, rel),
		}, nil

	case *pglogrepl.UpdateMessageV2:
		rel, ok := relations[m.RelationID]
		if !ok {
			return nil, fmt.Errorf("unknown relation OID %d", m.RelationID)
		}
		var before map[string]any
		if m.OldTuple != nil {
			before = tupleToMap(m.OldTuple, rel)
		}
		return &types.ChangeEvent{
			Operation:  types.OperationUpdate,
			Schema:     rel.Namespace,
			Table:      rel.RelationName,
			LSN:        lsn,
			CommitTime: commitTime,
			Before:     before,
			After:      tupleToMap(m.NewTuple, rel),
		}, nil

	case *pglogrepl.DeleteMessageV2:
		rel, ok := relations[m.RelationID]
		if !ok {
			return nil, fmt.Errorf("unknown relation OID %d", m.RelationID)
		}
		var before map[string]any
		if m.OldTuple != nil {
			before = tupleToMap(m.OldTuple, rel)
		}
		return &types.ChangeEvent{
			Operation:  types.OperationDelete,
			Schema:     rel.Namespace,
			Table:      rel.RelationName,
			LSN:        lsn,
			CommitTime: commitTime,
			Before:     before,
		}, nil

	default:
		// BEGIN, COMMIT, RELATION, TYPE messages are not row-level changes.
		return nil, nil
	}
}

// tupleToMap converts a Postgres tuple (row) into a plain map using column names
// from the relation schema. Column data arrives as text ('t'), null ('n'),
// or unchanged toast ('u') — we only decode text-format values for now.
func tupleToMap(tuple *pglogrepl.TupleData, rel *pglogrepl.RelationMessageV2) map[string]any {
	if tuple == nil {
		return nil
	}
	m := make(map[string]any, len(tuple.Columns))
	for i, col := range tuple.Columns {
		if i >= len(rel.Columns) {
			break
		}
		name := rel.Columns[i].Name
		switch col.DataType {
		case 'n': // null
			m[name] = nil
		case 't': // text representation
			m[name] = string(col.Data)
		default: // 'u' = unchanged TOAST value
			m[name] = nil
		}
	}
	return m
}
