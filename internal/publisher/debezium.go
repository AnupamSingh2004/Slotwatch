package publisher

import (
	"encoding/json"
	"fmt"

	"github.com/anupam/slotwatch/internal/types"
)

// debeziumPayload is the Debezium envelope format (payload only, no schema block).
// Downstream consumers that already work with Debezium work with this without changes.
type debeziumPayload struct {
	Before map[string]any `json:"before"`
	After  map[string]any `json:"after"`
	Source debeziumSource `json:"source"`
	Op     string         `json:"op"`
	TsMs   int64          `json:"ts_ms"`
}

type debeziumSource struct {
	Version   string `json:"version"`
	Connector string `json:"connector"`
	Name      string `json:"name"`
	TsMs      int64  `json:"ts_ms"`
	Snapshot  string `json:"snapshot"`
	DB        string `json:"db"`
	Schema    string `json:"schema"`
	Table     string `json:"table"`
	TxID      int64  `json:"txId"`
	LSN       uint64 `json:"lsn"`
}

// opCodes maps Go operation constants to Debezium single-char codes.
// Debezium uses single chars (c/u/d) to keep messages compact.
var opCodes = map[types.Operation]string{
	types.OperationInsert: "c",
	types.OperationUpdate: "u",
	types.OperationDelete: "d",
}

// Serialize converts a ChangeEvent into Debezium-compatible JSON bytes.
func Serialize(ev types.ChangeEvent) ([]byte, error) {
	op, ok := opCodes[ev.Operation]
	if !ok {
		return nil, fmt.Errorf("unknown operation: %s", ev.Operation)
	}
	tsMs := ev.CommitTime.UnixMilli()
	payload := debeziumPayload{
		Before: ev.Before,
		After:  ev.After,
		Op:     op,
		TsMs:   tsMs,
		Source: debeziumSource{
			Version:   "1.0.0",
			Connector: "slotwatch",
			Name:      "slotwatch",
			TsMs:      tsMs,
			Snapshot:  "false",
			Schema:    ev.Schema,
			Table:     ev.Table,
			LSN:       ev.LSN,
		},
	}
	return json.Marshal(payload)
}
