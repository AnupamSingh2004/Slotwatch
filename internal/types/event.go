package types

import "time"

type Operation string

const (
	OperationInsert Operation = "INSERT"
	OperationUpdate Operation = "UPDATE"
	OperationDelete Operation = "DELETE"
)

// ChangeEvent represents a single row-level change captured from the Postgres WAL.
// Before is nil for INSERT; After is nil for DELETE.
type ChangeEvent struct {
	Operation  Operation
	Schema     string
	Table      string
	LSN        uint64
	CommitTime time.Time
	Before     map[string]any
	After      map[string]any
}
