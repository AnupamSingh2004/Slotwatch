package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/anupam/slotwatch/internal/ringbuffer"
	"github.com/anupam/slotwatch/internal/types"
)

// StatusResponse is the shape of the /status JSON response.
type StatusResponse struct {
	Pipeline          string    `json:"pipeline"`
	CurrentLSN        string    `json:"current_lsn"`
	LastEventAt       time.Time `json:"last_event_at"`
	TablesTracked     []string  `json:"tables_tracked"`
	KafkaConnected    bool      `json:"kafka_connected"`
	PostgresConnected bool      `json:"postgres_connected"`
	WalLagBytes       int64     `json:"wal_lag_bytes"`
}

type recentEvent struct {
	Op    string    `json:"op"`
	Table string    `json:"table"`
	LSN   uint64    `json:"lsn"`
	Ts    time.Time `json:"ts"`
	After any       `json:"after,omitempty"`
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]any{
		"status":         "ok",
		"uptime_seconds": int(time.Since(s.start).Seconds()),
	})
}

func handleStatus(sp StatusProvider) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if sp == nil {
			writeJSON(w, StatusResponse{Pipeline: "starting"})
			return
		}
		writeJSON(w, sp.Status())
	}
}

func handleEventsRecent(ring *ringbuffer.RingBuffer) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		var events []recentEvent
		if ring != nil {
			for _, ev := range ring.All() {
				events = append(events, toRecent(ev))
			}
		}
		if events == nil {
			events = []recentEvent{}
		}
		writeJSON(w, map[string]any{"events": events})
	}
}

func handlePause(pl PipelineController) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if pl != nil {
			pl.Pause()
		}
		writeJSON(w, map[string]string{"status": "paused"})
	}
}

func handleResume(pl PipelineController) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if pl != nil {
			pl.Resume()
		}
		writeJSON(w, map[string]string{"status": "running"})
	}
}

func toRecent(ev types.ChangeEvent) recentEvent {
	return recentEvent{
		Op:    string(ev.Operation),
		Table: ev.Table,
		LSN:   ev.LSN,
		Ts:    ev.CommitTime,
		After: ev.After,
	}
}
