package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/anupam/slotwatch/internal/api"
	"github.com/anupam/slotwatch/internal/ringbuffer"
	"github.com/anupam/slotwatch/internal/types"
)

// mockPipeline satisfies api.PipelineController for testing without a real pipeline.
type mockPipeline struct{ paused bool }

func (m *mockPipeline) Pause()         { m.paused = true }
func (m *mockPipeline) Resume()        { m.paused = false }
func (m *mockPipeline) IsPaused() bool { return m.paused }

func TestHealthHandler(t *testing.T) {
	s := api.New(nil, nil, nil)
	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	var body map[string]any
	json.Unmarshal(w.Body.Bytes(), &body)
	if body["status"] != "ok" {
		t.Errorf("expected status=ok, got %v", body["status"])
	}
}

func TestPauseResume(t *testing.T) {
	pl := &mockPipeline{}
	s := api.New(pl, nil, nil)

	req := httptest.NewRequest("POST", "/pipeline/pause", nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("pause: expected 200, got %d", w.Code)
	}
	if !pl.paused {
		t.Error("expected pipeline to be paused")
	}

	req = httptest.NewRequest("POST", "/pipeline/resume", nil)
	w = httptest.NewRecorder()
	s.ServeHTTP(w, req)
	if pl.paused {
		t.Error("expected pipeline to be resumed")
	}
}

func TestEventsRecent(t *testing.T) {
	rb := ringbuffer.New(50)
	rb.Push(types.ChangeEvent{
		Operation:  types.OperationInsert,
		Table:      "orders",
		LSN:        123,
		CommitTime: time.Now(),
	})
	s := api.New(nil, rb, nil)
	req := httptest.NewRequest("GET", "/events/recent", nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "orders") {
		t.Errorf("expected 'orders' in response body, got: %s", w.Body.String())
	}
}

func TestUnknownRoute(t *testing.T) {
	s := api.New(nil, nil, nil)
	req := httptest.NewRequest("GET", "/nonexistent", nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}
