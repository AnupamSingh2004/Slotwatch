package api

import (
	"net/http"
	"time"

	"github.com/anupam/slotwatch/internal/ringbuffer"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// PipelineController is the subset of pipeline.Pipeline the API needs.
// Using an interface keeps the API package decoupled from the pipeline package.
type PipelineController interface {
	Pause()
	Resume()
	IsPaused() bool
}

// StatusProvider supplies the /status response payload.
// Implemented by the caller in main.go so it can access Postgres WAL lag.
type StatusProvider interface {
	Status() StatusResponse
}

// Server is the HTTP API for Slotwatch.
type Server struct {
	mux   *http.ServeMux
	start time.Time
}

// New wires all routes. Any of pl, ring, sp may be nil — handlers degrade gracefully.
func New(pl PipelineController, ring *ringbuffer.RingBuffer, sp StatusProvider) *Server {
	s := &Server{mux: http.NewServeMux(), start: time.Now()}

	s.mux.HandleFunc("GET /health", s.handleHealth)
	s.mux.Handle("GET /metrics", promhttp.Handler())
	s.mux.HandleFunc("GET /events/recent", handleEventsRecent(ring))
	s.mux.HandleFunc("GET /status", handleStatus(sp))
	s.mux.HandleFunc("POST /pipeline/pause", handlePause(pl))
	s.mux.HandleFunc("POST /pipeline/resume", handleResume(pl))

	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) ListenAndServe(addr string) error {
	return http.ListenAndServe(addr, s)
}
