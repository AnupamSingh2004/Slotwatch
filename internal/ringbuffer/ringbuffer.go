package ringbuffer

import (
	"sync"

	"github.com/anupam/slotwatch/internal/types"
)

// RingBuffer is a fixed-capacity circular buffer of ChangeEvents.
// When full, the oldest entry is overwritten. All methods are safe for
// concurrent use — Push from the pipeline goroutine, All from the HTTP handler.
type RingBuffer struct {
	mu       sync.RWMutex
	buf      []types.ChangeEvent
	capacity int
	head     int // next write position
	count    int // number of valid entries (≤ capacity)
}

func New(capacity int) *RingBuffer {
	return &RingBuffer{
		buf:      make([]types.ChangeEvent, capacity),
		capacity: capacity,
	}
}

func (r *RingBuffer) Push(ev types.ChangeEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.buf[r.head] = ev
	r.head = (r.head + 1) % r.capacity
	if r.count < r.capacity {
		r.count++
	}
}

// All returns events in insertion order, oldest first.
func (r *RingBuffer) All() []types.ChangeEvent {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.count == 0 {
		return nil
	}
	out := make([]types.ChangeEvent, r.count)
	// start is the index of the oldest entry
	start := (r.head - r.count + r.capacity) % r.capacity
	for i := 0; i < r.count; i++ {
		out[i] = r.buf[(start+i)%r.capacity]
	}
	return out
}
