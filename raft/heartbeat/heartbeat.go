package heartbeat

import (
	"sync"
	"sync/atomic"
	"time"
)

// Heartbeat is simple way to track heartbeats.
type Heartbeat struct {
	timeout int64
	timer   *time.Timer
	mu      sync.Mutex // Protects access to timer
	expired bool       // Tracks if the timer has expired
}

// NewHeartbeat creates new Heartbeat with specified duration. timeoutFunc will be called
// if timeout for heartbeat is expired. Note that in case of timeout you need to
// call Beat() to reactivate Heartbeat.
func NewHeartbeat(timeout time.Duration, timeoutFunc func()) *Heartbeat {
	hb := &Heartbeat{
		timeout: int64(timeout),
	}
	hb.timer = time.AfterFunc(timeout, func() {
		hb.mu.Lock()
		defer hb.mu.Unlock()
		hb.expired = true
		timeoutFunc()
	})
	return hb
}

// Beat resets internal timer to zero. It also can be used to reactivate
// Heartbeat after timeout.
func (hb *Heartbeat) Beat() {
	hb.mu.Lock()
	defer hb.mu.Unlock()
	if hb.timer != nil {
		hb.expired = false
		hb.timer.Reset(time.Duration(atomic.LoadInt64(&hb.timeout)))
	}
}

// Update updates internal timeout to d. It does not do Beat.
func (hb *Heartbeat) Update(d time.Duration) {
	atomic.StoreInt64(&hb.timeout, int64(d))
}

// Stop stops Heartbeat timer.
func (hb *Heartbeat) Stop() {
	hb.mu.Lock()
	defer hb.mu.Unlock()
	if hb.timer != nil {
		hb.timer.Stop()
		hb.timer = nil
		hb.expired = true
	}
}

func (hb *Heartbeat) Expired() bool {
	hb.mu.Lock()
	defer hb.mu.Unlock()
	return hb.expired
}
