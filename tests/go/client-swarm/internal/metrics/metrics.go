package metrics

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// MessageType represents the type of client message
type MessageType int

const (
	MessageSuccess MessageType = iota
	MessageFailure
	ClientStart
	ClientStop
)

// ClientMessage represents a message from a client
type ClientMessage struct {
	Type     MessageType
	ClientID int
}

// AggregatedSample represents aggregated metrics for a time window
type AggregatedSample struct {
	Timestamp    time.Time
	SuccessCount int64
	ErrorCount   int64
	ClientID     int
	// Rate in messages per second
	Rate float64
}

// Summary represents a metrics summary response
type Summary struct {
	Min            float64 `json:"min"`
	Max            float64 `json:"max"`
	Median         float64 `json:"median"`
	SuccessCount   int64   `json:"success_count"`
	ErrorCount     int64   `json:"error_count"`
	ClientsStarted int64   `json:"clients_started"`
	ClientsStopped int64   `json:"clients_stopped"`
}

// Aggregator aggregates metrics for a single client
type Aggregator struct {
	clientID     int
	metrics      *Metrics
	successCount int64
	errorCount   int64
	windowStart  time.Time
	windowSize   time.Duration
	mu           sync.Mutex
}

// NewAggregator creates a new metrics aggregator for a client
func NewAggregator(clientID int, m *Metrics) *Aggregator {
	return &Aggregator{
		clientID:    clientID,
		metrics:     m,
		windowStart: time.Now(),
		windowSize:  10 * time.Second,
	}
}

// Record records a client message
func (a *Aggregator) Record(msg MessageType) {
	a.mu.Lock()
	defer a.mu.Unlock()

	now := time.Now()

	// Check if we need to flush the window
	if now.Sub(a.windowStart) >= a.windowSize {
		a.flushLocked()
		a.windowStart = now
	}

	switch msg {
	case MessageSuccess:
		a.successCount++
	case MessageFailure:
		a.errorCount++
	case ClientStart:
		a.metrics.recordClientStart()
	case ClientStop:
		a.metrics.recordClientStop()
		a.flushLocked()
	}
}

// Flush forces a flush of the current window
func (a *Aggregator) Flush() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.flushLocked()
}

func (a *Aggregator) flushLocked() {
	if a.successCount == 0 && a.errorCount == 0 {
		return
	}

	elapsed := time.Since(a.windowStart)
	if elapsed == 0 {
		elapsed = time.Millisecond
	}

	rate := float64(a.successCount) / elapsed.Seconds()

	sample := AggregatedSample{
		Timestamp:    time.Now(),
		SuccessCount: a.successCount,
		ErrorCount:   a.errorCount,
		ClientID:     a.clientID,
		Rate:         rate,
	}

	a.metrics.recordSample(sample)

	a.successCount = 0
	a.errorCount = 0
}

// Metrics is the central metrics aggregator
type Metrics struct {
	mu             sync.RWMutex
	samples        []AggregatedSample
	maxSamples     int
	totalSuccess   int64
	totalErrors    int64
	clientsStarted int64
	clientsStopped int64
}

// NewMetrics creates a new metrics instance
func NewMetrics() *Metrics {
	return &Metrics{
		samples:    make([]AggregatedSample, 0, 10000),
		maxSamples: 100000, // Keep last 100k samples
	}
}

func (m *Metrics) recordSample(sample AggregatedSample) {
	m.mu.Lock()
	defer m.mu.Unlock()

	atomic.AddInt64(&m.totalSuccess, sample.SuccessCount)
	atomic.AddInt64(&m.totalErrors, sample.ErrorCount)

	m.samples = append(m.samples, sample)

	// Trim if needed
	if len(m.samples) > m.maxSamples {
		// Remove oldest 10%
		cutoff := m.maxSamples / 10
		m.samples = m.samples[cutoff:]
	}
}

func (m *Metrics) recordClientStart() {
	atomic.AddInt64(&m.clientsStarted, 1)
}

func (m *Metrics) recordClientStop() {
	atomic.AddInt64(&m.clientsStopped, 1)
}

// GetSummary returns a summary of metrics, optionally filtered by time window
func (m *Metrics) GetSummary(windowSeconds int) Summary {
	m.mu.RLock()
	defer m.mu.RUnlock()

	summary := Summary{
		SuccessCount:   atomic.LoadInt64(&m.totalSuccess),
		ErrorCount:     atomic.LoadInt64(&m.totalErrors),
		ClientsStarted: atomic.LoadInt64(&m.clientsStarted),
		ClientsStopped: atomic.LoadInt64(&m.clientsStopped),
	}

	if len(m.samples) == 0 {
		return summary
	}

	// Filter samples by time window
	var rates []float64
	cutoff := time.Now().Add(-time.Duration(windowSeconds) * time.Second)

	for _, sample := range m.samples {
		if windowSeconds <= 0 || sample.Timestamp.After(cutoff) {
			rates = append(rates, sample.Rate)
		}
	}

	if len(rates) == 0 {
		return summary
	}

	// Calculate statistics
	summary.Min = rates[0]
	summary.Max = rates[0]
	sum := 0.0

	for _, r := range rates {
		if r < summary.Min {
			summary.Min = r
		}
		if r > summary.Max {
			summary.Max = r
		}
		sum += r
	}

	// Simple median (not sorted, but approximation)
	summary.Median = sum / float64(len(rates))

	// Round to 2 decimal places
	summary.Min = math.Round(summary.Min*100) / 100
	summary.Max = math.Round(summary.Max*100) / 100
	summary.Median = math.Round(summary.Median*100) / 100

	return summary
}

// Context provides a shareable metrics context
type Context struct {
	metrics *Metrics
}

// NewContext creates a new metrics context
func NewContext() *Context {
	return &Context{
		metrics: NewMetrics(),
	}
}

// NewAggregator creates a new aggregator for a client
func (c *Context) NewAggregator(clientID int) *Aggregator {
	return NewAggregator(clientID, c.metrics)
}

// GetSummary returns the current metrics summary
func (c *Context) GetSummary(windowSeconds int) Summary {
	return c.metrics.GetSummary(windowSeconds)
}

// RecordSuccess records a successful message
func (c *Context) RecordSuccess() {
	atomic.AddInt64(&c.metrics.totalSuccess, 1)
}

// RecordError records a failed message
func (c *Context) RecordError() {
	atomic.AddInt64(&c.metrics.totalErrors, 1)
}

// RecordClientStart records a client starting
func (c *Context) RecordClientStart() {
	c.metrics.recordClientStart()
}

// RecordClientStop records a client stopping
func (c *Context) RecordClientStop() {
	c.metrics.recordClientStop()
}
