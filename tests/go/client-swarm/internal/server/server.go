package server

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/redpanda-data/redpanda/tests/go/client-swarm/internal/metrics"
)

// Start starts the metrics HTTP server
func Start(ctx context.Context, addr string, metricsCtx *metrics.Context) error {
	mux := http.NewServeMux()

	mux.HandleFunc("/metrics/summary", func(w http.ResponseWriter, r *http.Request) {
		handleSummary(w, r, metricsCtx)
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	server := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// Start server in goroutine
	errCh := make(chan error, 1)
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	// Wait for context cancellation
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

func handleSummary(w http.ResponseWriter, r *http.Request, metricsCtx *metrics.Context) {
	// Parse seconds parameter
	secondsStr := r.URL.Query().Get("seconds")
	seconds := 0
	if secondsStr != "" {
		var err error
		seconds, err = strconv.Atoi(secondsStr)
		if err != nil {
			http.Error(w, "invalid seconds parameter", http.StatusBadRequest)
			return
		}
	}

	summary := metricsCtx.GetSummary(seconds)

	// Format response to match Rust output structure
	response := struct {
		Min             float64 `json:"min"`
		Max             float64 `json:"max"`
		Median          float64 `json:"median"`
		CountsFromStart struct {
			SuccessCount int64 `json:"success_count"`
			ErrorCount   int64 `json:"error_count"`
		} `json:"counts_from_start"`
		ClientsStarted int64 `json:"clients_started"`
		ClientsStopped int64 `json:"clients_stopped"`
	}{
		Min:            summary.Min,
		Max:            summary.Max,
		Median:         summary.Median,
		ClientsStarted: summary.ClientsStarted,
		ClientsStopped: summary.ClientsStopped,
	}
	response.CountsFromStart.SuccessCount = summary.SuccessCount
	response.CountsFromStart.ErrorCount = summary.ErrorCount

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Error encoding response: %v", err)
	}
}
