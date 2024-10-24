/*
* Copyright 2023 Redpanda Data, Inc.
*
* Use of this software is governed by the Business Source License
* included in the file licenses/BSL.md
*
* As of the Change Date specified in that file, in accordance with
* the Business Source License, use of this software will be governed
* by the Apache License, Version 2.0
 */

package common

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
)

type StatusReporter struct {
	port    int16
	current any
	mu      *sync.Mutex
}

func NewStatusReporter(port int16) *StatusReporter {
	return &StatusReporter{
		port:    port,
		current: nil,
		mu:      &sync.Mutex{},
	}
}

func (sr *StatusReporter) Set(v any) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.current = v
}

func (sr *StatusReporter) ServeHTTP(ctx context.Context) error {
	server := &http.Server{Addr: fmt.Sprintf("0.0.0.0:%d", sr.port)}
	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		slog.Debug("request status")
		sr.mu.Lock()
		defer sr.mu.Unlock()
		buf, err := json.Marshal(sr.current)
		if err == nil {
			w.Write(buf)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		type ErrorMessage struct {
			msg string
		}
		// errors are impossible here
		buf, _ = json.Marshal(&ErrorMessage{msg: err.Error()})
		_, _ = w.Write(buf)
	})
	http.HandleFunc("/stop", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("stopping HTTP server...")
		_, _ = w.Write([]byte(`{"msg": "ok"}`))
		CancelContext(ctx)
	})
	go func() {
		slog.Debug("waiting for context to be done for HTTP status server")
		<-ctx.Done()
		slog.Info("context done: shutting down HTTP status server")
		_ = server.Shutdown(ctx)
	}()
	err := server.ListenAndServe()
	// This is an expected shutdown, don't return an error
	if err == http.ErrServerClosed {
		err = nil
	}
	return err
}
