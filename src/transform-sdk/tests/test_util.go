/*
* Copyright 2024 Redpanda Data, Inc.
*
* Use of this software is governed by the Business Source License
* included in the file licenses/BSL.md
*
* As of the Change Date specified in that file, in accordance with
* the Business Source License, use of this software will be governed
* by the Apache License, Version 2.0
 */

package integration_tests

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Load a Wasm file, if it doesn't exist the test is skipped.
func loadWasmFile(t *testing.T, envVar string) []byte {
	wasmFile, ok := os.LookupEnv(envVar)
	if !ok {
		t.Fatal("Test Wasm file is missing for", envVar)
	}
	if wasmFile == "@UNIMPLEMENTED@" {
		t.Skip("Wasm test for", envVar, "is unimplemented, skipping")
	}
	contents, err := os.ReadFile(wasmFile)
	if err != nil {
		t.Fatalf("failed to read wasm file %s: %v", wasmFile, err)
	}
	return contents
}

type stdoutLogConsumer struct{}

// Accept prints the log to stdout
func (lc *stdoutLogConsumer) Accept(l testcontainers.Log) {
	fmt.Print(string(l.Content))
}

// startRedpanda runs the Redpanda binary with a data transforms enabled.
func startRedpanda(ctx context.Context) (*redpanda.Container, context.CancelFunc) {
	redpandaContainer, err := redpanda.Run(
		ctx,
		"redpandadata/redpanda-nightly:latest",
		testcontainers.WithLogger(log.Default()),
		testcontainers.CustomizeRequestOption(func(req *testcontainers.GenericContainerRequest) error {
			if req.LogConsumerCfg == nil {
				req.LogConsumerCfg = &testcontainers.LogConsumerConfig{}
			}
			// Uncomment this to get broker logs
			req.LogConsumerCfg.Consumers = append(req.LogConsumerCfg.Consumers, &stdoutLogConsumer{})
			return nil
		}),
		redpanda.WithEnableWasmTransform(),
		redpanda.WithBootstrapConfig("data_transforms_per_core_memory_reservation", 135000000),
		redpanda.WithBootstrapConfig("data_transforms_per_function_memory_limit", 16777216),
	)
	if err != nil {
		log.Fatalf("failed to start container: %s", err)
	}
	stopFunc := func() {
		if err := redpandaContainer.Terminate(ctx); err != nil {
			log.Fatalf("failed to terminate container: %s", err)
		}
	}
	return redpandaContainer, stopFunc
}

func requireRecordsEquals(t *testing.T, fetches kgo.Fetches, records ...*kgo.Record) {
	require.NoError(t, fetches.Err())
	require.Equal(t, fetches.NumRecords(), len(records))
	for i, got := range fetches.Records() {
		want := records[i]
		requireRecordEquals(t, got, want, "record %d mismatch", i)
	}
}

func requireRecordEquals(t *testing.T, got *kgo.Record, want *kgo.Record, msg string, args ...interface{}) {
	require.Equal(t, want.Key, got.Key, "record key mismatch: %v", fmt.Sprintf(msg, args...))
	require.Equal(t, want.Value, got.Value, "record value mismatch: %v", fmt.Sprintf(msg, args...))
	require.Equal(t, want.Headers, got.Headers, "record headers mismatch: %v", fmt.Sprintf(msg, args...))
}
