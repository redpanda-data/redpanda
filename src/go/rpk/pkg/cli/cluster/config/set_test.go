package config

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	controlplanev1 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/controlplane/v1"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestParseArgs(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		expected  []string
		expectErr bool
	}{
		{
			name:      "valid key=value format",
			args:      []string{"key=value"},
			expected:  []string{"key=value"},
			expectErr: false,
		},
		{
			name:      "valid key=value key2=value2 format",
			args:      []string{"key=value", "key2=value2"},
			expected:  []string{"key=value", "key2=value2"},
			expectErr: false,
		},
		{
			name:      "valid key and value as separate arguments",
			args:      []string{"key", "value"},
			expected:  []string{"key=value"},
			expectErr: false,
		},
		{
			name:      "invalid format without '='",
			args:      []string{"key", "value1", "value2"},
			expected:  nil,
			expectErr: true,
		},
		{
			name:      "invalid single argument without '='",
			args:      []string{"key"},
			expected:  nil,
			expectErr: true,
		},
		{
			name:      "invalid multiple arguments without '='",
			args:      []string{"key", "value1", "key", "value2"},
			expected:  nil,
			expectErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := parseArgs(test.args)
			if test.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, result)
			}
		})
	}
}

// createMockOperationServer creates a test server that simulates the operation status API.
// The handler function receives the request count and returns the operation state.
func createMockOperationServer(t *testing.T, operationID string, handler func(requestCount int32) controlplanev1.Operation_State) (*httptest.Server, *atomic.Int32) {
	var requestCount atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Contains(t, r.URL.Path, "redpanda.api.controlplane.v1.OperationService/GetOperation")

		count := requestCount.Add(1)
		state := handler(count)

		response := &controlplanev1.GetOperationResponse{
			Operation: &controlplanev1.Operation{
				Id:    operationID,
				State: state,
			},
		}

		w.Header().Set("Content-Type", "application/proto")
		w.WriteHeader(http.StatusOK)

		marshaled, err := proto.Marshal(response)
		require.NoError(t, err)
		_, _ = w.Write(marshaled)
	}))

	return server, &requestCount
}

// createMockErrorServer creates a test server that returns an HTTP error.
func createMockErrorServer(statusCode int, message string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(statusCode)
		_, _ = w.Write([]byte(message))
	}))
}

func TestPollOperationStatus(t *testing.T) {
	t.Parallel()

	// Use fast polling intervals for tests to speed up execution
	testCfg := &pollConfig{
		initialDelay:      10 * time.Millisecond,
		fastPollInterval:  10 * time.Millisecond,
		slowPollInterval:  10 * time.Millisecond,
		fastPollThreshold: 50 * time.Millisecond,
	}

	tests := []struct {
		name               string
		initialState       controlplanev1.Operation_State
		transitionToState  controlplanev1.Operation_State
		transitionAfter    int // number of polls before transition
		expectCompleted    bool
		expectTimeout      bool
		expectedFinalState controlplanev1.Operation_State
	}{
		{
			name:               "operation completes immediately",
			initialState:       controlplanev1.Operation_STATE_COMPLETED,
			expectCompleted:    true,
			expectTimeout:      false,
			expectedFinalState: controlplanev1.Operation_STATE_COMPLETED,
		},
		{
			name:               "operation fails immediately",
			initialState:       controlplanev1.Operation_STATE_FAILED,
			expectCompleted:    true,
			expectTimeout:      false,
			expectedFinalState: controlplanev1.Operation_STATE_FAILED,
		},
		{
			name:               "operation transitions from in_progress to completed",
			initialState:       controlplanev1.Operation_STATE_IN_PROGRESS,
			transitionToState:  controlplanev1.Operation_STATE_COMPLETED,
			transitionAfter:    2, // transition after 2 polls
			expectCompleted:    true,
			expectTimeout:      false,
			expectedFinalState: controlplanev1.Operation_STATE_COMPLETED,
		},
		{
			name:               "operation transitions from in_progress to failed",
			initialState:       controlplanev1.Operation_STATE_IN_PROGRESS,
			transitionToState:  controlplanev1.Operation_STATE_FAILED,
			transitionAfter:    3,
			expectCompleted:    true,
			expectTimeout:      false,
			expectedFinalState: controlplanev1.Operation_STATE_FAILED,
		},
		{
			name:               "operation stays in_progress and times out",
			initialState:       controlplanev1.Operation_STATE_IN_PROGRESS,
			transitionToState:  controlplanev1.Operation_STATE_IN_PROGRESS,
			transitionAfter:    100, // never transitions
			expectCompleted:    false,
			expectTimeout:      true,
			expectedFinalState: controlplanev1.Operation_STATE_IN_PROGRESS,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			operationID := "test-operation-id"

			server, pollCount := createMockOperationServer(t, operationID, func(count int32) controlplanev1.Operation_State {
				state := tt.initialState
				// Transition to the target state after the specified number of polls
				if tt.transitionAfter > 0 && int(count) >= tt.transitionAfter {
					state = tt.transitionToState
				}
				return state
			})
			defer server.Close()

			// Create a cloud client pointing to our mock server
			cloudClient := publicapi.NewCloudClientSet(server.URL, "test-token")

			// Use shorter timeout for tests since we're using fast polling intervals
			testTimeout := 200 * time.Millisecond
			if tt.expectTimeout {
				testTimeout = 200 * time.Millisecond // Enough time for multiple polls
			}
			ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
			defer cancel()
			startTime := time.Now()

			// Call the function under test with fast test config
			operation, completedInTime, err := pollOperationStatusWithConfig(ctx, cloudClient, operationID, false, testCfg)

			// Verify no error occurred
			require.NoError(t, err)
			require.NotNil(t, operation)

			// Verify the completion status
			require.Equal(t, tt.expectCompleted, completedInTime, "completedInTime mismatch")

			// Verify the final state
			require.Equal(t, tt.expectedFinalState, operation.GetState(), "final state mismatch")

			// Verify operation ID is preserved
			require.Equal(t, operationID, operation.Id)

			// Additional time-based checks
			elapsed := time.Since(startTime)
			if tt.expectTimeout {
				// Should take at least the test timeout period (200ms)
				require.GreaterOrEqual(t, elapsed, testTimeout, "should have waited full timeout period")
			} else {
				// Should complete before the test timeout
				require.Less(t, elapsed, testTimeout, "should complete before timeout")
			}

			// Verify polling happened the expected number of times
			if tt.expectTimeout {
				// Should poll multiple times during the timeout period
				// With 10ms intervals and 200ms timeout, expect at least 5 polls
				// (actual may vary due to network latency and goroutine scheduling)
				require.GreaterOrEqual(t, pollCount.Load(), int32(5), "should have polled multiple times during timeout")
			} else if tt.transitionAfter > 0 {
				// Should have polled at least until the transition
				require.GreaterOrEqual(t, pollCount.Load(), int32(tt.transitionAfter), "should have polled until transition")
			}
		})
	}
}

func TestPollOperationStatus_ContextCancellation(t *testing.T) {
	t.Parallel()

	// Use fast polling intervals for tests
	testCfg := &pollConfig{
		initialDelay:      10 * time.Millisecond,
		fastPollInterval:  10 * time.Millisecond,
		slowPollInterval:  10 * time.Millisecond,
		fastPollThreshold: 50 * time.Millisecond,
	}

	operationID := "test-operation-id"

	server, _ := createMockOperationServer(t, operationID, func(count int32) controlplanev1.Operation_State {
		return controlplanev1.Operation_STATE_IN_PROGRESS
	})
	defer server.Close()

	cloudClient := publicapi.NewCloudClientSet(server.URL, "test-token")

	// Create a context that we'll cancel after 50ms
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel the context after 50ms (after initial delay but during polling)
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	startTime := time.Now()
	_, _, err := pollOperationStatusWithConfig(ctx, cloudClient, operationID, false, testCfg)
	elapsed := time.Since(startTime)

	// Should get a context cancellation error
	// Can happen during initial delay, polling loop, or API call
	require.Error(t, err)
	require.True(t,
		strings.Contains(err.Error(), "context cancelled while polling operation status") ||
			strings.Contains(err.Error(), "context cancelled while waiting for initial delay") ||
			strings.Contains(err.Error(), "failed to get operation status") && strings.Contains(err.Error(), "canceled"),
		"expected context cancellation error, got: %v", err)

	// Should return quickly after cancellation (within ~100ms), not wait for full polling interval
	require.Less(t, elapsed, 100*time.Millisecond, "should have been cancelled immediately, not waited for next poll")
	require.GreaterOrEqual(t, elapsed, 50*time.Millisecond, "should have waited at least until cancellation")
}

func TestPollOperationStatus_APIError(t *testing.T) {
	t.Parallel()

	// Use fast polling intervals for tests
	testCfg := &pollConfig{
		initialDelay:      10 * time.Millisecond,
		fastPollInterval:  10 * time.Millisecond,
		slowPollInterval:  10 * time.Millisecond,
		fastPollThreshold: 50 * time.Millisecond,
	}

	operationID := "test-operation-id"

	// Create a mock server that returns an error
	server := createMockErrorServer(http.StatusInternalServerError, `{"code":"internal","message":"internal server error"}`)
	defer server.Close()

	cloudClient := publicapi.NewCloudClientSet(server.URL, "test-token")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, _, err := pollOperationStatusWithConfig(ctx, cloudClient, operationID, false, testCfg)

	// Should return an error
	// Can be either "failed to get operation status" or "failed to get final operation status"
	// depending on when the error occurs
	require.Error(t, err)
	require.True(t,
		strings.Contains(err.Error(), "failed to get operation status") ||
			strings.Contains(err.Error(), "failed to get final operation status"),
		"expected operation status error, got: %v", err)
}

func TestPollOperationStatus_CustomTimeout(t *testing.T) {
	t.Parallel()

	// Use fast polling intervals for tests
	testCfg := &pollConfig{
		initialDelay:      10 * time.Millisecond,
		fastPollInterval:  10 * time.Millisecond,
		slowPollInterval:  10 * time.Millisecond,
		fastPollThreshold: 50 * time.Millisecond,
	}

	operationID := "test-operation-id"

	server, _ := createMockOperationServer(t, operationID, func(count int32) controlplanev1.Operation_State {
		return controlplanev1.Operation_STATE_IN_PROGRESS
	})
	defer server.Close()

	cloudClient := publicapi.NewCloudClientSet(server.URL, "test-token")

	// Test with a custom timeout (150ms)
	customTimeout := 150 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), customTimeout)
	defer cancel()
	startTime := time.Now()
	_, completedInTime, err := pollOperationStatusWithConfig(ctx, cloudClient, operationID, false, testCfg)
	elapsed := time.Since(startTime)

	// Should timeout without error
	require.NoError(t, err)
	require.False(t, completedInTime, "should have timed out")

	// Should have taken approximately 150ms
	require.GreaterOrEqual(t, elapsed, customTimeout, "should have waited full timeout period")
	require.Less(t, elapsed, 200*time.Millisecond, "should not have exceeded timeout significantly")
}

func TestPollOperationStatus_TimeoutShorterThanInitialDelay(t *testing.T) {
	t.Parallel()

	// Use config where initialDelay (200ms) is longer than timeout (150ms)
	testCfg := &pollConfig{
		initialDelay:      200 * time.Millisecond,
		fastPollInterval:  10 * time.Millisecond,
		slowPollInterval:  10 * time.Millisecond,
		fastPollThreshold: 50 * time.Millisecond,
	}

	operationID := "test-operation-id"

	// Create server that returns completed immediately
	server, pollCount := createMockOperationServer(t, operationID, func(count int32) controlplanev1.Operation_State {
		return controlplanev1.Operation_STATE_COMPLETED
	})
	defer server.Close()

	cloudClient := publicapi.NewCloudClientSet(server.URL, "test-token")

	// Use timeout (150ms) that is shorter than initialDelay (200ms)
	// Initial delay will be capped at timeout/2 = 75ms to reserve time for polling
	shortTimeout := 150 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), shortTimeout)
	defer cancel()
	startTime := time.Now()
	operation, completedInTime, err := pollOperationStatusWithConfig(ctx, cloudClient, operationID, false, testCfg)
	elapsed := time.Since(startTime)

	// Should complete successfully with capped initial delay
	require.NoError(t, err)
	require.True(t, completedInTime, "should have completed")
	require.Equal(t, controlplanev1.Operation_STATE_COMPLETED, operation.GetState())

	// Should have completed quickly (initial delay capped at ~75ms instead of 200ms)
	require.Less(t, elapsed, 150*time.Millisecond, "should cap initial delay to timeout/2")

	// Should have polled at least once
	require.GreaterOrEqual(t, pollCount.Load(), int32(1), "should have polled at least once")
}
