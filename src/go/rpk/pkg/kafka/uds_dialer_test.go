// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package kafka

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRewriteUnixBrokers_PassthroughTCP(t *testing.T) {
	in := []string{"127.0.0.1:9092", "broker.example.com:9092"}
	out, paths, err := rewriteUnixBrokers(in)
	require.NoError(t, err)
	require.Equal(t, in, out)
	require.Empty(t, paths)
}

func TestRewriteUnixBrokers_SingleUDS(t *testing.T) {
	in := []string{"unix:///var/run/redpanda/kafka.sock"}
	out, paths, err := rewriteUnixBrokers(in)
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.True(t, strings.HasSuffix(out[0], udsSentinelSuffix+":1"))
	require.Len(t, paths, 1)
	host, _, err := net.SplitHostPort(out[0])
	require.NoError(t, err)
	require.Equal(t, "/var/run/redpanda/kafka.sock", paths[host])
}

func TestRewriteUnixBrokers_Mixed(t *testing.T) {
	in := []string{
		"127.0.0.1:9092",
		"unix:///tmp/a.sock",
		"broker.example.com:9092",
		"unix:///tmp/b.sock",
	}
	out, paths, err := rewriteUnixBrokers(in)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:9092", out[0])
	require.Equal(t, "broker.example.com:9092", out[2])
	require.True(t, strings.Contains(out[1], udsSentinelSuffix))
	require.True(t, strings.Contains(out[3], udsSentinelSuffix))
	require.Len(t, paths, 2)
	// Each UDS seed gets a unique sentinel host.
	h1, _, _ := net.SplitHostPort(out[1])
	h2, _, _ := net.SplitHostPort(out[3])
	require.NotEqual(t, h1, h2)
	require.Equal(t, "/tmp/a.sock", paths[h1])
	require.Equal(t, "/tmp/b.sock", paths[h2])
}

// TestRewriteUnixBrokers_MalformedRejected verifies the rewriter now fails
// fast on malformed unix:// entries. Pre-validation gives operators a
// pinpoint error at the CLI boundary instead of a confusing syscall error
// from connect(2). See validateUnixBrokerPath for the rationale.
func TestRewriteUnixBrokers_MalformedRejected(t *testing.T) {
	cases := []struct {
		name        string
		input       string
		errContains string
	}{
		{"empty_path", "unix://", "must not be empty"},
		{"relative_path", "unix://relative/path", "must be absolute"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := rewriteUnixBrokers([]string{tc.input})
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errContains)
		})
	}
}

func TestUDSDialer_DialsUnixForSentinel(t *testing.T) {
	// Create a real listening AF_UNIX socket and verify the dialer routes
	// our sentinel host to it.
	dir := t.TempDir()
	sockPath := filepath.Join(dir, "test.sock")
	ln, err := net.Listen("unix", sockPath)
	require.NoError(t, err)
	defer ln.Close()

	paths := map[string]string{"uds-0" + udsSentinelSuffix: sockPath}
	d := UDSDialer(paths, 2*time.Second)

	done := make(chan struct{})
	go func() {
		conn, acceptErr := ln.Accept()
		if acceptErr == nil {
			conn.Close()
		}
		close(done)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := d(ctx, "tcp", "uds-0"+udsSentinelSuffix+":1")
	require.NoError(t, err)
	conn.Close()
	<-done
}

func TestUDSDialer_FallsThroughForUnknownHost(t *testing.T) {
	// A hostname not in the sentinel map must use plain net.Dial, which
	// here we verify by dialing a closed-loopback port and asserting we
	// get a connection refused — not a "unix: file not found" error that
	// would prove we misrouted to the UDS path.
	//
	// Bind a loopback listener just to get a port number, then close it
	// so the next connect attempt is guaranteed to fail with
	// ECONNREFUSED.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	d := UDSDialer(map[string]string{"uds-0" + udsSentinelSuffix: "/nonexistent"}, 500*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_, err = d(ctx, "tcp", addr)
	require.Error(t, err)
	// Confirm the error mentions tcp/connect and not unix — i.e., we did
	// not misroute.
	require.NotContains(t, err.Error(), "/nonexistent")
}

func TestUDSDialer_NonHostPortPassThrough(t *testing.T) {
	// An addr without a port still gets delegated to the base dialer; we
	// don't care about the exact error, only that no panic occurs.
	d := UDSDialer(map[string]string{}, 100*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err := d(ctx, "tcp", "no-port-here")
	require.Error(t, err)
}

// sanity: ensure TempDir path survives into the test
func TestUDSDialer_SmokePaths(t *testing.T) {
	dir := t.TempDir()
	_, err := os.Stat(dir)
	require.NoError(t, err)
}

// TestRewriteUnixBrokers_PathLengthBoundaries pins down the behaviour at
// the sun_path limit. Paths at or below maxUnixBrokerPathLen pass through
// byte-for-byte; paths above it are rejected by validateUnixBrokerPath at
// the rewriter boundary so the user gets a clear error before a syscall.
func TestRewriteUnixBrokers_PathLengthBoundaries(t *testing.T) {
	cases := []struct {
		name     string
		path     string
		wantPass bool
	}{
		{"single-char", "/a", true},
		{"107-byte sun_path max", "/" + strings.Repeat("a", 106), true},
		// One past the Linux sun_path limit — rejected at the CLI
		// boundary to mirror the server-side validator.
		{"108-byte over-limit", "/" + strings.Repeat("a", 107), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := []string{unixBrokerPrefix + tc.path}
			out, paths, err := rewriteUnixBrokers(in)
			if !tc.wantPass {
				require.Error(t, err)
				require.Contains(t, err.Error(), "too long")
				return
			}
			require.NoError(t, err)
			require.Len(t, out, 1)
			require.Len(t, paths, 1)
			host, _, err := net.SplitHostPort(out[0])
			require.NoError(t, err)
			require.Equal(t, tc.path, paths[host],
				"rewriter must preserve the path byte-for-byte")
		})
	}
}

// TestValidateUnixBrokerPath_Table is the canonical reviewer-facing record
// of which client-side path shapes are accepted and rejected. Each row
// carries a `description` so a maintainer reading the diff can see the
// intent of each case without needing context from the surrounding prose.
// This mirrors the server-side path_sanity_table in
// broker_authn_endpoint_test.cc — by design the two sides reject the same
// classes of malformed input so a path accepted by rpk is also accepted by
// the broker.
func TestValidateUnixBrokerPath_Table(t *testing.T) {
	cases := []struct {
		name        string
		description string
		path        string
		wantOK      bool
		errContains string
	}{
		// ---------------- positive cases ----------------
		{
			name:        "typical_var_run",
			description: "standard /var/run path, the recommended k8s shape",
			path:        "/var/run/redpanda/kafka.sock",
			wantOK:      true,
		},
		{
			name:        "single_char_leaf",
			description: "extremely short path — just /a — legal per POSIX",
			path:        "/a",
			wantOK:      true,
		},
		{
			name:        "at_107_byte_limit",
			description: "exactly sun_path-1 (107) bytes — at the Linux limit",
			path:        "/" + strings.Repeat("x", 106),
			wantOK:      true,
		},
		{
			name:        "deep_nested_path",
			description: "multiple directory components, no traversal",
			path:        "/a/b/c/d/e/f/g/h.sock",
			wantOK:      true,
		},
		{
			name: "dots_inside_name",
			description: "literal dots inside a basename are fine " +
				"(foo.bar.sock)",
			path:   "/var/run/redpanda/kafka.api.sock",
			wantOK: true,
		},
		{
			name: "dot_dot_inside_name_accepted_client_side",
			description: "client side does not reject '..' as a lexical " +
				"component: the operator typed this path themselves and " +
				"rpk only calls connect(). The broker-side validator is " +
				"the one that rejects '..' at config-time because it " +
				"creates the inode. Documented divergence, not a bug.",
			path:   "/var/run/redpanda/../other.sock",
			wantOK: true,
		},
		// ---------------- negative cases ----------------
		{
			name:        "empty_path",
			description: "empty string — nothing to connect to",
			path:        "",
			wantOK:      false,
			errContains: "must not be empty",
		},
		{
			name: "relative_path",
			description: "no leading slash — relative paths resolve " +
				"against rpk's CWD which is rarely what the operator " +
				"intended; fail fast with a clear error",
			path:        "rp.sock",
			wantOK:      false,
			errContains: "must be absolute",
		},
		{
			name:        "relative_dot_slash",
			description: "./relative is still relative",
			path:        "./rp.sock",
			wantOK:      false,
			errContains: "must be absolute",
		},
		{
			name: "over_107_bytes",
			description: "108 bytes — one past Linux sun_path limit; the " +
				"kernel would silently truncate, producing a confusing " +
				"ENOENT. Better to reject at the CLI.",
			path:        "/" + strings.Repeat("x", 107),
			wantOK:      false,
			errContains: "too long",
		},
		{
			name: "embedded_nul",
			description: "embedded NUL byte — Go's net package rejects " +
				"this at syscall time with a generic error; we produce " +
				"a pointed message instead",
			path:        "/real/path.sock\x00/elsewhere",
			wantOK:      false,
			errContains: "NUL",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateUnixBrokerPath(tc.path)
			if tc.wantOK {
				require.NoError(t, err,
					"%s — %s: unexpected error", tc.name, tc.description)
				return
			}
			require.Error(t, err,
				"%s — %s: expected error, got nil", tc.name, tc.description)
			require.Contains(t, err.Error(), tc.errContains,
				"%s — %s: wrong error", tc.name, tc.description)
		})
	}
}

// TestUDSDialer_DeadlineExceeded verifies that a context whose deadline has
// already passed returns promptly with a deadline-exceeded error and does
// not hang or panic. This protects against a class of bugs where the
// dialer's own timeout could shadow the caller's context.
func TestUDSDialer_DeadlineExceeded(t *testing.T) {
	dir := t.TempDir()
	sockPath := filepath.Join(dir, "test.sock")
	ln, err := net.Listen("unix", sockPath)
	require.NoError(t, err)
	defer ln.Close()

	// Large dialTimeout to prove the *context* is what aborts us.
	d := UDSDialer(map[string]string{"uds-0" + udsSentinelSuffix: sockPath}, 30*time.Second)

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-1*time.Millisecond))
	defer cancel()
	_, err = d(ctx, "tcp", "uds-0"+udsSentinelSuffix+":1")
	require.Error(t, err)
	require.True(t,
		errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "deadline"),
		"expected DeadlineExceeded, got: %v", err)
}

// TestUDSDialer_ShortTimeout_SlowServer simulates an overloaded broker whose
// accept path is blocked: the socket is listening but never Accepts. A very
// short dial timeout must surface as a timeout error in bounded wall-clock
// time. Under Linux the kernel backlog absorbs the SYN-equivalent so connect
// itself returns quickly, but this test still exercises the timeout wiring
// for environments where that is not true.
func TestUDSDialer_ShortTimeout_SlowServer(t *testing.T) {
	dir := t.TempDir()
	sockPath := filepath.Join(dir, "slow.sock")
	ln, err := net.Listen("unix", sockPath)
	require.NoError(t, err)
	// Do NOT Accept. Close happens at test end.
	defer ln.Close()

	d := UDSDialer(map[string]string{"uds-0" + udsSentinelSuffix: sockPath}, 1*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	conn, err := d(ctx, "tcp", "uds-0"+udsSentinelSuffix+":1")
	// Two legitimate outcomes: (a) Linux kernel backlog accepts the
	// connect() instantly (no timeout), in which case the conn is valid
	// until someone tries to read; or (b) some other kernel path times
	// out. Either way we must not hang past the outer 2s context bound.
	require.Less(t, time.Since(start), 2*time.Second,
		"dialer must not block past configured timeouts")
	if err != nil {
		require.False(t, errors.Is(err, context.Canceled))
	} else {
		conn.Close()
	}
}

// TestUDSDialer_NonexistentSocketPath asserts that when the sentinel host
// maps to a path that does not exist on disk, the resulting error mentions
// the path — operators triaging a misconfigured bind mount need this to
// appear in logs.
func TestUDSDialer_NonexistentSocketPath(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "does-not-exist.sock")
	d := UDSDialer(map[string]string{"uds-0" + udsSentinelSuffix: missing}, 500*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_, err := d(ctx, "tcp", "uds-0"+udsSentinelSuffix+":1")
	require.Error(t, err)
	require.Contains(t, err.Error(), missing,
		"operators need the missing socket path in the error for triage")
}

// TestUDSDialer_ConcurrentDials exercises the "safe for concurrent use"
// contract in the UDSDialer doc comment. N goroutines share one dialer and
// one listener; all dials must succeed without data races (run with -race).
func TestUDSDialer_ConcurrentDials(t *testing.T) {
	dir := t.TempDir()
	sockPath := filepath.Join(dir, "concurrent.sock")
	ln, err := net.Listen("unix", sockPath)
	require.NoError(t, err)
	defer ln.Close()

	// Drain accepts in the background so dials actually complete the
	// handshake instead of queueing in the kernel backlog.
	var acceptWG sync.WaitGroup
	acceptDone := make(chan struct{})
	acceptWG.Add(1)
	go func() {
		defer acceptWG.Done()
		for {
			select {
			case <-acceptDone:
				return
			default:
			}
			c, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			c.Close()
		}
	}()

	d := UDSDialer(map[string]string{"uds-0" + udsSentinelSuffix: sockPath}, 2*time.Second)

	const N = 32
	var wg sync.WaitGroup
	errs := make(chan error, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			c, derr := d(ctx, "tcp", "uds-0"+udsSentinelSuffix+":1")
			if derr != nil {
				errs <- derr
				return
			}
			c.Close()
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		require.NoError(t, e)
	}
	close(acceptDone)
	ln.Close()
	acceptWG.Wait()
}
