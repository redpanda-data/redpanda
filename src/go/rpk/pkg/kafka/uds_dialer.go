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
	"fmt"
	"net"
	"strings"
	"time"
)

const (
	// unixBrokerPrefix marks a broker entry as an AF_UNIX socket path. An
	// entry of the form `unix:///var/run/redpanda/kafka.sock` is rewritten
	// into a sentinel host:port pair that franz-go will accept; the custom
	// Dialer below translates the sentinel back to the UDS path at connect
	// time.
	unixBrokerPrefix = "unix://"

	// udsSentinelSuffix uses the IANA-reserved `.invalid` TLD so the
	// sentinel can never collide with a resolvable DNS name.
	udsSentinelSuffix = ".uds.invalid"

	// maxUnixBrokerPathLen is sun_path (108 on Linux) minus the mandatory
	// trailing NUL. Matches the server-side max_unix_path_length so a path
	// accepted by rpk is also accepted by the broker validator.
	maxUnixBrokerPathLen = 107
)

// validateUnixBrokerPath applies the client-side sanity checks we want rpk
// to enforce on `unix://...` seed entries. The trust boundary is the broker
// — server-side validation is what protects the cluster. The checks here
// exist purely to give operators a clear, fast error at CLI boundary rather
// than an opaque `connect: invalid argument` from the kernel, and to stay in
// lock-step with the server's length / absolute-path / NUL guards so a path
// that the broker will reject never makes it out of rpk. See
// src/v/config/broker_authn_endpoint.cc:validate_broker_authn_endpoint for
// the mirror side.
func validateUnixBrokerPath(path string) error {
	if path == "" {
		return fmt.Errorf("unix broker path must not be empty")
	}
	if path[0] != '/' {
		return fmt.Errorf(
			"unix broker path %q must be absolute (start with '/')", path)
	}
	if len(path) > maxUnixBrokerPathLen {
		return fmt.Errorf(
			"unix broker path %q too long (%d bytes, max %d)",
			path, len(path), maxUnixBrokerPathLen)
	}
	if strings.ContainsRune(path, 0) {
		return fmt.Errorf(
			"unix broker path must not contain embedded NUL bytes")
	}
	return nil
}

// rewriteUnixBrokers scans seed broker entries, replacing any
// `unix:///path` entry with a sentinel `"uds-<idx>.uds.invalid:1"` string
// and recording the path in the returned map keyed by the sentinel host.
// Non-UDS entries pass through unchanged.
//
// Each UDS entry's path is validated via validateUnixBrokerPath; a
// validation failure is returned with the offending seed index so the
// caller can surface a pinpoint error message. The rewriter fails fast on
// the first bad entry — rpk has no partial-success semantics for seeds.
//
// The sentinel hostname is only visible to franz-go internals; it is never
// sent on the wire because UDSDialer short-circuits before any DNS
// resolution.
func rewriteUnixBrokers(brokers []string) ([]string, map[string]string, error) {
	out := make([]string, len(brokers))
	paths := map[string]string{}
	idx := 0
	for i, b := range brokers {
		if !strings.HasPrefix(b, unixBrokerPrefix) {
			out[i] = b
			continue
		}
		path := strings.TrimPrefix(b, unixBrokerPrefix)
		if err := validateUnixBrokerPath(path); err != nil {
			return nil, nil, fmt.Errorf("seed broker[%d] %q: %w", i, b, err)
		}
		host := fmt.Sprintf("uds-%d%s", idx, udsSentinelSuffix)
		out[i] = net.JoinHostPort(host, "1")
		paths[host] = path
		idx++
	}
	return out, paths, nil
}

// UDSDialer returns a franz-go-compatible dial function that routes any
// connection targeted at a UDS sentinel host (as produced by
// rewriteUnixBrokers) to the corresponding AF_UNIX socket path. Connections
// to any other host are delegated to a net.Dialer with a bounded timeout.
//
// This is wired into kgo.NewClient via kgo.Dialer(...). The returned
// function is safe for concurrent use — it only reads the paths map.
func UDSDialer(paths map[string]string, dialTimeout time.Duration) func(ctx context.Context, network, addr string) (net.Conn, error) {
	base := &net.Dialer{Timeout: dialTimeout}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			// addr wasn't host:port form — pass through unchanged.
			return base.DialContext(ctx, network, addr)
		}
		if path, ok := paths[host]; ok {
			return base.DialContext(ctx, "unix", path)
		}
		return base.DialContext(ctx, network, addr)
	}
}
