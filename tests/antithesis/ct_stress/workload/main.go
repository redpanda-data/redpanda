// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Command workload is the ct_stress Antithesis workload. A single binary
// dispatches on its invocation name (argv[0]) so the test-composer command
// files (first_create_topic, parallel_driver_produce, ...) are just links
// to it. It talks to Redpanda with franz-go and reports properties and
// draws randomness through the Antithesis Go SDK.
//
// Randomness for every test-affecting choice goes through random.GetRandom
// so Antithesis both controls and can steer it; outside Antithesis the SDK
// falls back to crypto/rand.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	topic         = "foo"
	fooPartitions = 3
	fooReplicas   = 3
)

// commands maps every invocation name to its handler. Entries whose name
// carries an Antithesis test-composer prefix are the test commands, exposed
// as symlinks under /opt/antithesis/test/v1/main/ (see `list-commands`, which
// the image build uses to create those links). `setup` is the container
// entrypoint, not a test command.
var commands = map[string]func() error{
	"setup":                   setup,
	"first_create_topic":      createTopic,
	"parallel_driver_produce": produce,
	// consume reads and validates the same way the anytime checker does, but
	// as a parallel driver Antithesis may run several concurrent copies of it,
	// applying real read pressure while still asserting the invariants.
	"parallel_driver_consume":        check,
	"parallel_driver_move_metastore": moveMetastore,
	"parallel_driver_move_foo":       moveFoo,
	"anytime_check_range":            check,
	"anytime_check_cloud_io":         checkCloudIO,
	"finally_check_complete":         checkComplete,
	// check_offsets has no test-composer prefix, so it gets no symlink and
	// Antithesis never schedules it. It is a manual replay tool for the
	// multiverse debugger; see checkOffsets.
	"check_offsets": checkOffsets,
}

// cmdArgs holds the positional arguments that follow the command token, for
// the few manual commands (e.g. check_offsets) that take parameters.
var cmdArgs []string

// testCommandPrefixes are the Antithesis test-composer command prefixes; a
// command with one of these is scheduled by Antithesis and needs a symlink.
var testCommandPrefixes = []string{
	"first_", "parallel_driver_", "serial_driver_",
	"singleton_driver_", "anytime_", "eventually_", "finally_",
}

func isTestCommand(name string) bool {
	for _, p := range testCommandPrefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

// listCommands prints the test-composer command names, one per line, for the
// image build to turn into symlinks. Keeping the list here means the command
// set is defined once, in `commands`. It is dispatched specially (not via
// `commands`) to avoid an initialization cycle with the map it reads.
func listCommands() error {
	names := make([]string, 0, len(commands))
	for name := range commands {
		if isTestCommand(name) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, n := range names {
		fmt.Println(n)
	}
	return nil
}

func brokers() []string {
	return strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
}

func newClient(opts ...kgo.Opt) (*kgo.Client, error) {
	base := []kgo.Opt{
		kgo.SeedBrokers(brokers()...),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, nil)),
	}
	return kgo.NewClient(append(base, opts...)...)
}

func main() {
	cmd := filepath.Base(os.Args[0])
	cmdArgs = os.Args[1:]
	// When invoked by the binary's own name (e.g. the entrypoint's
	// `helper_workload setup`, or `workload produce` by hand) rather than
	// through a command symlink, take the command from argv[1].
	if _, known := commands[cmd]; !known && len(os.Args) > 1 {
		cmd = os.Args[1]
		cmdArgs = os.Args[2:]
	}

	// Dispatched outside `commands` to avoid an init cycle (it reads the map).
	if cmd == "list-commands" {
		if err := listCommands(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	fn, ok := commands[cmd]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown command %q\n", cmd)
		os.Exit(2)
	}
	fmt.Printf("workload: running command %q\n", cmd)
	if err := fn(); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", cmd, err)
		os.Exit(1)
	}
}
