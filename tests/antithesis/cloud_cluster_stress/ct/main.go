// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Command workload is the ct test template's Antithesis workload. A single
// binary
// dispatches on its invocation name (argv[0]) so the test-composer command
// files (first_create_topics, parallel_driver_produce_foo, ...) are just links
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
	fooTopic      = "foo"
	fooPartitions = 3
	fooReplicas   = 3
)

// commands maps every invocation name to its handler. Entries whose name
// carries an Antithesis test-composer prefix are the test commands, exposed
// as symlinks under /opt/antithesis/test/v1/ct/ (see `list-commands`, which
// the image build uses to create those links). `setup` is the container
// entrypoint, not a test command.
var commands = map[string]func() error{
	"setup":                       setup,
	"first_create_topics":         createTestTopics,
	"parallel_driver_produce_foo": produceFoo,
	// consume reads and validates the same way the anytime checker does, but
	// as a parallel driver Antithesis may run several concurrent copies of it,
	// applying real read pressure while still asserting the invariants.
	"parallel_driver_consume_foo":       checkFoo,
	"parallel_driver_move_metastore":    moveMetastore,
	"parallel_driver_move_kafka_topic":  moveKafkaTopic,
	"parallel_driver_flip_storage_mode": flipStorageMode,
	// The ctc commands fuzz compaction on a compacted cloud topic: the
	// sweeper asserts log-shape invariants and holds surviving records
	// against the trackers' acked-write summaries. See ctc.go.
	"parallel_driver_produce_ctc": produceCtc,
	"parallel_driver_sweep_ctc":   sweepCtc,
	"anytime_check_range_foo":     checkFoo,
	// eventually rather than finally: finally commands only run on timelines
	// where every started command completed and none was killed by a fault,
	// which excludes the most hostile histories — exactly the ones this data
	// completeness check exists for. An eventually command runs after any
	// driver has started; Antithesis kills the stragglers, which the acked
	// summaries tolerate by design (only observed acks, atomic replace).
	"eventually_check_complete": checkComplete,
	// check_offsets_foo has no test-composer prefix, so it gets no symlink and
	// Antithesis never schedules it. It is a manual replay tool for the
	// multiverse debugger; see checkFooOffsets.
	"check_offsets_foo": checkFooOffsets,
}

// cmdArgs holds the positional arguments that follow the command token, for
// the few manual commands (e.g. check_offsets_foo) that take parameters.
var cmdArgs []string

// cmdName is the command this invocation resolved to. The checkers shared
// across commands (validateFooRange, validateCtcRange) carry it in assertion
// details so a tripped property names the phase it fired in — e.g.
// parallel_driver_consume_foo vs anytime_check_range_foo vs eventually_check_complete.
var cmdName string

// quiescedPhase reports whether this invocation is an eventually command,
// i.e. fault injection has stopped and the cluster has healed. The shared
// validators skip their Sometimes/Reachable assertions in this phase: those
// liveness properties exist to prove reads make progress while faults are
// possible, and a quiesced-cluster read satisfying them would mask timelines
// where no such read ever succeeded. The eventually checks assert their own
// liveness under distinct names instead.
func quiescedPhase() bool {
	return strings.HasPrefix(cmdName, "eventually_")
}

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
	// `helper_workload setup`, or `workload parallel_driver_produce_foo` by
	// hand) rather than through a command symlink, take the command from
	// argv[1].
	if _, known := commands[cmd]; !known && len(os.Args) > 1 {
		cmd = os.Args[1]
		cmdArgs = os.Args[2:]
	}
	cmdName = cmd

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
