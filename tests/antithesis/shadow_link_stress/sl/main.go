// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Command workload is the sl test template's Antithesis workload: a single
// binary that dispatches on its invocation name (argv[0]) so the
// test-composer command files are just symlinks to it. It produces to the
// source cluster, validates reads on both clusters, and checks that the
// shadow link replicates everything (see eventually_check_replicated).
//
// Randomness for every test-affecting choice goes through the Antithesis SDK
// so the platform both controls and can steer it; outside Antithesis the SDK
// falls back to crypto/rand.
package main

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/antithesishq/antithesis-sdk-go/random"
)

// rng draws from Antithesis-controlled entropy; each Intn consults the
// source live, which is the per-decision use the SDK wants.
var rng = rand.New(random.Source())

// randN returns a uniformly random int in [0, n).
func randN(n int) int {
	if n <= 0 {
		return 0
	}
	return rng.Intn(n)
}

// commands maps every invocation name to its handler. Entries with an
// Antithesis test-composer prefix are exposed as symlinks under
// /opt/antithesis/test/v1/sl/ (see `list-commands`). `setup` is the
// container entrypoint, not a test command.
var commands = map[string]func() error{
	"setup":                          setup,
	"first_create_topics_and_link":   createTopicsAndLink,
	"parallel_driver_produce":        produce,
	"parallel_driver_consume_source": checkSourceRange,
	// The flip and move drivers churn state that replication must survive:
	// storage-mode transitions propagated source -> shadow by the reconciler,
	// and raft reconfiguration under the shadow fetchers on both clusters.
	"parallel_driver_flip_storage_mode": flipStorageMode,
	"parallel_driver_move_kafka_topic":  moveKafkaTopic,
	"parallel_driver_move_metastore":    moveMetastore,
	"anytime_check_target":              checkTargetRange,
	"anytime_check_link":                checkLink,
	"eventually_check_replicated":       checkReplicated,
}

// cmdArgs holds positional arguments after the command token.
var cmdArgs []string

// cmdName is the command this invocation resolved to; shared validators
// carry it in assertion details so a tripped property names its phase.
var cmdName string

// quiescedPhase reports whether this invocation is an eventually command:
// fault injection has stopped and the cluster has healed. Shared validators
// skip their Sometimes/Reachable liveness assertions in this phase so a
// quiesced-cluster read cannot mask timelines where no fault-window read
// ever succeeded.
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

// listCommands prints the test-composer command names for the image build to
// turn into symlinks. Dispatched outside `commands` to avoid an init cycle.
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

func main() {
	cmd := filepath.Base(os.Args[0])
	cmdArgs = os.Args[1:]
	// When invoked by the binary's own name rather than through a command
	// symlink, take the command from argv[1].
	if _, known := commands[cmd]; !known && len(os.Args) > 1 {
		cmd = os.Args[1]
		cmdArgs = os.Args[2:]
	}
	cmdName = cmd

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
