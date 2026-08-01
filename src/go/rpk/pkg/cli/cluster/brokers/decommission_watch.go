// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package brokers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"time"

	"github.com/docker/go-units"
	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/cobra"
	"github.com/twmb/types"
)

const defaultDecommissionPollInterval = 5 * time.Second

// watchDecommissionOptions configures a decommission watch loop.
type watchDecommissionOptions struct {
	detailed     bool
	human        bool
	waitTimeout  time.Duration // 0 means no limit.
	pollInterval time.Duration
}

// decommissionProgress is the aggregate view of a decommission derived from a
// single status response.
type decommissionProgress struct {
	partitionsMoving int
	bytesMoved       int
	bytesRemaining   int
}

func computeDecommissionProgress(dbs rpadmin.DecommissionStatusResponse) decommissionProgress {
	p := decommissionProgress{partitionsMoving: len(dbs.Partitions)}
	for _, part := range dbs.Partitions {
		p.bytesMoved += part.BytesMoved
		p.bytesRemaining += part.BytesLeftToMove
	}
	return p
}

// percent is the fraction of bytes already moved. An empty moving set yields
// 0: during an in-progress decommission it means reallocation has not started
// yet (or is stalled). Completion is signaled by the finished flag, which the
// caller checks first.
func (p decommissionProgress) percent() int {
	total := p.bytesMoved + p.bytesRemaining
	if total <= 0 {
		return 0
	}
	return p.bytesMoved * 100 / total
}

// failureSuffix renders a compact note about standing allocation /
// reallocation failures, which the cluster retries. It is meant to be appended
// after a parenthesized progress clause, and is empty when there are none.
func failureSuffix(dbs rpadmin.DecommissionStatusResponse) string {
	if n := len(dbs.ReallocationFailureDetails); n > 0 {
		return fmt.Sprintf(", %d reallocation %s (retrying)", n, pluralFailures(n))
	}
	if n := len(dbs.AllocationFailures); n > 0 {
		return fmt.Sprintf(", %d allocation %s (retrying)", n, pluralFailures(n))
	}
	return ""
}

func pluralFailures(n int) string {
	if n == 1 {
		return "failure"
	}
	return "failures"
}

func decommissionProgressLine(broker int, dbs rpadmin.DecommissionStatusResponse, human bool) string {
	p := computeDecommissionProgress(dbs)
	// Until partitions enter the moving set there is no measurable progress
	// yet, so report that the cluster has not scheduled the moves.
	if p.partitionsMoving == 0 {
		return fmt.Sprintf("broker %d: waiting for the cluster to schedule partition movement%s", broker, failureSuffix(dbs))
	}
	size := strconv.Itoa(p.bytesRemaining)
	if human {
		size = units.HumanSize(float64(p.bytesRemaining))
	}
	return fmt.Sprintf("broker %d: %d%% complete (%d partitions remaining, %s left to move)%s",
		broker, p.percent(), p.partitionsMoving, size, failureSuffix(dbs))
}

// watchDecommission polls the decommission status of a broker until it
// finishes, the wait timeout elapses, or the context is canceled. It streams
// progress to w while waiting.
//
// It returns nil once the decommission has finished (including the
// replicas-left warning case). It returns a non-nil error on timeout,
// cancellation, or an unrecoverable status error, so the caller surfaces a
// non-zero exit.
func watchDecommission(
	ctx context.Context,
	cl *rpadmin.AdminAPI,
	broker int,
	f config.OutFormatter,
	opts watchDecommissionOptions,
	w io.Writer,
) error {
	interval := opts.pollInterval
	if interval <= 0 {
		interval = defaultDecommissionPollInterval
	}
	if opts.waitTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.waitTimeout)
		defer cancel()
	}

	// In-place line redraw only when writing to a terminal and not emitting a
	// full table or machine-readable output, so we never corrupt logs, scripts,
	// or JSON/YAML documents.
	interactive := out.IsTerminal(w) && !opts.detailed && f.IsText()
	printedInPlace := false

	retries := 3
	var lastStatus rpadmin.DecommissionStatusResponse
	haveStatus := false
	for {
		dbs, err := cl.DecommissionBrokerStatus(ctx, broker)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				breakInPlace(w, &printedInPlace)
				return watchCtxError(ctxErr, broker, lastStatus, haveStatus, opts.waitTimeout)
			}
			if he := (*rpadmin.HTTPResponseError)(nil); errors.As(err, &he) && he.Response.StatusCode == 400 {
				// A 400 means the broker is not decommissioning: it may have
				// finished and been removed, been recommissioned, or never
				// have started. The wait cannot complete, so stop with a clear
				// reason. A 400 is not transient, so it is not retried.
				breakInPlace(w, &printedInPlace)
				return fmt.Errorf("broker %d is not decommissioning", broker)
			}
			if retries <= 0 {
				breakInPlace(w, &printedInPlace)
				return fmt.Errorf("unable to request decommission status: %v", err)
			}
			retries--
			if sleepErr := sleepInterval(ctx, interval); sleepErr != nil {
				breakInPlace(w, &printedInPlace)
				return watchCtxError(sleepErr, broker, lastStatus, haveStatus, opts.waitTimeout)
			}
			continue
		}
		retries = 3
		lastStatus = dbs
		haveStatus = true

		if dbs.Finished {
			breakInPlace(w, &printedInPlace)
			return printDecommissionFinished(f, dbs, broker, opts.detailed, w)
		}

		if f.IsText() {
			if opts.detailed {
				types.Sort(dbs.Partitions)
				sort.Strings(dbs.AllocationFailures)
				resp := buildDecommissionStatus(dbs, true)
				printDecommissionStatus(f, resp, true, opts.human, w)
				fmt.Fprintln(w)
			} else if interactive {
				fmt.Fprintf(w, "\r\033[K%s", decommissionProgressLine(broker, dbs, opts.human))
				printedInPlace = true
			} else {
				fmt.Fprintln(w, decommissionProgressLine(broker, dbs, opts.human))
			}
		}

		if sleepErr := sleepInterval(ctx, interval); sleepErr != nil {
			breakInPlace(w, &printedInPlace)
			return watchCtxError(sleepErr, broker, lastStatus, haveStatus, opts.waitTimeout)
		}
	}
}

// printDecommissionFinished renders the terminal state of a finished
// decommission. It returns nil even when replicas are left: the node has
// still been decommissioned, so that is a warning rather than an error.
func printDecommissionFinished(f config.OutFormatter, dbs rpadmin.DecommissionStatusResponse, broker int, detailed bool, w io.Writer) error {
	if isText, _, s, err := f.Format(buildDecommissionStatus(dbs, detailed)); !isText {
		if err != nil {
			return fmt.Errorf("unable to print in the requested format %q: %v", f.Kind, err)
		}
		fmt.Fprintln(w, s)
		return nil
	}
	if dbs.ReplicasLeft == 0 {
		fmt.Fprintf(w, "Node %d is decommissioned successfully.\n", broker)
		return nil
	}
	fmt.Fprintf(w, "Node %d is decommissioned but there are %d replicas left, which may be an issue inside Redpanda. Please describe how you encountered this at https://github.com/redpanda-data/redpanda/issues/new?assignees=&labels=kind%%2Fbug&template=01_bug_report.md\n", broker, dbs.ReplicasLeft)
	return nil
}

// watchCtxError turns a context error (timeout or cancellation) into a
// user-facing error that reports last-known progress and where the
// decommission continues.
func watchCtxError(ctxErr error, broker int, last rpadmin.DecommissionStatusResponse, haveStatus bool, timeout time.Duration) error {
	progress := ""
	if haveStatus {
		p := computeDecommissionProgress(last)
		progress = fmt.Sprintf(" (%d%% complete, %d partitions remaining)%s", p.percent(), p.partitionsMoving, failureSuffix(last))
	}
	resume := fmt.Sprintf("; it continues on the cluster, check progress with 'rpk cluster brokers decommission-status %d'", broker)
	if errors.Is(ctxErr, context.DeadlineExceeded) {
		return fmt.Errorf("timed out after %s waiting for broker %d to decommission%s%s", timeout, broker, progress, resume)
	}
	return fmt.Errorf("stopped waiting for broker %d to decommission%s%s", broker, progress, resume)
}

// breakInPlace ends an in-place-updated status line with a newline so that a
// following message starts cleanly. It is a no-op if nothing was drawn in
// place.
func breakInPlace(w io.Writer, printedInPlace *bool) {
	if *printedInPlace {
		fmt.Fprintln(w)
		*printedInPlace = false
	}
}

func sleepInterval(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// installWaitFlags registers the shared --wait/--wait-timeout/--poll-interval
// flags on a decommission command.
func installWaitFlags(cmd *cobra.Command, opts *watchDecommissionOptions, wait *bool) {
	cmd.Flags().BoolVarP(wait, "wait", "w", false, "Wait until the decommission completes, printing progress as it goes")
	cmd.Flags().DurationVar(&opts.waitTimeout, "wait-timeout", 0, "How long to wait for the decommission to complete (0 means no limit); only used with --wait. The decommission continues on the cluster regardless")
	cmd.Flags().DurationVar(&opts.pollInterval, "poll-interval", defaultDecommissionPollInterval, "How often to poll the decommission status while waiting; only used with --wait")
}
