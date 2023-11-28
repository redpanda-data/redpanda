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

package consume

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/inhies/go-bytesize"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/time/rate"
	"redpanda.com/testing/transform-verifier/common"
)

var (
	bps = 512 * bytesize.KB
)

type ConsumeStatus struct {
	LatestSeqnos   map[int]uint64 `json:"latest_seqnos"`
	InvalidRecords int            `json:"invalid_records"`
	ErrorCount     int            `json:"error_count"`
}

func (self ConsumeStatus) Merge(other ConsumeStatus) ConsumeStatus {
	combined := make(map[int]uint64)
	for k, v := range self.LatestSeqnos {
		combined[k] = max(v, combined[k])
	}
	for k, v := range other.LatestSeqnos {
		combined[k] = max(v, combined[k])
	}
	return ConsumeStatus{
		LatestSeqnos:   combined,
		InvalidRecords: self.InvalidRecords + other.InvalidRecords,
		ErrorCount:     self.ErrorCount + other.ErrorCount,
	}
}

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "consume",
		Short: "Consume messages from a output topic",
		Run: func(cmd *cobra.Command, args []string) {
			slog.Info("start consume")
			if err := consume(cmd.Context()); err != nil {
				common.Die("unable to consume: %v", err)
			}
			slog.Info("end consume")
		},
	}
	cmd.Flags().Var(&bps, "bytes-per-second", "How much to read per second")
	return cmd
}

func createReporter(ctx context.Context) func(ConsumeStatus) {
	l := &sync.Mutex{}
	latest := ConsumeStatus{}
	return func(update ConsumeStatus) {
		l.Lock()
		defer l.Unlock()
		latest = latest.Merge(update)
		common.ReportStatus(ctx, latest)
	}
}

// recordValidator takes a record and validates the record makes
// progress (ie is not a duplicate because of at least once
// processing) and an error if the record was invalid.
//
// The latest seqno is returned
type recordValidator = func(*kgo.Record) (uint64, error)

func createRecordValidator() recordValidator {
	latestSeqno := uint64(0)
	return func(r *kgo.Record) (uint64, error) {
		seqno, err := common.FindSeqnoHeader(r)
		if err != nil {
			return 0, fmt.Errorf("missing seqno header: %v", err)
		}
		// Make sure we initialize the seqno correctly (we can start from any offset).
		if latestSeqno == 0 {
			latestSeqno = seqno
			return latestSeqno, nil
		}
		// Do to at least once processing we can get duplicates, so equal seqno is fine
		// Additionally because we commit async, it's possible we rewind multiple seqno.
		// The real thing we can guarantee is that once we get a new seqno, there are no
		// gaps we've seen.
		if seqno > latestSeqno && latestSeqno+1 != seqno {
			return latestSeqno, fmt.Errorf("detected missing seqno: partition=%d seqno=%d latest=%d", r.Partition, seqno, latestSeqno)
		}
		latestSeqno = max(seqno, latestSeqno)
		return latestSeqno, nil
	}
}

// consume goes as fast as possible according to the settings and rate limit to consume records from the broker.
//
// if ctx is cancelled, then this function always returns nil.
func consume(ctx context.Context) error {
	client, err := common.NewClient(
		kgo.FetchMaxBytes(int32(bps)),
	)
	if err != nil {
		return fmt.Errorf("unable to create client: %v", err)
	}
	defer client.Close()
	rateLimiter := rate.NewLimiter(rate.Limit(bps), int(bps))
	reporter := createReporter(ctx)
	validatorByPartition := make(map[int]recordValidator)
	reporter(ConsumeStatus{})
	// Consume until we're cancelled
	for ctx.Err() == nil {
		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			// If we've been cancelled during the poll, stop
			break
		}
		errorCount := 0
		fetches.EachError(func(topic string, partition int32, err error) {
			slog.Error("consume error", "topic", topic, "partition", partition, "err", err)
			errorCount++
		})
		seqnos := make(map[int]uint64)
		bytes := 0
		invalidRecords := 0
		fetches.EachPartition(func(ftp kgo.FetchTopicPartition) {
			p := int(ftp.Partition)
			validator := validatorByPartition[p]
			if validator == nil {
				validator = createRecordValidator()
				validatorByPartition[p] = validator
			}
			for _, r := range ftp.Records {
				bytes += common.RecordSize(r)
				latestSeqno, err := validator(r)
				if err != nil {
					slog.Warn("invalid record", "err", err)
					invalidRecords++
				}
				seqnos[p] = max(seqnos[p], latestSeqno)
			}
		})
		reporter(ConsumeStatus{
			ErrorCount:     errorCount,
			LatestSeqnos:   seqnos,
			InvalidRecords: invalidRecords,
		})
		// Rate limit before fetching again
		if err := rateLimiter.WaitN(ctx, bytes); err != nil && ctx.Done() == nil {
			return err
		}
	}
	return nil
}
