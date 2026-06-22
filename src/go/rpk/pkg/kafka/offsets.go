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
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

const (
	// offsetListRetryTimeout bounds how long an offset listing retries. It
	// matches the client RetryTimeout so offset listing does not retry any
	// longer than the client would for an ordinary request.
	offsetListRetryTimeout = 11 * time.Second
	offsetListRetryBackoff = 250 * time.Millisecond
)

// ListStartOffsetsWithRetries wraps (*kadm.Client).ListStartOffsets, retrying
// transient errors; see retryListOffsets for the retry semantics.
func ListStartOffsetsWithRetries(
	ctx context.Context, adm *kadm.Client, topics ...string,
) (kadm.ListedOffsets, error) {
	return retryListOffsets(ctx, func(ctx context.Context) (kadm.ListedOffsets, error) {
		return adm.ListStartOffsets(ctx, topics...)
	})
}

// ListEndOffsetsWithRetries wraps (*kadm.Client).ListEndOffsets, retrying
// transient errors; see retryListOffsets for the retry semantics.
func ListEndOffsetsWithRetries(
	ctx context.Context, adm *kadm.Client, topics ...string,
) (kadm.ListedOffsets, error) {
	return retryListOffsets(ctx, func(ctx context.Context) (kadm.ListedOffsets, error) {
		return adm.ListEndOffsets(ctx, topics...)
	})
}

// ListOffsetsAfterMilliWithRetries wraps (*kadm.Client).ListOffsetsAfterMilli,
// retrying transient errors; see retryListOffsets for the retry semantics.
func ListOffsetsAfterMilliWithRetries(
	ctx context.Context, adm *kadm.Client, milli int64, topics ...string,
) (kadm.ListedOffsets, error) {
	return retryListOffsets(ctx, func(ctx context.Context) (kadm.ListedOffsets, error) {
		return adm.ListOffsetsAfterMilli(ctx, milli, topics...)
	})
}

// retryListOffsets runs a kadm offset-listing call, retrying for a bounded time
// while it fails with only retriable errors. A broker can briefly return a
// retriable error such as LEADER_NOT_AVAILABLE for a partition whose leader it
// has not yet learned -- for example right after topic creation or while
// leadership moves between brokers -- which a one-shot listing would otherwise
// surface as an outright failure. The error may be a top-level shard error or a
// per-partition error in the returned offsets; both are retried.
func retryListOffsets(
	ctx context.Context,
	list func(context.Context) (kadm.ListedOffsets, error),
) (kadm.ListedOffsets, error) {
	deadline := time.Now().Add(offsetListRetryTimeout)
	for {
		l, err := list(ctx)
		if err == nil {
			err = l.Error()
		}
		if err == nil || time.Now().After(deadline) || !offsetListRetriable(err) {
			return l, err
		}
		select {
		case <-ctx.Done():
			return l, err
		case <-time.After(offsetListRetryBackoff):
		}
	}
}

// offsetListRetriable reports whether an offset-listing failure is composed
// entirely of retriable Kafka errors.
func offsetListRetriable(err error) bool {
	var se *kadm.ShardErrors
	if errors.As(err, &se) {
		if len(se.Errs) == 0 {
			return false
		}
		for _, e := range se.Errs {
			if !kerr.IsRetriable(e.Err) {
				return false
			}
		}
		return true
	}
	return kerr.IsRetriable(err)
}
