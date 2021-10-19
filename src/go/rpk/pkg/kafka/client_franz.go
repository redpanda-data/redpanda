// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package kafka

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/afero"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
)

// NewFranzClient returns a franz-go based kafka client.
func NewFranzClient(
	fs afero.Fs, p *config.Params, cfg *config.Config, extraOpts ...kgo.Opt,
) (*kgo.Client, error) {
	k := &cfg.Rpk.KafkaApi

	opts := []kgo.Opt{
		kgo.SeedBrokers(k.Brokers...),
		kgo.ClientID("rpk"),
		kgo.RetryTimeout(5 * time.Second),

		// Redpanda may indicate one leader just before rebalancing the
		// leader to a different server. During this rebalance,
		// Redpanda may return stale metadata. We always want fresh
		// metadata, and we want it fast since this is a CLI, so we
		// will use a small min metadata age.
		//
		// https://github.com/vectorizedio/redpanda/issues/2546
		kgo.MetadataMinAge(250 * time.Millisecond),
	}

	if k.SASL != nil {
		mech := scram.Auth{
			User: k.SASL.User,
			Pass: k.SASL.Password,
		}
		switch strings.ToUpper(k.SASL.Mechanism) {
		case "SCRAM-SHA-256":
			opts = append(opts, kgo.SASL(mech.AsSha256Mechanism()))
		case "SCRAM-SHA-512":
			opts = append(opts, kgo.SASL(mech.AsSha512Mechanism()))
		}
	}

	tc, err := k.TLS.Config(fs)
	if err != nil {
		return nil, err
	}
	if tc != nil {
		opts = append(opts, kgo.DialTLSConfig(tc))
	}

	if p.Verbose {
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, nil)))
	}

	opts = append(opts, extraOpts...)

	return kgo.NewClient(opts...)
}

// NewAdmin returns a franz-go admin client.
func NewAdmin(
	fs afero.Fs, p *config.Params, cfg *config.Config, extraOpts ...kgo.Opt,
) (*kadm.Client, error) {
	cl, err := NewFranzClient(fs, p, cfg, extraOpts...)
	if err != nil {
		return nil, err
	}
	adm := kadm.NewClient(cl)
	adm.SetTimeoutMillis(5000) // 5s timeout default for any timeout based request
	return adm, nil
}

// MetaString returns what we will print within rpk for kgo.BrokerMetadata.
func MetaString(meta kgo.BrokerMetadata) string {
	return fmt.Sprintf("%s (%d)", net.JoinHostPort(meta.Host, strconv.Itoa(int(meta.Port))), meta.NodeID)
}

// EachShard calls fn for each non-erroring response in shards. If some, but not
// all, requests fail, this prints a summary message.
func EachShard(
	req kmsg.Request, shards []kgo.ResponseShard, fn func(kgo.ResponseShard),
) (allFailed bool) {

	if len(shards) == 1 && shards[0].Err != nil {
		shard := shards[0]
		meta := ""
		if shard.Meta.NodeID >= 0 {
			meta = " to broker " + MetaString(shard.Meta)
		}
		fmt.Printf("(%s%s failure: %v)\n",
			kmsg.NameForKey(req.Key()),
			meta,
			shard.Err,
		)
		return true
	}

	var failures int

	for _, shard := range shards {
		if shard.Err != nil {
			failures++
			meta := ""
			if shard.Meta.NodeID >= 0 {
				meta = " to broker " + MetaString(shard.Meta)
			}
			fmt.Printf("(partial %s%s failure: %v)\n",
				kmsg.NameForKey(req.Key()),
				meta,
				shard.Err,
			)
			continue
		}
		fn(shard)
	}

	return failures == len(shards)
}

// MaybeErrMessage returns either an empty string if code is 0, or the short
// error message string corresponding to the Kafka error for code.
func MaybeErrMessage(code int16) string {
	var msg string
	if err := kerr.TypedErrorForCode(code); err != nil {
		msg = err.Message
	}
	return msg
}
