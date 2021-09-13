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
	"strconv"
	"strings"
	"time"

	"github.com/spf13/afero"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
)

// NewFranzClient returns a franz-go based kafka client.
//
// The settings are close to, but not identical to the sarama client
// configuration.  Particularly, our timeouts are higher.
func NewFranzClient(fs afero.Fs, cfg *config.Config) (*kgo.Client, error) {
	k := &cfg.Rpk.KafkaApi

	opts := []kgo.Opt{
		kgo.SeedBrokers(k.Brokers...),
		kgo.ClientID("rpk"),
		kgo.RetryTimeout(5 * time.Second),

		kgo.ProduceRequestTimeout(5 * time.Second),
		kgo.RecordDeliveryTimeout(8 * time.Second),
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

	return kgo.NewClient(opts...)
}

// MetaString returns what we will print within rpk for kgo.BrokerMetadata.
func MetaString(meta kgo.BrokerMetadata) string {
	return net.JoinHostPort(meta.Host, strconv.Itoa(int(meta.Port)))
}

// PrintShardError prints a standard message for shard failures in sharded
// requests.
func PrintShardError(req kmsg.Request, shard kgo.ResponseShard) {
	fmt.Printf("Unable to issue %s request to broker %s: %v\n",
		kmsg.NameForKey(req.Key()),
		MetaString(shard.Meta),
		shard.Err,
	)
}

// EachShard calls fn for each non-erroring response in shards. Any errored
// response calls PrintShardError, and this returns if all shards failed.
func EachShard(
	req kmsg.Request, shards []kgo.ResponseShard, fn func(kgo.ResponseShard),
) (allFailed bool) {
	var failures int
	for _, shard := range shards {
		if shard.Err != nil {
			PrintShardError(req, shard)
			failures++
			continue
		}
		fn(shard)
	}
	return failures == len(shards)
}
