// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package kafka

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth/providers/auth0"
	"github.com/spf13/afero"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	koauth "github.com/twmb/franz-go/pkg/sasl/oauth"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/plugin/kzap"
)

// NewFranzClient returns a franz-go based kafka client.
func NewFranzClient(fs afero.Fs, p *config.RpkProfile, extraOpts ...kgo.Opt) (*kgo.Client, error) {
	k := &p.KafkaAPI

	d := p.Defaults()
	if len(k.Brokers) == 0 && d.NoDefaultCluster {
		return nil, errors.New("no brokers specified and rpk.yaml is configured to not use a default cluster")
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(k.Brokers...),
		kgo.ClientID("rpk"),

		// We want our timeouts to be _short_ but still allow for
		// slowness if people use rpk against a remote cluster.
		//
		// 3s dial timeout (overriding default 10s): dialing should be
		// quick.
		//
		// 5s request timeout overhead (overriding default 10s): we
		// want to kill requests that hang. The timeout is on top of
		// any request's timeout field, so this only affects requests
		// that *should* be fast. See #6317 for why we want to adjust
		// this down.
		//
		// 11s retry timeout (overriding default 30s): we do not want
		// to retry too much and keep hanging.
		//
		// TODO: we should lower these limits even more (2s, 4s, 9s)
		// once we support -X and then add these as configurable
		// options. We cannot be "aggressively" low without override
		// options because we may affect end users.
		kgo.DialTimeout(3 * time.Second),
		kgo.RequestTimeoutOverhead(5 * time.Second),
		kgo.RetryTimeout(11 * time.Second), // if updating this, update below's SetTimeoutMillis

		// Redpanda may indicate one leader just before rebalancing the
		// leader to a different server. During this rebalance,
		// Redpanda may return stale metadata. We always want fresh
		// metadata, and we want it fast since this is a CLI, so we
		// will use a small min metadata age.
		//
		// https://github.com/redpanda-data/redpanda/issues/2546
		kgo.MetadataMinAge(250 * time.Millisecond),
	}

	// We apply user overrides after our defaults above. Options are
	// applied in order, so appending at the end overrides anything
	// above.
	if d := d.DialTimeout; d.Duration != 0 {
		opts = append(opts, kgo.DialTimeout(d.Duration))
	}
	if d := d.RequestTimeoutOverhead; d.Duration != 0 {
		opts = append(opts, kgo.RequestTimeoutOverhead(d.Duration))
	}
	if d := d.RetryTimeout; d.Duration != 0 {
		opts = append(opts, kgo.RetryTimeout(d.Duration))
	}
	if d := d.FetchMaxWait; d.Duration != 0 {
		opts = append(opts, kgo.FetchMaxWait(d.Duration))
	}
	if id := d.KafkaProtocolReqClientID; id != "" {
		opts = append(opts, kgo.ClientID(id))
	}

	if k.SASL != nil {
		if k.SASL.Mechanism == adminapi.CloudOIDC {
			a := p.CurrentAuth()
			if a == nil || a.AuthToken == "" {
				return nil, errors.New("no current auth found, please login with 'rpk cloud login'")
			}
			expired, err := oauth.ValidateToken(
				a.AuthToken,
				auth0.NewClient(p.DevOverrides()).Audience(),
				a.ClientID,
			)
			if err != nil {
				if errors.Is(err, oauth.ErrMissingToken) {
					return nil, err
				}
				return nil, fmt.Errorf("unable to validate cloud token, please login again using 'rpk cloud login': %v", err)
			}
			if expired {
				return nil, fmt.Errorf("your cloud token has expired, please login again using 'rpk cloud login'")
			}
			opts = append(opts, kgo.SASL((koauth.Auth{
				Token: a.AuthToken,
			}).AsMechanism()))
		} else {
			a := scram.Auth{
				User: k.SASL.User,
				Pass: k.SASL.Password,
			}
			switch name := strings.ToUpper(k.SASL.Mechanism); name {
			case "SCRAM-SHA-256", "": // we default to SCRAM-SHA-256 -- people commonly specify user & pass without --sasl-mechanism
				opts = append(opts, kgo.SASL(a.AsSha256Mechanism()))
			case "SCRAM-SHA-512":
				opts = append(opts, kgo.SASL(a.AsSha512Mechanism()))
			case "PLAIN":
				opts = append(opts, kgo.SASL((&plain.Auth{
					User: k.SASL.User,
					Pass: k.SASL.Password,
				}).AsMechanism()))
			default:
				return nil, fmt.Errorf("unknown SASL mechanism %q, supported: [SCRAM-SHA-256, SCRAM-SHA-512, PLAIN]", name)
			}
		}
	}

	tc, err := k.TLS.Config(fs)
	if err != nil {
		return nil, err
	}
	if tc != nil {
		opts = append(opts, kgo.DialTLSConfig(tc))
	}
	opts = append(opts, kgo.WithLogger(kzap.New(p.Logger())))
	opts = append(opts, extraOpts...)

	return kgo.NewClient(opts...)
}

// NewAdmin returns a franz-go admin client.
func NewAdmin(fs afero.Fs, p *config.RpkProfile, extraOpts ...kgo.Opt) (*kadm.Client, error) {
	cl, err := NewFranzClient(fs, p, extraOpts...)
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
func EachShard(req kmsg.Request, shards []kgo.ResponseShard, fn func(kgo.ResponseShard)) (allFailed bool) {
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

// ErrMessage returns the Message if err is a *kerr.Error, otherwise the error
// text.
func ErrMessage(err error) string {
	if err == nil {
		return ""
	}
	if ke := (*kerr.Error)(nil); errors.As(err, &ke) {
		return ke.Message
	}
	return err.Error()
}
