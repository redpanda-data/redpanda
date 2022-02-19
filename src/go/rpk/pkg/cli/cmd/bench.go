// Copyright 2022 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/plugin/kprom"
	"github.com/twmb/tlscfg"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/out"
)

func printRate(rateRecs *int64, rateBytes *int64) {
	for range time.Tick(time.Second) {
		recs := atomic.SwapInt64(rateRecs, 0)
		bytes := atomic.SwapInt64(rateBytes, 0)
		fmt.Printf("%0.2f MiB/s; %0.2fk records/s\n", float64(bytes)/(1024*1024), float64(recs)/1000)
	}
}

func newRecord(
	num int64,
	useStaticValue bool,
	staticPool sync.Pool,
	pool sync.Pool,
	poolProduce bool,
	recordBytes int,
) *kgo.Record {
	var r *kgo.Record
	if useStaticValue {
		return staticPool.Get().(*kgo.Record)
	} else if poolProduce {
		r = pool.Get().(*kgo.Record)
	} else {
		r = kgo.SliceRecord(make([]byte, recordBytes))
	}
	formatValue(num, r.Value)
	return r
}

func formatValue(num int64, v []byte) {
	var buf [20]byte // max int64 takes 19 bytes, then we add a space
	b := strconv.AppendInt(buf[:0], num, 10)
	b = append(b, ' ')

	n := copy(v, b)
	for n != len(v) {
		n += copy(v[n:], b)
	}
}

func NewBenchCommand(fs afero.Fs) *cobra.Command {
	var (
		seedBrokers string
		topic       string
		pprofPort   string
		prom        bool

		useStaticValue bool

		recordBytes   int
		compression   string
		poolProduce   bool
		noIdempotency bool
		linger        time.Duration
		batchMaxBytes int

		logLevel string

		consume bool
		group   string

		dialTLS  bool
		caFile   string
		certFile string
		keyFile  string

		saslMethod string
		saslUser   string
		saslPass   string

		rateRecs  int64
		rateBytes int64

		staticValue []byte
		staticPool  = sync.Pool{New: func() interface{} { return kgo.SliceRecord(staticValue) }}
		pool        = sync.Pool{New: func() interface{} { return kgo.SliceRecord(make([]byte, recordBytes)) }}
	)

	command := &cobra.Command{
		Use:   "bench",
		Short: "Benchmark the Redpanda cluster",
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			var customTLS bool
			if caFile != "" || certFile != "" || keyFile != "" {
				dialTLS = true
				customTLS = true
			}

			if recordBytes <= 0 {
				out.Die("record bytes must be larger than zero")
			}

			if useStaticValue {
				staticValue = make([]byte, recordBytes)
				formatValue(0, staticValue)
			}

			opts := []kgo.Opt{
				kgo.SeedBrokers(strings.Split(seedBrokers, ",")...),
				kgo.DefaultProduceTopic(topic),
				kgo.MaxBufferedRecords(250<<20/recordBytes + 1),
				kgo.MaxConcurrentFetches(3),
				// We have good compression, so we want to limit what we read
				// back because snappy deflation will balloon our memory usage.
				kgo.FetchMaxBytes(5 << 20),
				kgo.ProducerBatchMaxBytes(int32(batchMaxBytes)),
			}

			if noIdempotency {
				opts = append(opts, kgo.DisableIdempotentWrite())
			}
			if consume {
				opts = append(opts, kgo.ConsumeTopics(topic))
				if group != "" {
					opts = append(opts, kgo.ConsumerGroup(group))
				}
			}

			if prom {
				metrics := kprom.NewMetrics("kgo")
				http.Handle("/metrics", metrics.Handler())
				opts = append(opts, kgo.WithHooks(metrics))
			}

			switch strings.ToLower(logLevel) {
			case "":
			case "debug":
				opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, nil)))
			case "info":
				opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, nil)))
			case "warn":
				opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelWarn, nil)))
			case "error":
				opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelError, nil)))
			default:
				out.Die("unrecognized log level %s", logLevel)
			}

			if linger != 0 {
				opts = append(opts, kgo.ProducerLinger(linger))
			}
			switch strings.ToLower(compression) {
			case "", "none":
				opts = append(opts, kgo.ProducerBatchCompression(kgo.NoCompression()))
			case "gzip":
				opts = append(opts, kgo.ProducerBatchCompression(kgo.GzipCompression()))
			case "snappy":
				opts = append(opts, kgo.ProducerBatchCompression(kgo.SnappyCompression()))
			case "lz4":
				opts = append(opts, kgo.ProducerBatchCompression(kgo.Lz4Compression()))
			case "zstd":
				opts = append(opts, kgo.ProducerBatchCompression(kgo.ZstdCompression()))
			default:
				out.Die("unrecognized compression %s", compression)
			}

			if dialTLS {
				if customTLS {
					tc, err := tlscfg.New(
						tlscfg.MaybeWithDiskCA(caFile, tlscfg.ForClient),
						tlscfg.MaybeWithDiskKeyPair(certFile, keyFile),
					)
					if err != nil {
						out.Die("unable to create tls config: %v", err)
					}
					opts = append(opts, kgo.DialTLSConfig(tc))
				} else {
					opts = append(opts, kgo.DialTLSConfig(new(tls.Config)))
				}
			}

			if saslMethod != "" || saslUser != "" || saslPass != "" {
				if saslMethod == "" || saslUser == "" || saslPass == "" {
					out.Die("all of -sasl-method, -sasl-user, -sasl-pass must be specified if any are")
				}
				method := strings.ToLower(saslMethod)
				method = strings.ReplaceAll(method, "-", "")
				method = strings.ReplaceAll(method, "_", "")
				switch method {
				case "plain":
					opts = append(opts, kgo.SASL(plain.Auth{
						User: saslUser,
						Pass: saslPass,
					}.AsMechanism()))
				case "scramsha256":
					opts = append(opts, kgo.SASL(scram.Auth{
						User: saslUser,
						Pass: saslPass,
					}.AsSha256Mechanism()))
				case "scramsha512":
					opts = append(opts, kgo.SASL(scram.Auth{
						User: saslUser,
						Pass: saslPass,
					}.AsSha512Mechanism()))
				case "awsmskiam":
					opts = append(opts, kgo.SASL(aws.Auth{
						AccessKey: saslUser,
						SecretKey: saslPass,
					}.AsManagedStreamingIAMMechanism()))
				default:
					out.Die("unrecognized sasl option %s", saslMethod)
				}
			}

			cl, err := kafka.NewFranzClient(fs, p, cfg, opts...)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)

			if pprofPort != "" {
				go func() {
					err := http.ListenAndServe(pprofPort, nil)
					out.MaybeDie(err, "unable to run pprof listener: %v", err)
				}()
			}

			go printRate(&rateRecs, &rateBytes)

			switch consume {
			case false:
				var num int64
				for {
					cl.Produce(
						context.Background(),
						newRecord(num, useStaticValue, staticPool, pool, poolProduce, recordBytes),
						func(r *kgo.Record, err error) {
							if useStaticValue {
								staticPool.Put(r)
							} else if poolProduce {
								pool.Put(r)
							}
							out.MaybeDie(err, "produce error: %v", err)
							atomic.AddInt64(&rateRecs, 1)
							atomic.AddInt64(&rateBytes, int64(recordBytes))
						},
					)
					num++
				}
			case true:
				for {
					fetches := cl.PollFetches(context.Background())
					fetches.EachError(func(t string, p int32, err error) {
						out.MaybeDie(err, "topic %s partition %d had error: %v", t, p, err)
					})
					var recs int64
					var bytes int64
					fetches.EachRecord(func(r *kgo.Record) {
						recs++
						bytes += int64(len(r.Value))
					})
					atomic.AddInt64(&rateRecs, recs)
					atomic.AddInt64(&rateBytes, bytes)
				}
			}
		},
	}
	command.Flags().StringVar(&seedBrokers, "brokers", "localhost:9092", "comma delimited list of seed brokers")
	command.Flags().StringVar(&topic, "topic", "", "topic to produce to or consume from")
	command.Flags().StringVar(&pprofPort, "pprof", ":9876", "port to bind to for pprof, if non-empty")
	command.Flags().BoolVar(&prom, "prometheus", false, "if true, install a /metrics path for prometheus metrics to the default handler (usage requires -pprof)")

	command.Flags().BoolVar(&useStaticValue, "static-record", false, "if true, use the same record value for every record (eliminates creating and formatting values for records; implies -pool)")

	command.Flags().IntVar(&recordBytes, "record-bytes", 100, "bytes per record value (producing)")
	command.Flags().StringVar(&compression, "compression", "none", "compression algorithm to use (none,gzip,snappy,lz4,zstd, for producing)")
	command.Flags().BoolVar(&poolProduce, "pool", false, "if true, use a sync.Pool to reuse record structs/slices (producing)")
	command.Flags().BoolVar(&noIdempotency, "disable-idempotency", false, "if true, disable idempotency (force 1 produce rps)")
	command.Flags().DurationVar(&linger, "linger", 0, "if non-zero, linger to use when producing")
	command.Flags().IntVar(&batchMaxBytes, "batch-max-bytes", 1000000, "the maximum batch size to allow per-partition (must be less than Kafka's max.message.bytes, producing)")

	command.Flags().StringVar(&logLevel, "log-level", "", "if non-empty, use a basic logger with this log level (debug, info, warn, error)")

	command.Flags().BoolVar(&consume, "consume", false, "if true, consume rather than produce")
	command.Flags().StringVar(&group, "group", "", "if non-empty, group to use for consuming rather than direct partition consuming (consuming)")

	command.Flags().BoolVar(&dialTLS, "tls", false, "if true, use tls for connecting (if using well-known TLS certs)")
	command.Flags().StringVar(&caFile, "ca-cert", "", "if non-empty, path to CA cert to use for TLS (implies -tls)")
	command.Flags().StringVar(&certFile, "client-cert", "", "if non-empty, path to client cert to use for TLS (requires -client-key, implies -tls)")
	command.Flags().StringVar(&keyFile, "client-key", "", "if non-empty, path to client key to use for TLS (requires -client-cert, implies -tls)")

	command.Flags().StringVar(&saslMethod, "sasl-method", "", "if non-empty, sasl method to use (must specify all options; supports plain, scram-sha-256, scram-sha-512, aws_msk_iam)")
	command.Flags().StringVar(&saslUser, "sasl-user", "", "if non-empty, username to use for sasl (must specify all options)")
	command.Flags().StringVar(&saslPass, "sasl-pass", "", "if non-empty, password to use for sasl (must specify all options)")

	return command
}
