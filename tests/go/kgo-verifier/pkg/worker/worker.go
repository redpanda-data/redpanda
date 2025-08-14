package worker

import (
	"crypto/tls"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	metrics "github.com/rcrowley/go-metrics"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type HistogramSummary struct {
	P50 float64 `json:"p50"`
	P90 float64 `json:"p90"`
	P99 float64 `json:"p99"`
}

func SummarizeHistogram(h *metrics.Histogram) HistogramSummary {
	return HistogramSummary{
		P50: (*h).Percentile(0.5),
		P90: (*h).Percentile(0.90),
		P99: (*h).Percentile(0.99),
	}
}

type MessageStats struct {
	Produced int64
	Consumed int64
	Errors   int64

	// Note: the underlying metrics.Sample structure uses a
	// mutex on each insert :-/

	// Latency from calling produce to ack handler call
	Ack_latency metrics.Histogram

	// Latency from calling produce to receiving message in consumer
	E2e_latency metrics.Histogram

	// Server errors that caused us to tear down and rebuild the client
	// to proceed: these are potential redpanda bugs.
	Rebuilds int64
}

func NewMessageStats() MessageStats {
	s_ack := metrics.NewExpDecaySample(1024, 0.015)
	s_e2e := metrics.NewExpDecaySample(1024, 0.015)
	return MessageStats{
		Ack_latency: metrics.NewHistogram(s_ack),
		E2e_latency: metrics.NewHistogram(s_e2e),
		Errors:      0,
	}
}

type Result struct {
	Stats MessageStats
}

func (r *Result) String() string {
	var b strings.Builder

	fmt.Fprintf(&b, "Ack: 50/90/99 (us) : %.2f/%.2f/%.2fus",
		r.Stats.Ack_latency.Percentile(0.5),
		r.Stats.Ack_latency.Percentile(0.90),
		r.Stats.Ack_latency.Percentile(0.99),
	)
	fmt.Fprintf(&b, "E2e: 50/90/99 (us) : %.2f/%.2f/%.2fus",
		r.Stats.E2e_latency.Percentile(0.5),
		r.Stats.E2e_latency.Percentile(0.90),
		r.Stats.E2e_latency.Percentile(0.99),
	)

	return b.String()
}

type Outage struct {
	start time.Time
	end   time.Time
}

func (ms *MessageStats) Reset() {
	ms.Errors = 0
	ms.Ack_latency.Clear()
	ms.E2e_latency.Clear()
}

type Worker interface {
	GetStatus() (interface{}, *sync.Mutex)
	ResetStats()
}

type KeySpace struct {
	UniqueCount uint64
}

type ValueGenerator struct {
	PayloadSize          uint64
	Compressible         bool
	TombstoneProbability float64
	RandomData           []byte
	ProduceRandomBytes   bool
}

var compressible_payload []byte

func min(vars ...int) int {
	v := vars[0]
	for _, i := range vars {
		if i < v {
			v = i
		}
	}
	return v
}

func (vg *ValueGenerator) GenerateRandom() []byte {
	if vg.RandomData == nil {
		// 2mb should be enough
		data := make([]byte, 1<<21)
		for i := range data {
			// printable ascii range
			data[i] = byte(rand.Intn(126+1-32) + 32)
		}
		vg.RandomData = data
	}
	frag_size := 1024 // didn't seem to have much perf impact
	size := int(vg.PayloadSize)
	res := make([]byte, 0, size)
	for {
		remaining := size - len(res)
		if remaining == 0 {
			break
		}
		start := rand.Intn(size)
		view := vg.RandomData[start:]
		end := min(len(view), remaining, frag_size)
		res = append(res, view[:end]...)
	}
	return res
}

func (vg *ValueGenerator) GenerateCompressible() []byte {
	// Zeros, which is about as compressible as an array can be.
	if len(compressible_payload) == 0 {
		compressible_payload = make([]byte, vg.PayloadSize)
	} else if len(compressible_payload) != int(vg.PayloadSize) {
		// This is an implementation shortcut that lets us use a simple
		// global array of zeros for compressible payloads, as long
		// as everyone wants the same size.
		panic("Can't have multiple compressible generators of different sizes")
	}

	// Everyone who asks for compressible payload gets a ref to the same array
	// of zeros: this is worthwhile because a compressible producer might do
	// huge message sizes (e.g. 128MIB of zeros compresses down to <1MiB.
	return compressible_payload
}

func (vg *ValueGenerator) Generate() []byte {
	isTombstone := rand.Float64() < vg.TombstoneProbability
	if isTombstone {
		return nil
	}
	if vg.Compressible {
		return vg.GenerateCompressible()
	} else {
		return vg.GenerateRandom()
	}
}

type WorkerConfig struct {
	Name               string
	Brokers            string
	Trace              bool
	Topic              string
	Linger             time.Duration
	MaxBufferedRecords uint
	BatchMaxbytes      uint
	SaslUser           string
	SaslPass           string
	UseTls             bool
	Transactions       bool

	// A kafka `compression.type`, or `mixed` to use a random one for each worker (requires
	// that you have multiple workers to be meaningful)
	CompressionType string

	// If true, use a payload that compresses easily.  If false, use an
	// incompressible payload.
	CompressiblePayload bool

	TolerateDataLoss      bool
	TolerateFailedProduce bool
	Continuous            bool
	ValidateLatestValues  bool
}

func CompressionCodecFromString(s string) (kgo.CompressionCodec, error) {
	if s == "mixed" {
		i := 1 + rand.Uint32()%4
		if i == 1 {
			return kgo.GzipCompression(), nil
		} else if i == 2 {
			return kgo.SnappyCompression(), nil
		} else if i == 3 {
			return kgo.Lz4Compression(), nil
		} else if i == 4 {
			return kgo.ZstdCompression(), nil
		} else {
			panic("Unreachable")
		}
	} else {
		if s == "none" {
			return kgo.NoCompression(), nil
		} else if s == "gzip" {
			return kgo.GzipCompression(), nil
		} else if s == "snappy" {
			return kgo.SnappyCompression(), nil
		} else if s == "lz4" {
			return kgo.Lz4Compression(), nil
		} else if s == "zstd" {
			return kgo.ZstdCompression(), nil
		} else {
			return kgo.NoCompression(), fmt.Errorf("Unknown compression type %s", s)
		}
	}

}

func (wc *WorkerConfig) MakeKgoOpts() []kgo.Opt {
	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(wc.Brokers, ",")...),

		// Producer properties
		kgo.DefaultProduceTopic(wc.Topic),

		kgo.ProducerBatchMaxBytes(int32(wc.BatchMaxbytes)),
		kgo.MaxBufferedRecords(int(wc.MaxBufferedRecords)),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	}

	if wc.CompressionType != "" {
		codec, err := CompressionCodecFromString(wc.CompressionType)
		if err != nil {
			panic(err.Error())
		} else {
			opts = append(opts, kgo.ProducerBatchCompression(codec))
		}
	} else {
		// Default: do not compress.  This aligns with typical test/bench use cases
		// where we wanted a given message size to translate into the same sized I/O
		// on the server.
		opts = append(opts, kgo.ProducerBatchCompression(kgo.NoCompression()))
	}

	if wc.Name != "" {
		opts = append(opts, kgo.ClientID(wc.Name))

	}

	if wc.Transactions {
		opts = append(opts, []kgo.Opt{
			// By default kgo reads uncommited records and unstable offsets
			kgo.RequireStableFetchOffsets(),
			kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		}...)
	}

	// Disable auth if username not given
	if len(wc.SaslUser) > 0 {
		auth_mech := scram.Auth{
			User: wc.SaslUser,
			Pass: wc.SaslPass,
		}
		auth := auth_mech.AsSha256Mechanism()
		opts = append(opts,
			kgo.SASL(auth))
	}

	if wc.UseTls {
		tlsDialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}}
		opts = append(opts, kgo.Dialer(tlsDialer.DialContext))
	}

	if wc.Trace {
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, func() string {
			return fmt.Sprintf("time=\"%s\" name=%s", time.Now().UTC().Format(time.RFC3339), wc.Name)
		})))
	}

	return opts
}

func NewWorkerConfig(name string, brokers string, trace bool, topic string, linger time.Duration, maxBufferedRecords uint, transactions bool,
	compressionType string, commpressiblePayload bool, username string, password string, useTls bool) WorkerConfig {
	return WorkerConfig{
		Name:                name,
		Brokers:             brokers,
		Trace:               trace,
		Topic:               topic,
		Linger:              linger,
		MaxBufferedRecords:  maxBufferedRecords,
		SaslUser:            username,
		SaslPass:            password,
		UseTls:              useTls,
		Transactions:        transactions,
		CompressionType:     compressionType,
		CompressiblePayload: commpressiblePayload,
	}
}
