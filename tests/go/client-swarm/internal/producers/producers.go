package producers

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	randv2 "math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redpanda-data/redpanda/tests/go/client-swarm/internal/metrics"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/time/rate"
)

// Options for producer configuration
type Options struct {
	Brokers              []string
	Topic                string
	UniqueTopics         bool
	Count                int
	Messages             int64
	MessagesPerSecond    float64
	MessagePeriod        time.Duration
	Properties           []string
	CompressionType      string
	MinRecordSize        int
	MaxRecordSize        int
	CompressiblePayload  bool
	Keys                 int64
	ClientSpawnWait      time.Duration
	Timeout              time.Duration
	TopicsPerClient      int
	PayloadDirectory     string
	BatchMaxBytes        int
	MaxBufferedRecords   int
	Acks                 string
}

// Stats contains producer statistics
type Stats struct {
	TotalMessages int64
	TotalErrors   int64
	TotalBytes    int64
	Duration      time.Duration
	MinRate       float64
	MaxRate       float64
	AvgRate       float64
}

// DirectoryPayloadSource loads payloads from files
type DirectoryPayloadSource struct {
	payloads [][]byte
}

// NewDirectoryPayloadSource creates a new payload source from a directory
func NewDirectoryPayloadSource(dir string) (*DirectoryPayloadSource, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read payload directory: %w", err)
	}

	var payloads [][]byte
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".data") {
			continue
		}

		path := filepath.Join(dir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("failed to read payload file %s: %w", path, err)
		}
		payloads = append(payloads, data)
		log.Printf("Loaded payload file: %s (%d bytes)", path, len(data))
	}

	if len(payloads) == 0 {
		return nil, fmt.Errorf("no .data files found in %s", dir)
	}

	log.Printf("Loaded %d payload files", len(payloads))
	return &DirectoryPayloadSource{payloads: payloads}, nil
}

// GetPayload returns a random payload
func (d *DirectoryPayloadSource) GetPayload() []byte {
	if len(d.payloads) == 0 {
		return nil
	}
	return d.payloads[randv2.IntN(len(d.payloads))]
}

// PayloadGenerator generates message payloads
type PayloadGenerator struct {
	minSize             int
	maxSize             int
	compressible        bool
	compressibleBuffer  []byte
	directorySource     *DirectoryPayloadSource
}

// NewPayloadGenerator creates a new payload generator
func NewPayloadGenerator(minSize, maxSize int, compressible bool, dirSource *DirectoryPayloadSource) *PayloadGenerator {
	pg := &PayloadGenerator{
		minSize:         minSize,
		maxSize:         maxSize,
		compressible:    compressible,
		directorySource: dirSource,
	}

	if compressible {
		// Create a buffer of highly compressible data (all 0x0f)
		pg.compressibleBuffer = make([]byte, maxSize)
		for i := range pg.compressibleBuffer {
			pg.compressibleBuffer[i] = 0x0f
		}
	}

	return pg
}

// Generate generates a payload
func (pg *PayloadGenerator) Generate() []byte {
	// If we have a directory source, use it
	if pg.directorySource != nil {
		return pg.directorySource.GetPayload()
	}

	// Determine size
	size := pg.minSize
	if pg.maxSize > pg.minSize {
		size = pg.minSize + randv2.IntN(pg.maxSize-pg.minSize+1)
	}

	if pg.compressible {
		return pg.compressibleBuffer[:size]
	}

	// Generate random data
	data := make([]byte, size)
	rand.Read(data)
	return data
}

// parseCompression converts compression string to franz-go codec
func parseCompression(s string) kgo.CompressionCodec {
	switch strings.ToLower(s) {
	case "gzip":
		return kgo.GzipCompression()
	case "snappy":
		return kgo.SnappyCompression()
	case "lz4":
		return kgo.Lz4Compression()
	case "zstd":
		return kgo.ZstdCompression()
	case "none", "":
		return kgo.NoCompression()
	default:
		return kgo.NoCompression()
	}
}

// getRandomCompression returns a random compression codec
func getRandomCompression() kgo.CompressionCodec {
	compressions := []string{"gzip", "snappy", "lz4", "zstd"}
	return parseCompression(compressions[randv2.IntN(len(compressions))])
}

// parseAcks converts acks string to franz-go acks setting
func parseAcks(s string) kgo.Acks {
	switch strings.ToLower(s) {
	case "all", "-1":
		return kgo.AllISRAcks()
	case "leader", "1":
		return kgo.LeaderAck()
	case "none", "0":
		return kgo.NoAck()
	default:
		return kgo.AllISRAcks()
	}
}

// parseProperties parses key=value properties into franz-go options
func parseProperties(props []string) []kgo.Opt {
	var opts []kgo.Opt
	for _, prop := range props {
		parts := strings.SplitN(prop, "=", 2)
		if len(parts) != 2 {
			continue
		}
		// franz-go doesn't use librdkafka-style properties
		// Map common ones if needed
		log.Printf("Property %s=%s (note: franz-go uses native Go options)", parts[0], parts[1])
	}
	return opts
}

// producerStats holds stats for a single producer
type producerStats struct {
	messages int64
	errors   int64
	bytes    int64
	rate     float64
}

// Run runs the producer swarm
func Run(ctx context.Context, opts Options, metricsCtx *metrics.Context) (*Stats, error) {
	// Load payloads from directory if specified
	var dirSource *DirectoryPayloadSource
	if opts.PayloadDirectory != "" {
		var err error
		dirSource, err = NewDirectoryPayloadSource(opts.PayloadDirectory)
		if err != nil {
			return nil, err
		}
	}

	// Create payload generator
	payloadGen := NewPayloadGenerator(
		opts.MinRecordSize,
		opts.MaxRecordSize,
		opts.CompressiblePayload,
		dirSource,
	)

	// Calculate rate limit
	var limiter *rate.Limiter
	if opts.MessagesPerSecond > 0 {
		limiter = rate.NewLimiter(rate.Limit(opts.MessagesPerSecond), 1)
	} else if opts.MessagePeriod > 0 {
		rps := 1.0 / opts.MessagePeriod.Seconds()
		limiter = rate.NewLimiter(rate.Limit(rps), 1)
	}

	startTime := time.Now()
	var wg sync.WaitGroup
	statsCh := make(chan producerStats, opts.Count)

	log.Printf("Starting %d producers", opts.Count)

	for i := 0; i < opts.Count; i++ {
		wg.Add(1)
		producerID := i

		// Determine topic(s) for this producer
		topics := getTopics(opts.Topic, opts.UniqueTopics, producerID, opts.TopicsPerClient)

		// Determine compression
		var compression kgo.CompressionCodec
		if strings.ToLower(opts.CompressionType) == "mixed" {
			compression = getRandomCompression()
		} else {
			compression = parseCompression(opts.CompressionType)
		}

		go func(id int, topics []string, compression kgo.CompressionCodec) {
			defer wg.Done()
			stats := runProducer(ctx, id, topics, opts, payloadGen, compression, limiter, metricsCtx)
			statsCh <- stats
		}(producerID, topics, compression)

		// Staggered spawning
		if opts.ClientSpawnWait > 0 && i < opts.Count-1 {
			select {
			case <-ctx.Done():
				break
			case <-time.After(opts.ClientSpawnWait):
			}
		}
	}

	// Wait for all producers to finish
	go func() {
		wg.Wait()
		close(statsCh)
	}()

	// Collect stats
	var totalMessages, totalErrors, totalBytes int64
	var rates []float64

	for stats := range statsCh {
		totalMessages += stats.messages
		totalErrors += stats.errors
		totalBytes += stats.bytes
		if stats.rate > 0 {
			rates = append(rates, stats.rate)
		}
	}

	duration := time.Since(startTime)

	// Calculate rate statistics
	var minRate, maxRate, avgRate float64
	if len(rates) > 0 {
		minRate = rates[0]
		maxRate = rates[0]
		sum := 0.0
		for _, r := range rates {
			if r < minRate {
				minRate = r
			}
			if r > maxRate {
				maxRate = r
			}
			sum += r
		}
		avgRate = sum / float64(len(rates))
	}

	return &Stats{
		TotalMessages: totalMessages,
		TotalErrors:   totalErrors,
		TotalBytes:    totalBytes,
		Duration:      duration,
		MinRate:       minRate,
		MaxRate:       maxRate,
		AvgRate:       avgRate,
	}, nil
}

func getTopics(baseTopic string, unique bool, producerID, topicsPerClient int) []string {
	if !unique {
		return []string{baseTopic}
	}

	topics := make([]string, topicsPerClient)
	for i := 0; i < topicsPerClient; i++ {
		topics[i] = fmt.Sprintf("%s-%d-%d", baseTopic, producerID, i)
	}
	return topics
}

func runProducer(
	ctx context.Context,
	id int,
	topics []string,
	opts Options,
	payloadGen *PayloadGenerator,
	compression kgo.CompressionCodec,
	limiter *rate.Limiter,
	metricsCtx *metrics.Context,
) producerStats {
	// Build client options
	clientOpts := []kgo.Opt{
		kgo.SeedBrokers(opts.Brokers...),
		kgo.ProducerBatchCompression(compression),
		kgo.RequiredAcks(parseAcks(opts.Acks)),
		kgo.ProducerBatchMaxBytes(int32(opts.BatchMaxBytes)),
		kgo.MaxBufferedRecords(opts.MaxBufferedRecords),
		kgo.ProduceRequestTimeout(opts.Timeout),
		kgo.RecordDeliveryTimeout(opts.Timeout * 3),
	}

	// Add any custom properties
	clientOpts = append(clientOpts, parseProperties(opts.Properties)...)

	client, err := kgo.NewClient(clientOpts...)
	if err != nil {
		log.Printf("Producer %d: failed to create client: %v", id, err)
		return producerStats{errors: 1}
	}
	defer client.Close()

	metricsCtx.RecordClientStart()
	defer metricsCtx.RecordClientStop()

	var messages, errors, bytes int64
	startTime := time.Now()

	// Message counter for unlimited mode
	var msgCount int64

	for {
		select {
		case <-ctx.Done():
			goto done
		default:
		}

		// Check if we've sent enough messages
		if opts.Messages > 0 && atomic.LoadInt64(&msgCount) >= opts.Messages {
			goto done
		}

		// Rate limit
		if limiter != nil {
			if err := limiter.Wait(ctx); err != nil {
				goto done
			}
		}

		// Generate payload
		payload := payloadGen.Generate()

		// Generate key
		keyNum, _ := rand.Int(rand.Reader, big.NewInt(opts.Keys))
		key := []byte(fmt.Sprintf("key-%d", keyNum.Int64()))

		// Select topic (round-robin if multiple)
		topic := topics[int(msgCount)%len(topics)]

		// Create record
		record := &kgo.Record{
			Topic: topic,
			Key:   key,
			Value: payload,
		}

		// Produce synchronously for accurate counting
		results := client.ProduceSync(ctx, record)
		if err := results.FirstErr(); err != nil {
			if ctx.Err() != nil {
				goto done
			}
			atomic.AddInt64(&errors, 1)
			metricsCtx.RecordError()
			if errors < 10 || errors%1000 == 0 {
				log.Printf("Producer %d: send error: %v", id, err)
			}
		} else {
			atomic.AddInt64(&messages, 1)
			atomic.AddInt64(&bytes, int64(len(payload)+len(key)))
			metricsCtx.RecordSuccess()
		}

		atomic.AddInt64(&msgCount, 1)

		// Log progress
		if messages > 0 && messages%10000 == 0 {
			elapsed := time.Since(startTime).Seconds()
			log.Printf("Producer %d: sent %d messages (%.2f msg/s)", id, messages, float64(messages)/elapsed)
		}
	}

done:
	// Flush any remaining messages
	if err := client.Flush(context.Background()); err != nil {
		log.Printf("Producer %d: flush error: %v", id, err)
	}

	elapsed := time.Since(startTime).Seconds()
	msgRate := 0.0
	if elapsed > 0 {
		msgRate = float64(messages) / elapsed
	}

	log.Printf("Producer %d: finished - %d messages, %d errors, %.2f msg/s", id, messages, errors, msgRate)

	return producerStats{
		messages: messages,
		errors:   errors,
		bytes:    bytes,
		rate:     msgRate,
	}
}
