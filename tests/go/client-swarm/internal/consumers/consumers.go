package consumers

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redpanda-data/redpanda/tests/go/client-swarm/internal/metrics"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Options for consumer configuration
type Options struct {
	Brokers          []string
	Topic            string
	UniqueTopics     bool
	UniqueGroups     bool
	Group            string
	StaticPrefix     string
	Count            int
	Messages         int64
	Properties       []string
	ClientSpawnWait  time.Duration
	TopicsPerClient  int
	SessionTimeout   time.Duration
	RebalanceTimeout time.Duration
}

// Stats contains consumer statistics
type Stats struct {
	TotalMessages int64
	TotalBytes    int64
	Duration      time.Duration
}

// ConsumeCounter tracks global message consumption
type ConsumeCounter struct {
	target  int64
	current int64
	done    chan struct{}
	once    sync.Once
}

// NewConsumeCounter creates a new consume counter
func NewConsumeCounter(target int64) *ConsumeCounter {
	return &ConsumeCounter{
		target: target,
		done:   make(chan struct{}),
	}
}

// Increment adds to the counter and returns true if target reached
func (c *ConsumeCounter) Increment(n int64) bool {
	if c.target <= 0 {
		atomic.AddInt64(&c.current, n)
		return false
	}

	newVal := atomic.AddInt64(&c.current, n)
	if newVal >= c.target {
		c.once.Do(func() {
			close(c.done)
		})
		return true
	}
	return false
}

// Done returns a channel that's closed when target is reached
func (c *ConsumeCounter) Done() <-chan struct{} {
	return c.done
}

// Current returns the current count
func (c *ConsumeCounter) Current() int64 {
	return atomic.LoadInt64(&c.current)
}

// consumerStats holds stats for a single consumer
type consumerStats struct {
	messages int64
	bytes    int64
}

// Run runs the consumer swarm
func Run(ctx context.Context, opts Options, metricsCtx *metrics.Context) (*Stats, error) {
	startTime := time.Now()
	var wg sync.WaitGroup
	statsCh := make(chan consumerStats, opts.Count)

	// Create shared counter for message target
	counter := NewConsumeCounter(opts.Messages)

	// Create a cancellable context for when target is reached
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Watch for target completion
	go func() {
		select {
		case <-counter.Done():
			log.Printf("Target message count reached, stopping consumers")
			cancel()
		case <-ctx.Done():
		}
	}()

	log.Printf("Starting %d consumers", opts.Count)

	for i := 0; i < opts.Count; i++ {
		wg.Add(1)
		consumerID := i

		// Determine topic(s) for this consumer
		topics := getTopics(opts.Topic, opts.UniqueTopics, consumerID, opts.TopicsPerClient)

		// Determine group for this consumer
		group := opts.Group
		if opts.UniqueGroups {
			group = fmt.Sprintf("%s-%d", opts.Group, consumerID)
		}

		go func(id int, topics []string, group string) {
			defer wg.Done()
			stats := runConsumer(runCtx, id, topics, group, opts, counter, metricsCtx)
			statsCh <- stats
		}(consumerID, topics, group)

		// Staggered spawning
		if opts.ClientSpawnWait > 0 && i < opts.Count-1 {
			select {
			case <-ctx.Done():
				break
			case <-time.After(opts.ClientSpawnWait):
			}
		}
	}

	// Wait for all consumers to finish
	go func() {
		wg.Wait()
		close(statsCh)
	}()

	// Collect stats
	var totalMessages, totalBytes int64
	for stats := range statsCh {
		totalMessages += stats.messages
		totalBytes += stats.bytes
	}

	duration := time.Since(startTime)

	return &Stats{
		TotalMessages: totalMessages,
		TotalBytes:    totalBytes,
		Duration:      duration,
	}, nil
}

func getTopics(baseTopic string, unique bool, consumerID, topicsPerClient int) []string {
	if !unique {
		return []string{baseTopic}
	}

	topics := make([]string, topicsPerClient)
	for i := 0; i < topicsPerClient; i++ {
		topics[i] = fmt.Sprintf("%s-%d-%d", baseTopic, consumerID, i)
	}
	return topics
}

func runConsumer(
	ctx context.Context,
	id int,
	topics []string,
	group string,
	opts Options,
	counter *ConsumeCounter,
	metricsCtx *metrics.Context,
) consumerStats {
	// Build client options
	clientOpts := []kgo.Opt{
		kgo.SeedBrokers(opts.Brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.SessionTimeout(opts.SessionTimeout),
		kgo.RebalanceTimeout(opts.RebalanceTimeout),
		kgo.DisableAutoCommit(),
	}

	// Static group membership
	if opts.StaticPrefix != "" {
		instanceID := fmt.Sprintf("%s-%d", opts.StaticPrefix, id)
		clientOpts = append(clientOpts, kgo.InstanceID(instanceID))
	}

	client, err := kgo.NewClient(clientOpts...)
	if err != nil {
		log.Printf("Consumer %d: failed to create client: %v", id, err)
		return consumerStats{}
	}
	defer client.Close()

	metricsCtx.RecordClientStart()
	defer metricsCtx.RecordClientStop()

	var messages, bytes int64
	startTime := time.Now()
	lastCommit := time.Now()
	commitInterval := 5 * time.Second

	for {
		select {
		case <-ctx.Done():
			goto done
		default:
		}

		// Poll for records
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			goto done
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, err := range errs {
				if ctx.Err() != nil {
					goto done
				}
				log.Printf("Consumer %d: fetch error on %s/%d: %v", id, err.Topic, err.Partition, err.Err)
			}
			continue
		}

		// Process records
		recordCount := int64(0)
		var recordBytes int64

		fetches.EachRecord(func(r *kgo.Record) {
			recordCount++
			recordBytes += int64(len(r.Key) + len(r.Value))
			metricsCtx.RecordSuccess()
		})

		if recordCount > 0 {
			messages += recordCount
			bytes += recordBytes

			// Check if we've reached the target
			if counter.Increment(recordCount) {
				goto done
			}

			// Periodic commit
			if time.Since(lastCommit) >= commitInterval {
				if err := client.CommitUncommittedOffsets(ctx); err != nil {
					if ctx.Err() == nil {
						log.Printf("Consumer %d: commit error: %v", id, err)
					}
				}
				lastCommit = time.Now()
			}

			// Log progress
			if messages%10000 == 0 {
				elapsed := time.Since(startTime).Seconds()
				log.Printf("Consumer %d: consumed %d messages (%.2f msg/s)", id, messages, float64(messages)/elapsed)
			}
		}
	}

done:
	// Final commit
	commitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.CommitUncommittedOffsets(commitCtx); err != nil {
		log.Printf("Consumer %d: final commit error: %v", id, err)
	}

	elapsed := time.Since(startTime).Seconds()
	msgRate := 0.0
	if elapsed > 0 {
		msgRate = float64(messages) / elapsed
	}

	log.Printf("Consumer %d: finished - %d messages, %.2f msg/s", id, messages, msgRate)

	return consumerStats{
		messages: messages,
		bytes:    bytes,
	}
}
