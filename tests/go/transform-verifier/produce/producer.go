package produce

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/inhies/go-bytesize"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"redpanda.com/testing/transform-verifier/common"
)

var (
	bps           = 512 * bytesize.KB
	totalBytes    = 10 * bytesize.MB
	messageSize   = 1 * bytesize.KB
	maxBatchSize  = 1 * bytesize.MB
	transactional = false
)

type ProduceStatus struct {
	BytesSent    int            `json:"bytes_sent"`
	LatestSeqnos map[int]uint64 `json:"latest_seqnos"`
	ErrorCount   int            `json:"error_count"`
	Done         bool           `json:"done"`
}

func (self ProduceStatus) Merge(other ProduceStatus) ProduceStatus {
	combined := make(map[int]uint64)
	for k, v := range self.LatestSeqnos {
		combined[k] = max(v, combined[k])
	}
	for k, v := range other.LatestSeqnos {
		combined[k] = max(v, combined[k])
	}
	return ProduceStatus{
		BytesSent:    self.BytesSent + other.BytesSent,
		LatestSeqnos: combined,
		ErrorCount:   self.ErrorCount + other.ErrorCount,
		Done:         self.Done || other.Done,
	}
}

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "produce",
		Short: "Produce messages to an input topic",
		Run: func(cmd *cobra.Command, args []string) {
			slog.Info("starting produce")
			if err := produce(cmd.Context()); err != nil {
				common.Die("unable to produce: %v", err)
			}
			slog.Info("produce completed")
		},
	}

	cmd.Flags().Var(&bps, "bytes-per-second", "How much traffic to send per second")
	cmd.Flags().Var(&totalBytes, "max-bytes", "How many bytes to send overall")
	cmd.Flags().Var(&messageSize, "message-size", "How many bytes to send per message")
	cmd.Flags().Var(&maxBatchSize, "max-batch-size", "How many bytes to send per batch")
	cmd.Flags().BoolVar(&transactional, "transactional", false, "Use transactions to produce, and occationally abort transactions")

	return cmd
}

func createKeyGenerator() func(maxSize int) ([]byte, error) {
	return func(maxSize int) ([]byte, error) {
		id := uuid.NewString()
		if len(id) > maxSize {
			return nil, fmt.Errorf("not enough bytes: size=%d max=%d", len(id), maxSize)
		}
		return []byte(id), nil
	}
}

func createValueGenerator() func(size int) ([]byte, error) {
	// Generate random alphabetic payloads
	//
	// Don't generate random bytes because some tests ensure these are valid UTF-8
	var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	rng := rand.New(rand.NewSource(9))
	return func(size int) ([]byte, error) {
		payload := make([]byte, size)
		for i := range payload {
			payload[i] = letters[rng.Intn(len(letters))]
		}
		return payload, nil
	}
}

type recordGenerator = func() (*kgo.Record, error)

func createRecordGenerator(partition int32) recordGenerator {
	seqno := uint64(0)
	keyGen := createKeyGenerator()
	valGen := createValueGenerator()
	return func() (*kgo.Record, error) {
		// seqno header
		seqno++
		header := common.MakeSeqnoHeader(seqno)
		// generate key
		sizeLeft := int(messageSize) - len(header.Key) - len(header.Value)
		k, err := keyGen(sizeLeft)
		if err != nil {
			return nil, fmt.Errorf("unable to make key: %v", err)
		}
		// generate value
		sizeLeft -= len(k)
		v, err := valGen(sizeLeft)
		if err != nil {
			return nil, fmt.Errorf("unable to make key: %v", err)
		}
		return &kgo.Record{
			Key:       k,
			Value:     v,
			Timestamp: time.Now(),
			Partition: partition,
			Headers: []kgo.RecordHeader{
				header,
			},
		}, nil
	}
}

func createReporter(ctx context.Context) func(ProduceStatus) {
	l := &sync.Mutex{}
	latest := ProduceStatus{}
	return func(update ProduceStatus) {
		l.Lock()
		defer l.Unlock()
		latest = latest.Merge(update)
		common.ReportStatus(ctx, latest)
	}
}

// produce goes as fast as possible according to the settings and rate limit to produce records to the broker.
//
// if ctx is cancelled, then this function always returns nil.
func produce(ctx context.Context) error {
	topic, err := common.TopicMetadata(ctx)
	if err != nil {
		return fmt.Errorf("unable to fetch metadata: %v", err)
	}
	rateLimiter := rate.NewLimiter(rate.Limit(bps), int(bps))
	reporter := createReporter(ctx)
	reporter(ProduceStatus{})
	// Run each partition in parallel and if one returns an error we'll cancel them all.
	wg, ctx := errgroup.WithContext(ctx)
	// Have each partition send about the same amount of data
	maxBytes := int(totalBytes) / len(topic.Partitions)
	for _, p := range topic.Partitions {
		partition := p // prevent silly golang loop variable bounding issues
		wg.Go(func() error {
			slog.Info("starting to produce", "partition", partition)
			config := partitionProduceConfig{
				maxBytes,
				partition,
				reporter,
				rateLimiter,
			}
			var err error
			if transactional {
				err = produceTransactionallyForPartition(ctx, config)
			} else {
				err = produceForPartition(ctx, config)
			}
			slog.Info("finished producing", "partition", partition, "err", err)
			return err
		})
	}
	if err := wg.Wait(); err != nil {
		return err
	}
	reporter(ProduceStatus{Done: true})
	return nil
}

type partitionProduceConfig struct {
	maxBytes    int
	partition   int32
	reporter    func(ProduceStatus)
	rateLimiter *rate.Limiter
}

func produceForPartition(ctx context.Context, config partitionProduceConfig) error {
	client, err := common.NewClient(kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		return fmt.Errorf("unable to create client: %v", err)
	}
	defer client.Close()
	generator := createRecordGenerator(config.partition)
	bytesSent := 0
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	// Produce until we've reached our limit or are cancelled
	for bytesSent < config.maxBytes && ctx.Err() == nil {
		r, err := generator()
		if err != nil {
			return fmt.Errorf("unable to create record: %v", err)
		}
		size := common.RecordSize(r)
		if err := config.rateLimiter.WaitN(ctx, size); err != nil {
			// Only return the error if we were not cancelled.
			if ctx.Err() == nil {
				return fmt.Errorf("unable to rate limit: %v", err)
			}
			return nil
		}
		wg.Add(1)
		client.Produce(ctx, r, func(r *kgo.Record, err error) {
			defer wg.Done()
			if ctx.Err() != nil {
				// Do nothing we were cancelled, just stop ASAP
			} else if err != nil {
				slog.Warn("error producing record", "err", err)
				config.reporter(ProduceStatus{ErrorCount: 1})
			} else {
				seqno, err := common.FindSeqnoHeader(r)
				if err != nil {
					slog.Warn("invalid produced record", "err", err)
					config.reporter(ProduceStatus{ErrorCount: 1})
				} else {
					seqnos := make(map[int]uint64)
					seqnos[int(r.Partition)] = seqno
					config.reporter(ProduceStatus{LatestSeqnos: seqnos})
				}
			}
		})
		bytesSent += size
		config.reporter(ProduceStatus{BytesSent: size})
	}
	wg.Wait()
	return nil
}

func createInvalidRecordGenerator(partition int32) recordGenerator {
	// Create an empty record with the highest seqno that would cause the checks in the consumer to fail.
	return func() (*kgo.Record, error) {
		return &kgo.Record{
			Key:       nil,
			Value:     nil,
			Timestamp: time.Now(),
			Partition: partition,
			Headers: []kgo.RecordHeader{
				common.MakeSeqnoHeader(math.MaxUint64),
			},
		}, nil
	}
}

func produceTransactionallyForPartition(ctx context.Context, config partitionProduceConfig) error {
	client, err := common.NewClient(
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.TransactionalID(fmt.Sprintf("%d-%s-txn-producer-%d", os.Getpid(), os.Args[0], config.partition)),
	)
	if err != nil {
		return fmt.Errorf("unable to create client: %v", err)
	}
	defer client.Close()
	generator := createRecordGenerator(config.partition)
	invalidGenerator := createInvalidRecordGenerator(config.partition)
	bytesSent := 0
	produce := func(r *kgo.Record, commit kgo.TransactionEndTry) ProduceStatus {
		if err := client.BeginTransaction(); err != nil {
			slog.Warn("error starting txn", "err", err)
			return ProduceStatus{ErrorCount: 1}
		}
		err := client.ProduceSync(ctx, r).FirstErr()
		if ctx.Err() != nil {
			// we're cancelled do nothing
			return ProduceStatus{}
		} else if err != nil {
			slog.Warn("error producing record", "err", err)
			return ProduceStatus{ErrorCount: 1}
		}
		if err := client.Flush(ctx); err != nil {
			slog.Warn("error flushing record", "err", err)
			return ProduceStatus{ErrorCount: 1}
		}
		if err := client.EndTransaction(ctx, commit); err != nil {
			slog.Warn("error ending txn", "err", err)
			return ProduceStatus{ErrorCount: 1}
		}
		seqnos := make(map[int]uint64)
		h, err := common.FindSeqnoHeader(r)
		if err != nil {
			slog.Warn("error finding seqno header", "err", err)
			return ProduceStatus{ErrorCount: 1}
		}
		seqnos[int(r.Partition)] = h
		return ProduceStatus{LatestSeqnos: seqnos}
	}

	for bytesSent < config.maxBytes && ctx.Err() == nil {
		r, err := generator()
		if err != nil {
			return fmt.Errorf("unable to create record: %v", err)
		}
		size := common.RecordSize(r)
		if err := config.rateLimiter.WaitN(ctx, size); err != nil {
			// Only return the error if we were not cancelled
			if ctx.Err() == nil {
				return fmt.Errorf("unable to rate limit: %v", err)
			}
			return nil
		}
		status := produce(r, kgo.TryCommit)
		bytesSent += size
		status.BytesSent += size
		config.reporter(status)

		// Every time we commit something successfully, also abort a record that would break the consumer to ensure those records aren't surfaced.
		r, err = invalidGenerator()
		if err != nil {
			return fmt.Errorf("unable to create record: %v", err)
		}
		produce(r, kgo.TryAbort)
	}
	return nil
}
