package verifier

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/redpanda-data/redpanda/tests/go/kgo-verifier/pkg/util"
	worker "github.com/redpanda-data/redpanda/tests/go/kgo-verifier/pkg/worker"
	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/time/rate"
)

type GroupReadConfig struct {
	workerCfg      worker.WorkerConfig
	groupName      string
	nPartitions    int32
	nReaders       int
	maxReadCount   int
	rateLimitBytes int
	maxUncommitted int
}

func NewGroupReadConfig(
	wc worker.WorkerConfig, name string, nPartitions int32, nReaders int,
	maxReadCount int, rateLimitBytes int, maxUncommitted int) GroupReadConfig {
	return GroupReadConfig{
		workerCfg:      wc,
		groupName:      name,
		nPartitions:    nPartitions,
		nReaders:       nReaders,
		maxReadCount:   maxReadCount,
		rateLimitBytes: rateLimitBytes,
		maxUncommitted: maxUncommitted,
	}
}

type GroupWorkerStatus struct {
	Topic     string          `json:"topic"`
	Validator ValidatorStatus `json:"validator"`
	Active    bool            `json:"active"`
	Errors    int             `json:"errors"`
	runCount  int
}

type GroupReadWorker struct {
	config GroupReadConfig
	Status GroupWorkerStatus
}

func NewGroupReadWorker(cfg GroupReadConfig, validatorStatus ValidatorStatus) GroupReadWorker {
	return GroupReadWorker{
		config: cfg,
		Status: GroupWorkerStatus{Topic: cfg.workerCfg.Topic, Validator: validatorStatus},
	}
}

type ConsumerGroupOffsets struct {
	// This is called by one of the readers to signal that we have read all
	// offsets that we intended to.
	cancelFunc context.CancelFunc

	lock sync.Mutex
	// Partition id -> offset last seen by readers
	lastSeen []int64
	// Partition id -> max offset that we intend to read (exclusive)
	upTo []int64
	// number of currently consumed messages
	curReadCount int
	// max number of messages to consume
	maxReadCount int
	// rate limiter
	rlimiter *rate.Limiter
}

func NewConsumerGroupOffsets(
	hwms []int64,
	maxReadCount int,
	rateLimitBytes int,
	cancelFunc context.CancelFunc) ConsumerGroupOffsets {

	var lastSeen, upTo []int64
	if len(hwms) > 0 {
		lastSeen = make([]int64, len(hwms))
		upTo = make([]int64, len(hwms))
		copy(upTo, hwms)
	}

	var rlimiter *rate.Limiter
	if rateLimitBytes > 0 {
		rlimiter = rate.NewLimiter(rate.Limit(rateLimitBytes), rateLimitBytes)
	}
	return ConsumerGroupOffsets{
		cancelFunc:   cancelFunc,
		lastSeen:     lastSeen,
		upTo:         upTo,
		maxReadCount: maxReadCount,
		rlimiter:     rlimiter,
	}
}

func (cgs *ConsumerGroupOffsets) AddRecord(ctx context.Context, r *kgo.Record) {
	cgs.lock.Lock()
	defer cgs.lock.Unlock()

	if cgs.rlimiter != nil {
		cgs.rlimiter.WaitN(ctx, len(r.Value))
	}

	cgs.curReadCount += 1

	if cgs.maxReadCount >= 0 && cgs.curReadCount >= cgs.maxReadCount {
		cgs.cancelFunc()
		return
	}

	if len(cgs.upTo) > 0 {
		if r.Offset > cgs.lastSeen[r.Partition] {
			cgs.lastSeen[r.Partition] = r.Offset
		}

		if cgs.lastSeen[r.Partition] >= cgs.upTo[r.Partition]-1 {
			allComplete := true
			for p, hwm := range cgs.upTo {
				if cgs.lastSeen[p] < hwm-1 {
					allComplete = false
					break
				}
			}
			if allComplete {
				cgs.cancelFunc()
			}
		}
	}
}

func (grw *GroupReadWorker) Wait(ctx context.Context) error {
	grw.Status.Active = true
	defer func() { grw.Status.Active = false }()

	var hwms []int64
	if !grw.config.workerCfg.Continuous {
		client, err := kgo.NewClient(grw.config.workerCfg.MakeKgoOpts()...)
		if err != nil {
			log.Errorf("Error constructing client: %v", err)
			return err
		}

		startOffsets := GetOffsets(client, grw.config.workerCfg.Topic, grw.config.nPartitions, -2)
		hwms = GetOffsets(client, grw.config.workerCfg.Topic, grw.config.nPartitions, -1)
		client.Close()

		hasMessages := false
		for p := 0; p < int(grw.config.nPartitions); p++ {
			if startOffsets[p] < hwms[p] {
				hasMessages = true
				break
			}
		}

		if !hasMessages {
			log.Infof("Topic is empty, exiting...")
			return nil
		}
	}

	groupName := grw.config.groupName
	if grw.config.groupName == "" {
		groupName = fmt.Sprintf(
			"kgo-verifier-%d-%d-%d", time.Now().Unix(), os.Getpid(), grw.Status.runCount)
	}

	grw.Status.runCount += 1

	log.Infof("Reading with consumer group %s", groupName)

	ctx, cancelFunc := context.WithCancel(ctx)
	cgOffsets := NewConsumerGroupOffsets(
		hwms, grw.config.maxReadCount, grw.config.rateLimitBytes, cancelFunc)

	var wg sync.WaitGroup
	for i := 0; i < int(grw.config.nReaders); i++ {
		wg.Add(1)
		go func(fiberId int) {
			for {
				err := grw.consumerGroupReadInner(
					ctx, fiberId, groupName, &cgOffsets)
				if err != nil {
					log.Warnf(
						"fiber %v: restarting consumer group reader for error %v",
						fiberId, err)

					// Since we are consuming with a consumer group it is legal
					// to read older offsets on next try.
					//
					// Note for maintainers: Consider implementing checkpointing
					// on consumer group commits and reverting monotonicity test
					// state up to last commit. This will further validate
					// linearizability of offset commits.
					grw.Status.Validator.ResetMonotonicityTestState()

					// Loop around and retry
				} else {
					log.Infof("fiber %v: consumer group reader finished", fiberId)
					break
				}
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
	grw.Status.Validator.Checkpoint()
	grw.Status.Validator.ResetMonotonicityTestState()
	return nil
}

func (grw *GroupReadWorker) consumerGroupReadInner(
	ctx context.Context,
	fiberId int, groupName string,
	cgOffsets *ConsumerGroupOffsets) error {

	opts := grw.config.workerCfg.MakeKgoOpts()
	opts = append(opts, []kgo.Opt{
		kgo.ConsumeTopics(grw.config.workerCfg.Topic),
		kgo.ConsumerGroup(groupName),
	}...)
	if grw.config.rateLimitBytes > 0 {
		// reduce batch size for smoother rate limiting
		opts = append(opts,
			kgo.FetchMaxBytes(int32(grw.config.rateLimitBytes/grw.config.nReaders/10)))
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		// Our caller can retry us.
		log.Warnf("Error creating kafka client: %v", err)
		return err
	}
	defer client.Close()

	validRanges := LoadTopicOffsetRanges(grw.config.workerCfg.Topic, grw.config.nPartitions)
	var latestValuesProduced LatestValueMap
	if grw.Status.Validator.expectFullyCompacted {
		latestValuesProduced = LoadLatestValues(grw.config.workerCfg.Topic, grw.config.nPartitions)
	}

	for {
		if grw.config.maxUncommitted == 0 {
			// for users to be aware that immediate commits are not supported
			util.Die("max-uncommitted must be non-zero")
		}
		fetches := client.PollRecords(ctx, grw.config.maxUncommitted)
		if ctx.Err() == context.Canceled {
			break
		} else if ctx.Err() != nil {
			return ctx.Err()
		}

		var r_err error
		fetches.EachError(func(t string, p int32, err error) {
			log.Warnf(
				"fiber %v: Consumer group fetch %s/%d e=%v...",
				fiberId, t, p, err)
			var lossErr *kgo.ErrDataLoss
			if errors.As(err, &lossErr) {
				if grw.config.workerCfg.TolerateDataLoss {
					grw.Status.Validator.RecordLostOffsets(lossErr.Partition, lossErr.ConsumedTo-lossErr.ResetTo)
					grw.Status.Validator.SetMonotonicityTestStateForPartition(p, lossErr.ResetTo-1)
				} else {
					log.Fatalf("Unexpected data loss detected: %v", lossErr)
				}
			} else {
				r_err = err
			}
		})

		if r_err != nil {
			return r_err
		}

		fetches.EachRecord(func(r *kgo.Record) {
			log.Debugf(
				"fiber %v: Consumer group read %s/%d o=%d...",
				fiberId, grw.config.workerCfg.Topic, r.Partition, r.Offset)
			grw.Status.Validator.ValidateRecord(r, &validRanges, &latestValuesProduced)
			// Will cancel the context if we have read everything
			cgOffsets.AddRecord(ctx, r)
		})

		// Otherwise offsets will be enqueued for commit on the next PollFetches invocation
		// and the actual commit will happen in background respecting `kgo.AutoCommitInterval` (default: 5s).
		if grw.config.maxUncommitted > 0 {
			if err := client.CommitUncommittedOffsets(ctx); err != nil {
				return err
			}
		}

	}

	return nil
}

func (grw *GroupReadWorker) ResetStats() {
	grw.Status = GroupWorkerStatus{Topic: grw.config.workerCfg.Topic}
}

func (grw *GroupReadWorker) GetStatus() (interface{}, *sync.Mutex) {
	return &grw.Status, &grw.Status.Validator.lock
}
