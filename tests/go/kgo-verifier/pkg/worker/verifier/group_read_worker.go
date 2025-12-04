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
	"github.com/twmb/franz-go/pkg/kmsg"
	"golang.org/x/time/rate"
)

type GroupReadConfig struct {
	workerCfg        worker.WorkerConfig
	groupName        string
	nPartitions      int32
	nReaders         int
	maxReadCount     int
	rateLimitBytes   int
	maxUncommitted   int
	sessionTimeout   time.Duration
	rebalanceTimeout time.Duration
}

func NewGroupReadConfig(
	wc worker.WorkerConfig, name string, nPartitions int32, nReaders int,
	maxReadCount int, rateLimitBytes int, maxUncommitted int, sessionTimeout time.Duration, rebalanceTimeout time.Duration) GroupReadConfig {
	return GroupReadConfig{
		workerCfg:        wc,
		groupName:        name,
		nPartitions:      nPartitions,
		nReaders:         nReaders,
		maxReadCount:     maxReadCount,
		rateLimitBytes:   rateLimitBytes,
		maxUncommitted:   maxUncommitted,
		sessionTimeout:   sessionTimeout,
		rebalanceTimeout: rebalanceTimeout,
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

func (grw *GroupReadWorker) handlePartitionsAssigned(
	_ context.Context,
	_ *kgo.Client,
	assigned map[string][]int32) {

	grw.Status.Validator.lock.Lock()
	defer grw.Status.Validator.lock.Unlock()

	for topic, partitions := range assigned {
		for _, partition := range partitions {
			if grw.Status.Validator.partitionState[partition] == nil {
				grw.Status.Validator.partitionState[partition] = &partitionTracker{}
				grw.Status.Validator.partitionState[partition].groupAssigned = true
				grw.Status.Validator.partitionState[partition].lastOffsetConsumed = -1
				grw.Status.Validator.partitionState[partition].lastLeaderEpoch = -1
			}
			log.Debugf("Partition %s/%d assigned", topic, partition)
		}
	}
}

func (grw *GroupReadWorker) handlePartitionsRevoked(
	_ context.Context,
	_ *kgo.Client,
	revoked map[string][]int32) {

	grw.Status.Validator.lock.Lock()
	defer grw.Status.Validator.lock.Unlock()

	for topic, partitions := range revoked {
		for _, partition := range partitions {
			if grw.Status.Validator.partitionState[partition] != nil {
				grw.Status.Validator.partitionState[partition].groupAssigned = false
			}
			log.Debugf("Partition %s/%d revoked", topic, partition)
		}
	}
}

func (grw *GroupReadWorker) handleOffsetsFetched(
	_ context.Context,
	_ *kgo.Client,
	offsetFetch *kmsg.OffsetFetchResponse) error {

	grw.Status.Validator.lock.Lock()
	defer grw.Status.Validator.lock.Unlock()

	for _, group := range offsetFetch.Groups {
		for _, topic := range group.Topics {
			for _, partition := range topic.Partitions {
				partitionID := partition.Partition
				fetchedOffset := partition.Offset

				// Check if this partition is assigned and if we're fetching at an offset
				// less than what we've already consumed (duplicate read scenario)
				state := grw.Status.Validator.partitionState[partitionID]
				var isDuplicate bool
				var lastConsumed int64

				if state != nil && state.groupAssigned && state.lastOffsetConsumed != -1 && fetchedOffset <= state.lastOffsetConsumed {
					isDuplicate = true
					lastConsumed = state.lastOffsetConsumed
				}
				if isDuplicate {
					if grw.Status.Validator.exactlyOnceGroupConsumption {
						log.Panicf("Duplicate read detected with exactly-once enabled: Partition %s/%d offset assigned at %d, but already consumed up to %d",
							topic.Topic, partitionID, fetchedOffset, lastConsumed)
					} else {
						log.Warnf("Partition %s/%d offset assigned at %d, but already consumed up to %d - potential duplicate reads detected",
							topic.Topic, partitionID, fetchedOffset, lastConsumed)

						// Reset monotonicity state to allow re-reading from the fetched offset
						// Note: we're already holding the lock, so call the unlocked version
						grw.Status.Validator.resetMonotonicityTestStateUnlocked()
					}
				} else if state != nil && state.groupAssigned {
					log.Debugf("Partition %s/%d offset fetched at %d (last consumed: %d)",
						topic.Topic, partitionID, fetchedOffset, state.lastOffsetConsumed)
				}
			}
		}
	}

	return nil
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

func (grw *GroupReadWorker) pollAndProcessRecords(
	ctx context.Context,
	client *kgo.Client,
	fiberId int,
	cgOffsets *ConsumerGroupOffsets,
	validRanges *TopicOffsetRanges,
	latestValuesProduced *LatestValueMap) error {

	fetches := client.PollRecords(ctx, grw.config.maxUncommitted)

	// Unblock any pending rebalance after processing records from this poll.
	// This is required when using BlockRebalanceOnPoll().
	defer client.AllowRebalance()

	if ctx.Err() == context.Canceled {
		return ctx.Err()
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
		grw.Status.Validator.ValidateRecord(r, validRanges, latestValuesProduced)
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
		kgo.SessionTimeout(grw.config.sessionTimeout),
		kgo.RebalanceTimeout(grw.config.rebalanceTimeout),
		// Hook when the partition assignment happens after a rebalance operation
		kgo.OnPartitionsAssigned(func(ctx context.Context, cl *kgo.Client, assigned map[string][]int32) {
			grw.handlePartitionsAssigned(ctx, cl, assigned)
		}),
		// Hook when the partition assignment is removed after a reabalance operation
		kgo.OnPartitionsRevoked(func(ctx context.Context, cl *kgo.Client, revoked map[string][]int32) {
			grw.handlePartitionsRevoked(ctx, cl, revoked)
		}),
		// Hook when offsets are fetched after an assignment operation happens.
		kgo.OnOffsetsFetched(func(ctx context.Context, cl *kgo.Client, offsetFetch *kmsg.OffsetFetchResponse) error {
			return grw.handleOffsetsFetched(ctx, cl, offsetFetch)
		}),
		// Check franz-go docs for details on this option.
		// It allows us to control when rebalances happen, ensuring that
		// we only rebalance before/after we poll and process the records. This ensures that
		// the handlePartition* hooks do not racily mutate the monotonicity test state while
		// we're still processing records.
		kgo.BlockRebalanceOnPoll(),
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

		err := grw.pollAndProcessRecords(ctx, client, fiberId, cgOffsets, &validRanges, &latestValuesProduced)
		if err == context.Canceled {
			break
		} else if err != nil {
			return err
		}
	}

	// Commit any uncommitted offsets at the end of the read loop, ignoring any errors
	// since we're shutting down anyway
	_ = client.CommitUncommittedOffsets(context.Background())

	return nil
}

func (grw *GroupReadWorker) ResetStats() {
	grw.Status = GroupWorkerStatus{Topic: grw.config.workerCfg.Topic}
}

func (grw *GroupReadWorker) GetStatus() (interface{}, *sync.Mutex) {
	return &grw.Status, &grw.Status.Validator.lock
}
