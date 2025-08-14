package verifier

import (
	"context"
	"errors"
	"sync"

	worker "github.com/redpanda-data/redpanda/tests/go/kgo-verifier/pkg/worker"
	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/time/rate"
)

type SeqReadConfig struct {
	workerCfg      worker.WorkerConfig
	name           string
	nPartitions    int32
	maxReadCount   int
	rateLimitBytes int
}

func NewSeqReadConfig(
	wc worker.WorkerConfig, name string, nPartitions int32,
	maxReadCount int, rateLimitBytes int) SeqReadConfig {
	return SeqReadConfig{
		workerCfg:      wc,
		name:           name,
		nPartitions:    nPartitions,
		maxReadCount:   maxReadCount,
		rateLimitBytes: rateLimitBytes,
	}
}

type SeqWorkerStatus struct {
	Topic     string          `json:"topic"`
	Validator ValidatorStatus `json:"validator"`
	Active    bool            `json:"active"`
	Errors    int             `json:"errors"`
}

type SeqReadWorker struct {
	config SeqReadConfig
	Status SeqWorkerStatus
}

func NewSeqReadWorker(cfg SeqReadConfig, validatorStatus ValidatorStatus) SeqReadWorker {
	return SeqReadWorker{
		config: cfg,
		Status: SeqWorkerStatus{Topic: cfg.workerCfg.Topic, Validator: validatorStatus},
	}
}

func (srw *SeqReadWorker) Wait(ctx context.Context) error {
	srw.Status.Active = true
	defer func() { srw.Status.Active = false }()

	lwm := make([]int64, srw.config.nPartitions)
	var hwm []int64

	if !srw.config.workerCfg.Continuous {
		client, err := kgo.NewClient(srw.config.workerCfg.MakeKgoOpts()...)
		if err != nil {
			log.Errorf("Error constructing client: %v", err)
			return err
		}
		hwm = GetOffsets(client, srw.config.workerCfg.Topic, srw.config.nPartitions, -1)
		client.Close()
	}

	for {
		var err error
		lwm, err = srw.sequentialReadInner(ctx, lwm, hwm)
		if err == nil || ctx.Err() == context.Canceled {
			break
		} else if err != nil {
			log.Warnf("Restarting reader for error %v", err)
			// Loop around
		}
	}

	srw.Status.Validator.ResetMonotonicityTestState()

	return nil
}

func (srw *SeqReadWorker) sequentialReadInner(ctx context.Context, startAt []int64, upTo []int64) ([]int64, error) {
	log.Infof("Sequential read start offsets: %v", startAt)
	log.Infof("Sequential read end offsets: %v", upTo)

	offsets := make(map[string]map[int32]kgo.Offset)
	partOffsets := make(map[int32]kgo.Offset, srw.config.nPartitions)
	complete := make([]bool, srw.config.nPartitions)
	for i, o := range startAt {
		partOffsets[int32(i)] = kgo.NewOffset().At(o)
		log.Infof("Sequential start offset %s/%d  %#v...", srw.config.workerCfg.Topic, i, partOffsets[int32(i)])
		if len(upTo) > 0 {
			if o == upTo[i] {
				complete[i] = true
			}
		}
	}
	offsets[srw.config.workerCfg.Topic] = partOffsets

	validRanges := LoadTopicOffsetRanges(srw.config.workerCfg.Topic, srw.config.nPartitions)
	var latestValuesProduced LatestValueMap
	if srw.Status.Validator.expectFullyCompacted {
		latestValuesProduced = LoadLatestValues(srw.config.workerCfg.Topic, srw.config.nPartitions)
	}

	if validRanges.AdjustConsumableOffsets {
		for i, o := range upTo {
			if validRanges.LastConsumableOffsets[i] < o {
				upTo[i] = validRanges.LastConsumableOffsets[i]
			}
			log.Debugf("Adjusted offset of upTo from %d to %d", o, upTo[i])
		}
	}

	opts := srw.config.workerCfg.MakeKgoOpts()
	opts = append(opts, []kgo.Opt{
		kgo.ConsumePartitions(offsets),
		// By default control records are dropped by kgo.
		// This can cause SeqReadWorker to indefinitely hang on
		// PollFetches if control records are at the end of a
		// partition's log. Since SeqReadWorker will never
		// see a message at a log's HWM if kgo drops the
		// control records.
		kgo.KeepControlRecords(),
	}...)
	if srw.config.rateLimitBytes > 0 {
		// reduce batch size for smoother rate limiting
		opts = append(opts, kgo.FetchMaxBytes(int32(srw.config.rateLimitBytes/10)))
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		log.Errorf("Error creating Kafka client: %v", err)
		return nil, err
	}

	curReadCount := 0
	var rlimiter *rate.Limiter
	if srw.config.rateLimitBytes > 0 {
		rlimiter = rate.NewLimiter(rate.Limit(srw.config.rateLimitBytes), srw.config.rateLimitBytes)
	}

	lwm := append([]int64{}, startAt...)

	for {
		log.Debugf("Calling PollFetches (lwm=%v status %s)", lwm, srw.Status.Validator.String())
		fetches := client.PollFetches(ctx)
		log.Debugf("PollFetches returned %d fetches", len(fetches))

		var r_err error
		fetches.EachError(func(t string, p int32, err error) {
			log.Warnf("Sequential fetch %s/%d e=%v...", t, p, err)
			var lossErr *kgo.ErrDataLoss
			if errors.As(err, &lossErr) {
				if srw.config.workerCfg.TolerateDataLoss {
					srw.Status.Validator.RecordLostOffsets(lossErr.Partition, lossErr.ConsumedTo-lossErr.ResetTo)
					srw.Status.Validator.SetMonotonicityTestStateForPartition(p, lossErr.ResetTo-1)
				} else {
					log.Fatalf("Unexpected data loss detected: %v", lossErr)
				}
			} else {
				r_err = err
			}
		})

		if r_err != nil {
			// This is not fatal: server is allowed to return an error, the loop outside
			// this function will try again, picking up from last_read.
			log.Warnf("Returning on fetch error %v, lwm %v", r_err, lwm)
			return lwm, r_err
		}

		fetches.EachRecord(func(r *kgo.Record) {
			if rlimiter != nil {
				rlimiter.WaitN(ctx, len(r.Value))
			}

			log.Debugf("Sequential read %s/%d o=%d...", srw.config.workerCfg.Topic, r.Partition, r.Offset)
			curReadCount += 1

			// Increment low watermark to skip the message on next try.
			if r.Offset >= lwm[r.Partition] {
				lwm[r.Partition] = r.Offset + 1
			}

			if len(upTo) > 0 {
				if r.Offset >= upTo[r.Partition]-1 {
					complete[r.Partition] = true
				}
			}

			// We subscribe to control records to make consuming-to-offset work, but we
			// do not want to try and validate them.
			if r.Attrs.IsControl() {
				return
			}
			srw.Status.Validator.ValidateRecord(r, &validRanges, &latestValuesProduced)
		})

		if srw.config.maxReadCount >= 0 && curReadCount >= srw.config.maxReadCount {
			break
		}

		any_incomplete := false
		for _, c := range complete {
			if !c {
				any_incomplete = true
			}

		}

		if !any_incomplete {
			break
		}
	}

	log.Infof("Sequential read complete up to %v (validator status %v)", lwm, srw.Status.Validator.String())
	client.Close()

	return lwm, nil
}

func (srw *SeqReadWorker) ResetStats() {
	srw.Status = SeqWorkerStatus{Topic: srw.config.workerCfg.Topic}
}

func (srw *SeqReadWorker) GetStatus() (interface{}, *sync.Mutex) {
	return &srw.Status, &srw.Status.Validator.lock
}
