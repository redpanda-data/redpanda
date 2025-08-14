package verifier

import (
	"context"
	"math/rand"
	"runtime"
	"sync"
	"time"

	"github.com/redpanda-data/redpanda/tests/go/kgo-verifier/pkg/util"
	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kgo"

	worker "github.com/redpanda-data/redpanda/tests/go/kgo-verifier/pkg/worker"
)

type RandomReadConfig struct {
	workerCfg   worker.WorkerConfig
	name        string
	readCount   int
	nPartitions int32
}

type RandomReadWorker struct {
	config RandomReadConfig
	Status RandomWorkerStatus
}

type RandomWorkerStatus struct {
	Topic     string          `json:"topic"`
	Validator ValidatorStatus `json:"validator"`
	Active    bool            `json:"active"`
	Errors    int             `json:"errors"`
}

func NewRandomReadConfig(wc worker.WorkerConfig, name string, nPartitions int32, readCount int) RandomReadConfig {
	return RandomReadConfig{
		workerCfg:   wc,
		name:        name,
		nPartitions: nPartitions,
		readCount:   readCount,
	}
}

func NewRandomReadWorker(cfg RandomReadConfig, validatorStatus ValidatorStatus) RandomReadWorker {
	return RandomReadWorker{
		config: cfg,
		Status: RandomWorkerStatus{Topic: cfg.workerCfg.Topic, Validator: validatorStatus},
	}
}

func (w *RandomReadWorker) newClient(opts []kgo.Opt) (*kgo.Client, error) {
	opts = append(opts, w.config.workerCfg.MakeKgoOpts()...)

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func (w *RandomReadWorker) loadOffsets() ([]int64, []int64, error) {
	// Basic client to read offsets
	client, err := w.newClient(make([]kgo.Opt, 0))
	if err != nil {
		log.Errorf("Error constructing client: %v", err)
		return nil, nil, err
	}
	endOffsets := GetOffsets(client, w.config.workerCfg.Topic, w.config.nPartitions, -1)
	client.Close()
	client, err = w.newClient(make([]kgo.Opt, 0))
	if err != nil {
		log.Errorf("Error constructing client: %v", err)
		return nil, nil, err
	}
	startOffsets := GetOffsets(client, w.config.workerCfg.Topic, w.config.nPartitions, -2)
	client.Close()
	runtime.GC()

	return startOffsets, endOffsets, err
}

func (w *RandomReadWorker) Wait() error {
	w.Status.Active = true
	defer func() { w.Status.Active = false }()

	startOffsets, endOffsets, err := w.loadOffsets()
	if err != nil {
		log.Errorf("Error loading offsets: %v", err)
		return err
	}

	validRanges := LoadTopicOffsetRanges(w.config.workerCfg.Topic, w.config.nPartitions)
	var latestValuesProduced LatestValueMap
	if w.Status.Validator.expectFullyCompacted {
		latestValuesProduced = LoadLatestValues(w.config.workerCfg.Topic, w.config.nPartitions)
	}

	ctxLog := log.WithFields(log.Fields{"tag": w.config.name})

	readCount := w.config.readCount

	// Select a partition and location
	ctxLog.Infof("Reading %d random offsets", w.config.readCount)

	i := 0
	for i < readCount {
		w.Status.Validator.ResetMonotonicityTestState()

		p := rand.Int31n(w.config.nPartitions)
		pStart := startOffsets[p]
		pEnd := endOffsets[p]

		if pEnd-pStart < 2 {
			ctxLog.Warnf("Partition %d is empty, skipping read", p)
			continue
		}
		o := rand.Int63n(pEnd-pStart-1) + pStart
		offset := kgo.NewOffset().At(o)

		// Construct a map of topic->partition->offset to seek our new client to the right place
		offsets := make(map[string]map[int32]kgo.Offset)
		partOffsets := make(map[int32]kgo.Offset, 1)
		partOffsets[p] = offset
		offsets[w.config.workerCfg.Topic] = partOffsets

		// Fully-baked client for actual consume
		opts := []kgo.Opt{
			kgo.ConsumePartitions(offsets),
		}

		client, err := w.newClient(opts)
		if err != nil {
			log.Errorf("Error constructing client: %v", err)
			return err
		}

		// Read one record
		ctxLog.Debugf("Reading partition %d (%d-%d) at offset %s", p, pStart, pEnd, offset)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		fetches := client.PollRecords(ctx, 1)
		ctxLog.Debugf("Read done for partition %d (%d-%d) at offset %s", p, pStart, pEnd, offset)
		fetches.EachError(func(topic string, partition int32, e error) {
			// In random read mode, we tolerate read errors: if the server is unavailable
			// we will just proceed to read the next random offset.
			ctxLog.Errorf("Error reading from partition %s:%d: %v", topic, partition, e)
		})
		fetches.EachRecord(func(r *kgo.Record) {
			if r.Partition != p {
				util.Die("Wrong partition %d in read at offset %d on partition %s/%d", r.Partition, r.Offset, w.config.workerCfg.Topic, p)
			}
			w.Status.Validator.ValidateRecord(r, &validRanges, &latestValuesProduced)
		})
		if len(fetches.Records()) == 0 {
			ctxLog.Errorf("Reloading offsets on empty response reading from partition %d at %s", p, offset)

			// If we get an empty response, it may be because the partition was prefix-truncated
			// and we tried to read an out of range offset: handle this by re-loading our
			// offset bounds
			startOffsets, endOffsets, err = w.loadOffsets()
			if err != nil {
				log.Errorf("Error reloading offsets: %v", err)
				return err
			}
		} else {
			// Each read on which we get some records counts toward
			// the number of reads we were requested to do.
			i += 1
		}
		fetches = nil

		client.Flush(context.Background())
		client.Close()
	}

	return nil
}

func (rrw *RandomReadWorker) ResetStats() {
	rrw.Status = RandomWorkerStatus{Topic: rrw.config.workerCfg.Topic}
}

func (rrw *RandomReadWorker) GetStatus() (interface{}, *sync.Mutex) {
	return &rrw.Status, &rrw.Status.Validator.lock
}
