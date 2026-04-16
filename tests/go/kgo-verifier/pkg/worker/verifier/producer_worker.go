package verifier

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rcrowley/go-metrics"
	"github.com/redpanda-data/redpanda/tests/go/kgo-verifier/pkg/util"
	worker "github.com/redpanda-data/redpanda/tests/go/kgo-verifier/pkg/worker"
	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"
)

type ProducerConfig struct {
	workerCfg             worker.WorkerConfig
	name                  string
	nPartitions           int32
	messageSize           int
	messageCount          int
	fakeTimestampMs       int64
	fakeTimestampStepMs   int64
	rateLimitBytes        int
	keySetCardinality     int
	messagesPerProducerId int
	valueGenerator        worker.ValueGenerator
	producesTombstones    bool
}

func NewProducerConfig(wc worker.WorkerConfig, name string, nPartitions int32,
	messageSize int, messageCount int, fakeTimestampMs int64, fakeTimestampStepMs int64, rateLimitBytes int, keySetCardinality int, messagesPerProducerId int, tombstoneProbability float64, produceRandomBytes bool) ProducerConfig {
	return ProducerConfig{
		workerCfg:             wc,
		name:                  name,
		nPartitions:           nPartitions,
		messageCount:          messageCount,
		messageSize:           messageSize,
		fakeTimestampMs:       fakeTimestampMs,
		fakeTimestampStepMs:   fakeTimestampStepMs,
		rateLimitBytes:        rateLimitBytes,
		keySetCardinality:     keySetCardinality,
		messagesPerProducerId: messagesPerProducerId,
		producesTombstones:    tombstoneProbability != 0,
		valueGenerator: worker.ValueGenerator{
			PayloadSize:          uint64(messageSize),
			Compressible:         wc.CompressiblePayload,
			TombstoneProbability: tombstoneProbability,
			ProduceRandomBytes:   produceRandomBytes,
		},
	}
}

type ProducerWorker struct {
	config              ProducerConfig
	Status              ProducerWorkerStatus
	validOffsets        TopicOffsetRanges
	latestValueProduced LatestValueMap

	// Used for enabling transactional produces
	transactionsEnabled  bool
	transactionSTMConfig worker.TransactionSTMConfig
	transactionSTM       *worker.TransactionSTM
	churnProducers       bool

	tolerateDataLoss      bool
	tolerateFailedProduce bool

	validateLatestValues bool
}

func NewProducerWorker(cfg ProducerConfig) ProducerWorker {
	validOffsets := LoadTopicOffsetRanges(cfg.workerCfg.Topic, cfg.nPartitions)
	if cfg.workerCfg.TolerateDataLoss {
		for ix := range validOffsets.PartitionRanges {
			validOffsets.PartitionRanges[ix].TolerateDataLoss = true
		}
	}

	// If we produce tombstones, we may need to adjust the offsets we attempt to consume up to in the sequential read worker (for tombstone records that represent the HWM and have been removed due to retention)
	if cfg.producesTombstones {
		validOffsets.AdjustConsumableOffsets = true
	}

	return ProducerWorker{
		config:                cfg,
		Status:                NewProducerWorkerStatus(cfg.workerCfg.Topic),
		latestValueProduced:   NewLatestValueMap(cfg.workerCfg.Topic, cfg.nPartitions),
		validOffsets:          validOffsets,
		churnProducers:        cfg.messagesPerProducerId > 0,
		tolerateDataLoss:      cfg.workerCfg.TolerateDataLoss,
		tolerateFailedProduce: cfg.workerCfg.TolerateFailedProduce,
		validateLatestValues:  cfg.workerCfg.ValidateLatestValues,
	}
}

func (v *ProducerWorker) EnableTransactions(config worker.TransactionSTMConfig) {
	v.transactionSTMConfig = config
	v.transactionsEnabled = true
}

func (pw *ProducerWorker) newRecord(producerId int, sequence int64) *kgo.Record {

	var header_key bytes.Buffer
	if !pw.transactionsEnabled || !pw.transactionSTM.InAbortedTransaction() {
		fmt.Fprintf(&header_key, "%06d.%018d", producerId, sequence)

	} else {
		// This message ensures that `ValidatorStatus.ValidateRecord`
		// will report it as an invalid read if it's consumed. This is
		// since messages in aborted transactions should never be read.
		fmt.Fprintf(&header_key, "ABORTED MSG: %06d.%018d", producerId, sequence)
		pw.Status.AbortedTransactionMessages += 1
	}

	var payload []byte
	isTombstone := rand.Float64() < pw.config.valueGenerator.TombstoneProbability
	if isTombstone {
		payload = nil
		pw.Status.TombstonesProduced += 1
	} else {
		if pw.validateLatestValues {
			var value bytes.Buffer
			fmt.Fprintf(&value, "value-%018d", sequence)
			paddingSize := pw.config.messageSize - value.Len()
			if paddingSize > 0 {
				value.Write(make([]byte, paddingSize))
			}
			payload = value.Bytes()
		} else if pw.config.valueGenerator.Compressible {
			payload = pw.config.valueGenerator.GenerateCompressible()
		} else if pw.config.valueGenerator.ProduceRandomBytes {
			payload = make([]byte, pw.config.valueGenerator.PayloadSize)
			rand.Read(payload)
		} else {
			payload = pw.config.valueGenerator.GenerateRandom()
		}
	}

	var r *kgo.Record

	if pw.config.keySetCardinality < 0 {
		// by default use the same value as in record id header key
		r = kgo.KeySliceRecord(header_key.Bytes(), payload)
	} else {
		// generate a random key from a set of requested cardinality
		var key bytes.Buffer
		fmt.Fprintf(&key, "key-%d", rand.Intn(pw.config.keySetCardinality))
		r = kgo.KeySliceRecord(key.Bytes(), payload)

	}

	r.Headers = append(r.Headers, kgo.RecordHeader{Key: "KGO_VERIFIER_RECORD_ID", Value: header_key.Bytes()})

	if pw.config.fakeTimestampMs != -1 {
		r.Timestamp = time.Unix(0, pw.config.fakeTimestampMs*1000000)
		pw.config.fakeTimestampMs += pw.config.fakeTimestampStepMs
	}
	return r
}

type ProducerWorkerStatus struct {
	// Topic being produced to
	Topic string `json:"topic"`
	// How many messages did we try to transmit?
	Sent int64 `json:"sent"`

	// How many messages did we send successfully (were acked
	// by the server at the offset we expected)?
	Acked int64 `json:"acked"`

	// How many messages landed at an unexpected offset?
	// (indicates retries/resends)
	BadOffsets int64 `json:"bad_offsets"`

	MaxOffsetsProduced map[int32]int64 `json:"max_offsets_produced"`

	// How many times did we restart the producer loop?
	Restarts int64 `json:"restarts"`

	// How many times produce request failed?
	Fails int64 `json:"fails"`

	// How many tombstone records were produced?
	TombstonesProduced int64 `json:"tombstones_produced"`

	// How many failures occured while trying to begin, abort,
	// or commit a transaction.
	FailedTransactions int64 `json:"failed_transactions"`

	// How many messages were sent inside aborted transactions?
	// (this is not the transaction abort count, it's the count of the messages
	//  inside those transactions)
	AbortedTransactionMessages int64 `json:"aborted_transaction_msgs"`

	// Ack latency: a private histogram for the data,
	// and a public summary for JSON output
	latency metrics.Histogram
	Latency worker.HistogramSummary `json:"latency"`

	Active bool `json:"active"`

	lock sync.Mutex

	// For emitting checkpoints on time intervals
	lastCheckpoint time.Time
}

func NewProducerWorkerStatus(topic string) ProducerWorkerStatus {
	return ProducerWorkerStatus{
		Topic:              topic,
		MaxOffsetsProduced: make(map[int32]int64),
		lastCheckpoint:     time.Now(),
		latency:            metrics.NewHistogram(metrics.NewExpDecaySample(1024, 0.015)),
	}
}

func (pw *ProducerWorker) OnAcked(r *kgo.Record) {
	pw.Status.lock.Lock()
	defer pw.Status.lock.Unlock()

	pw.Status.OnAcked(r.Partition, r.Offset)

	pw.validOffsets.Insert(r.Partition, r.Offset)
	if pw.validateLatestValues || pw.config.producesTombstones {
		pw.latestValueProduced.InsertKeyValue(r.Partition, string(r.Key), r.Value)
		pw.latestValueProduced.InsertKeyOffset(r.Partition, string(r.Key), r.Offset)
	}
}

func (self *ProducerWorkerStatus) OnAcked(Partition int32, Offset int64) {
	self.Acked += 1

	currentMax, present := self.MaxOffsetsProduced[Partition]
	if present {
		if currentMax < Offset {
			expected := currentMax + 1
			if Offset != expected {
				log.Warnf("Gap detected in produced offsets. Expected %d, but got %d", expected, Offset)
			}

			self.MaxOffsetsProduced[Partition] = Offset
		}
	} else {
		self.MaxOffsetsProduced[Partition] = Offset
	}
}

func (self *ProducerWorkerStatus) OnBadOffset() {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.BadOffsets += 1
}

func (self *ProducerWorkerStatus) OnFail() {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.Fails += 1
}

func (pw *ProducerWorker) parseLastConsumableOffsets() {
	for partition, keyOffset := range pw.latestValueProduced.latestKoByPartition {
		for key, offset := range keyOffset {
			value, exists := pw.latestValueProduced.GetValue(int32(partition), key)
			if exists && value != nil {
				if offset > pw.validOffsets.GetLastConsumableOffset(int32(partition)) {
					pw.validOffsets.SetLastConsumableOffset(int32(partition), offset)
				}
			}
		}
	}
}

func (pw *ProducerWorker) Store() {
	err := pw.validOffsets.Store()
	util.Chk(err, "Error writing offset map: %v", err)

	if pw.validateLatestValues {
		err = pw.latestValueProduced.Store()
		util.Chk(err, "Error writing latest value map: %v", err)
	}
}

func (pw *ProducerWorker) produceCheckpoint() {
	status, lock := pw.GetStatus()

	lock.Lock()
	data, err := json.Marshal(status)
	pw.Store()
	lock.Unlock()

	util.Chk(err, "Status serialization error")
	log.Infof("Producer status: %s", data)
}

func (pw *ProducerWorker) Wait() error {
	pw.Status.Active = true
	defer func() { pw.Status.Active = false }()

	n := int64(pw.config.messageCount)

	for {
		n_produced, bad_offsets, err := pw.produceInner(n)
		if err != nil {
			return err
		}
		n = n - n_produced

		if len(bad_offsets) > 0 {
			log.Infof("Produce stopped early, %d still to do", n)
		}

		if n <= 0 {
			return nil
		} else {
			// Record that we took another run at produceInner
			pw.Status.Restarts += 1
		}
	}
}

type BadOffset struct {
	P int32
	O int64
}

func (pw *ProducerWorker) produceInner(n int64) (int64, []BadOffset, error) {
	opts := pw.config.workerCfg.MakeKgoOpts()

	opts = append(opts, []kgo.Opt{
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.ProducerLinger(0),
	}...)

	if pw.transactionsEnabled {
		randId := uuid.New()
		tid := "p" + randId.String()
		log.Debugf("Configuring transactions with TransactionalID %s", tid)

		opts = append(opts, []kgo.Opt{
			kgo.TransactionalID(tid),
		}...)
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		log.Errorf("Error creating Kafka client: %v", err)
		return 0, nil, err
	}

	if pw.transactionsEnabled {
		pw.transactionSTM = worker.NewTransactionSTM(context.Background(), client, pw.transactionSTMConfig)
	}

	nextOffset := GetOffsets(client, pw.config.workerCfg.Topic, pw.config.nPartitions, -1)

	for i, o := range nextOffset {
		log.Infof("Produce start offset %s/%d %d...", pw.config.workerCfg.Topic, i, o)
	}

	var wg sync.WaitGroup

	errored := false
	produced := int64(0)

	// Channel must be >= concurrency
	bad_offsets := make(chan BadOffset, 16384)
	concurrent := semaphore.NewWeighted(4096)

	log.Infof("Producing %d messages (%d bytes), rate limit=%d", n, pw.config.messageSize, pw.config.rateLimitBytes)

	var rlimiter *rate.Limiter
	if pw.config.rateLimitBytes > 0 {
		rlimiter = rate.NewLimiter(rate.Limit(pw.config.rateLimitBytes), pw.config.rateLimitBytes)
	}

	// Per-partition tracking for transaction control record offsets.
	// txPartitions tracks which partitions the current transaction has
	// produced to (for commit/abort marker offset prediction).
	// txPartitionSeen tracks whether we've already accounted for the
	// fence batch on a partition in this transaction.
	txPartitions := make(map[int32]bool)
	txPartitionSeen := make(map[int32]bool)

	for i := int64(0); i < n && !errored; i = i + 1 {
		concurrent.Acquire(context.Background(), 1)
		produced += 1
		pw.Status.Sent += 1
		var p = rand.Int31n(pw.config.nPartitions)

		if pw.transactionsEnabled {
			willEnd := pw.transactionSTM.WillEndTransaction()

			err := pw.transactionSTM.BeforeMessageSent()
			if err != nil {
				log.Errorf("Transaction error %v", err)
				errored = true
				pw.Status.FailedTransactions += 1
				break
			}

			if willEnd {
				// EndTransaction writes one commit/abort control
				// batch to each partition the transaction touched
				for tp := range txPartitions {
					nextOffset[tp] += 1
				}
				txPartitions = make(map[int32]bool)
				txPartitionSeen = make(map[int32]bool)
			}

			// First produce to this partition in this transaction
			// triggers AddPartitionsToTxn which writes a fence
			// batch (1 offset) before the data record
			if !txPartitionSeen[p] {
				nextOffset[p] += 1
				txPartitionSeen[p] = true
			}
			txPartitions[p] = true
		}

		if pw.churnProducers && pw.Status.Sent > 0 && pw.Status.Sent%int64(pw.config.messagesPerProducerId) == 0 {
			break
		}

		expectOffset := nextOffset[p]
		nextOffset[p] += 1

		r := pw.newRecord(0, expectOffset)
		r.Partition = p
		wg.Add(1)

		if rlimiter != nil {
			rlimiter.WaitN(context.Background(), len(r.Value))
		}

		log.Debugf("Writing partition %d at %d", r.Partition, expectOffset)

		sentAt := time.Now()
		handler := func(r *kgo.Record, err error) {
			concurrent.Release(1)

			if err != nil {
				pw.Status.OnFail()
				errHandler := util.Die
				if pw.tolerateFailedProduce {
					errHandler = log.Warnf
				}
				errHandler("Produce failed: %v", err)
				errored = true
			} else if expectOffset != r.Offset {
				log.Warnf("Produced at unexpected offset %d (expected %d) on partition %d", r.Offset, expectOffset, r.Partition)
				pw.Status.OnBadOffset()
				bad_offsets <- BadOffset{r.Partition, r.Offset}
				errored = true
				log.Debugf("errored = %t", errored)
			} else {
				ackLatency := time.Now().Sub(sentAt)
				pw.OnAcked(r)
				pw.Status.latency.Update(ackLatency.Microseconds())
				log.Debugf("Wrote partition %d at %d", r.Partition, r.Offset)
			}
			wg.Done()
		}
		client.Produce(context.Background(), r, handler)

		// Not strictly necessary, but useful if a long running producer gets killed
		// before finishing

		if time.Since(pw.Status.lastCheckpoint) > 5*time.Second {
			pw.Status.lastCheckpoint = time.Now()
			pw.produceCheckpoint()
		}
	}

	if pw.transactionsEnabled {
		if err := pw.transactionSTM.TryEndTransaction(); err != nil {
			log.Errorf("unable to end transaction: %v", err)
			errored = true
			pw.Status.FailedTransactions += 1
		}
	}

	log.Info("Waiting...")
	wg.Wait()
	log.Info("Waited.")
	wg.Wait()
	close(bad_offsets)

	log.Info("Closing client...")
	client.Close()
	log.Info("Closed client.")

	pw.parseLastConsumableOffsets()
	pw.produceCheckpoint()

	if errored {
		log.Warnf("%d bad offsets", len(bad_offsets))
		var r []BadOffset
		for o := range bad_offsets {
			r = append(r, o)
		}
		if len(r) == 0 && pw.Status.Fails == 0 && pw.Status.FailedTransactions == 0 {
			util.Die("No bad offsets or failed produces or transactions but errored?")
		}
		successful_produced := produced - int64(len(r))
		return successful_produced, r, nil
	} else {
		wg.Wait()
		return produced, nil, nil
	}
}

func (pw *ProducerWorker) ResetStats() {
	pw.Status = NewProducerWorkerStatus(pw.config.workerCfg.Topic)
}

func (pw *ProducerWorker) GetStatus() (interface{}, *sync.Mutex) {
	// Update public summary from private statustics
	pw.Status.Latency = worker.SummarizeHistogram(&pw.Status.latency)

	return &pw.Status, &pw.Status.lock
}
