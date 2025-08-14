// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"
	"strconv"
	"sync"
	"time"

	"github.com/redpanda-data/redpanda/tests/go/kgo-verifier/pkg/util"
	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/redpanda-data/redpanda/tests/go/kgo-verifier/pkg/worker"
	"github.com/redpanda-data/redpanda/tests/go/kgo-verifier/pkg/worker/verifier"
)

var (
	debug               = flag.Bool("debug", false, "Enable verbose logging")
	trace               = flag.Bool("trace", false, "Enable super-verbose (franz-go internals)")
	brokers             = flag.String("brokers", "localhost:9092", "comma delimited list of brokers")
	topic               = flag.String("topic", "", "topic to produce to or consume from")
	username            = flag.String("username", "", "SASL username")
	password            = flag.String("password", "", "SASL password")
	enableTls           = flag.Bool("enable-tls", false, "Enables use of TLS")
	mSize               = flag.Int("msg_size", 16384, "Size of messages to produce")
	pCount              = flag.Int("produce_msgs", 0, "Number of messages to produce")
	cCount              = flag.Int("rand_read_msgs", 0, "Number of validation reads to do from each random reader")
	seqRead             = flag.Bool("seq_read", false, "Whether to do sequential read validation")
	parallelRead        = flag.Int("parallel", 1, "How many readers to run in parallel")
	seqConsumeCount     = flag.Int("seq_read_msgs", -1, "Seq/group consumer: set max number of records to consume")
	batchMaxBytes       = flag.Int("batch_max_bytes", 1048576, "the maximum batch size to allow per-partition (must be less than Kafka's max.message.bytes, producing)")
	cgReaders           = flag.Int("consumer_group_readers", 0, "Number of parallel readers in the consumer group")
	cgName              = flag.String("consumer_group_name", "", "The name of the consumer group. Generated randomly if not set.")
	linger              = flag.Duration("linger", 0, "if non-zero, linger to use when producing")
	maxBufferedRecords  = flag.Uint("max-buffered-records", 1024, "Producer buffer size: the default of 1 is makes roughly one event per batch, useful for measurement.  Set to something higher to make it easier to max out bandwidth.")
	remote              = flag.Bool("remote", false, "Remote control mode, driven by HTTP calls, for use in automated tests")
	remotePort          = flag.Uint("remote-port", 7884, "HTTP listen port for remote control/query")
	loop                = flag.Bool("loop", false, "For readers, repeatedly consume from the beginning, looping to the beginning after hitting the end of the topic until stopped via signal")
	continuous          = flag.Bool("continuous", false, "For readers, wait for new messages to arrive after hitting the end of the topic until stopped via signal or HTTP call")
	name                = flag.String("client-name", "kgo", "Name of kafka client")
	fakeTimestampMs     = flag.Int64("fake-timestamp-ms", -1, "Producer: set artificial batch timestamps on an incrementing basis, starting from this number")
	fakeTimestampStepMs = flag.Int64("fake-timestamp-step-ms", 1, "Producer: step size used to increment fake timestamp")
	consumeTputMb       = flag.Int("consume-throughput-mb", -1, "Seq/group consumer: set max throughput in mb/s")
	produceRateLimitBps = flag.Int("produce-throughput-bps", -1, "Producer: set max throughput in bytes/s")
	keySetCardinality   = flag.Int("key-set-cardinality", -1, "Cardinality of a set of possible record keys (makes data compactible)")
	msgsPerProducerId   = flag.Int("msgs-per-producer-id", -1, "Number of messages produced before creating new Producer Client.  (used to generate many producer ids)")

	useTransactions      = flag.Bool("use-transactions", false, "Use a transactional producer. Consumer with ReadComitted isolation level.")
	transactionAbortRate = flag.Float64("transaction-abort-rate", 0.0, "The probability that any given transaction should abort")
	msgsPerTransaction   = flag.Uint("msgs-per-transaction", 1, "The number of messages that should be in a given transaction")

	compressionType     = flag.String("compression-type", "", "One of none, gzip, snappy, lz4, zstd, or 'mixed' to pick a random codec for each producer")
	compressiblePayload = flag.Bool("compressible-payload", false, "If true, use a highly compressible payload instead of the default random payload")

	tolerateDataLoss      = flag.Bool("tolerate-data-loss", false, "If true, tolerate data-loss events")
	tolerateFailedProduce = flag.Bool("tolerate-failed-produce", false, "If true, tolerate and retry failed produce")
	tombstoneProbability  = flag.Float64("tombstone-probability", 0.0, "The probability (between 0.0 and 1.0) that a record produced is a tombstone record.")
	compacted             = flag.Bool("compacted", false, "Whether the topic to be verified is compacted or not. This will suppress warnings about offset gaps in consumed values.")
	validateLatestValues  = flag.Bool("validate-latest-values", false, "If true, values consumed by a worker will be validated against the last produced value by a producer. This value should only be set if compaction has been allowed to fully de-duplicate the entirety of the log before consuming.")

	produceRandomBytes = flag.Bool("produce-random-bytes", false, "If true, when generating random values, generate random bytes rather than random ascii")
)

func makeWorkerConfig() worker.WorkerConfig {
	c := worker.WorkerConfig{
		Brokers:               *brokers,
		Trace:                 *trace,
		Topic:                 *topic,
		Linger:                *linger,
		MaxBufferedRecords:    *maxBufferedRecords,
		BatchMaxbytes:         uint(*batchMaxBytes),
		SaslUser:              *username,
		SaslPass:              *password,
		UseTls:                *enableTls,
		Name:                  *name,
		Transactions:          *useTransactions,
		CompressionType:       *compressionType,
		CompressiblePayload:   *compressiblePayload,
		TolerateDataLoss:      *tolerateDataLoss,
		TolerateFailedProduce: *tolerateFailedProduce,
		Continuous:            *continuous,
		ValidateLatestValues:  *validateLatestValues,
	}

	return c
}

type workersGroup struct {
	workers []worker.Worker
	ready   bool
	mu      sync.Mutex
}

func (w *workersGroup) Add(worker worker.Worker) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.workers = append(w.workers, worker)
}

func (w *workersGroup) SetReady() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.workers) == 0 {
		util.Die("No workers added to the group, cannot set ready")
	}

	if w.ready {
		util.Die("Workers group is already marked as ready")
	}

	w.ready = true
}

func (w *workersGroup) StatusJson() ([]byte, error) {
	if !w.ready {
		return nil, errors.New("workers group is not marked as ready yet")
	}

	var results []interface{}
	var locks []*sync.Mutex
	for _, v := range w.workers {
		status, lock := v.GetStatus()
		results = append(results, status)
		locks = append(locks, lock)
	}

	for _, lock := range locks {
		lock.Lock()
	}

	defer func() {
		for _, lock := range locks {
			lock.Unlock()
		}
	}()

	serialized, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to serialize worker status: %w", err)
	}

	return serialized, nil
}

func (w *workersGroup) ResetStats() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.ready {
		return errors.New("workers group is not marked as ready yet")
	}

	for _, worker := range w.workers {
		worker.ResetStats()
	}

	return nil
}

func main() {
	flag.Parse()

	if *topic == "" {
		util.Die("No topic specified (use -topic)")
	}

	if *debug || *trace {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	signalChan := make(chan os.Signal, 1)
	if *remote {
		signal.Notify(signalChan, os.Interrupt)
	}

	// Once we are done, keep the process alive until this channel is fired
	shutdownChan := make(chan int, 1)

	// For sequential consumers, keep re-reading the topic from start until
	// this channel is fired.
	lastPassChan := make(chan int, 1)

	log.Info("Getting topic metadata...")
	conf := makeWorkerConfig()
	opts := conf.MakeKgoOpts()
	client, err := kgo.NewClient(opts...)
	util.Chk(err, "Error creating kafka client: %v", err)

	var t kmsg.MetadataResponseTopic
	{
		req := kmsg.NewPtrMetadataRequest()
		reqTopic := kmsg.NewMetadataRequestTopic()
		reqTopic.Topic = kmsg.StringPtr(*topic)
		req.Topics = append(req.Topics, reqTopic)

		resp, err := req.RequestWith(context.Background(), client)
		util.Chk(err, "unable to request topic metadata: %v", err)
		if len(resp.Topics) != 1 {
			util.Die("metadata response returned %d topics when we asked for 1", len(resp.Topics))
		}
		t = resp.Topics[0]
		if t.ErrorCode != 0 {
			util.Die("Error %s getting topic metadata", kerr.ErrorForCode(t.ErrorCode))
		}
	}

	nPartitions := int32(len(t.Partitions))
	log.Debugf("Targeting topic %s with %d partitions", *topic, nPartitions)

	var workers workersGroup

	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		serialized, err := workers.StatusJson()
		if err != nil {
			log.Errorf("Failed to serialize worker status: %v", err)
			http.Error(w, fmt.Sprintf("Failed to serialize worker status: %v", err), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(serialized)
	})

	mux.HandleFunc("/reset", func(w http.ResponseWriter, r *http.Request) {
		log.Info("Remote request /reset")
		workers.ResetStats()
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/shutdown", func(w http.ResponseWriter, r *http.Request) {
		log.Info("Remote request /shutdown")
		select {
		case shutdownChan <- 1:
		default:
			log.Warn("shutdown channel is full, skipping")
		}
	})

	mux.HandleFunc("/last_pass", func(w http.ResponseWriter, r *http.Request) {
		log.Info("Remote request /last_pass")
		timeout := r.URL.Query().Get("timeout")
		if len(timeout) > 0 {
			timeoutSec, err := strconv.Atoi(timeout)
			if err == nil {
				log.Infof("Setting a timeout of %v seconds to print the stack trace", timeoutSec)
				go func() {
					time.Sleep(time.Duration(timeoutSec) * time.Second)
					pprof.Lookup("goroutine").WriteTo(os.Stdout, 2)
				}()
			} else {
				log.Warn("unable to parse timeout query param, skipping printing stack trace logs")
			}
		}
		select {
		case lastPassChan <- 1:
		default:
			log.Warn("last_pass channel is full, skipping")
		}
	})

	mux.HandleFunc("/print_stack", func(w http.ResponseWriter, r *http.Request) {
		log.Infof("Printing stack on remote request:")
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	})

	go func() {
		listenAddr := fmt.Sprintf("0.0.0.0:%d", *remotePort)
		if err := http.ListenAndServe(listenAddr, mux); err != nil {
			panic(fmt.Sprintf("failed to listen on %s: %v", listenAddr, err))
		}
	}()

	if *loop && *continuous {
		util.Die("Cannot use -loop and -continuous together")
	}

	ctx, cancel := context.WithCancel(context.Background())
	loopState := util.NewLoopState(*loop)
	go func() {
		<-lastPassChan
		if *continuous {
			cancel()
		} else {
			loopState.RequestLastPass()
		}
	}()

	if *pCount > 0 {
		log.Info("Starting producer...")
		pwc := verifier.NewProducerConfig(makeWorkerConfig(), "producer", nPartitions, *mSize, *pCount, *fakeTimestampMs, *fakeTimestampStepMs, (*produceRateLimitBps), *keySetCardinality, *msgsPerProducerId, *tombstoneProbability, *produceRandomBytes)
		pw := verifier.NewProducerWorker(pwc)

		if *useTransactions {
			tconfig := worker.NewTransactionSTMConfig(*transactionAbortRate, *msgsPerTransaction)
			pw.EnableTransactions(tconfig)
		}

		workers.Add(&pw)
		workers.SetReady()
		waitErr := pw.Wait()
		util.Chk(err, "Producer error: %v", waitErr)
		log.Info("Finished producer.")
	} else if *seqRead {
		srw := verifier.NewSeqReadWorker(verifier.NewSeqReadConfig(
			makeWorkerConfig(), "sequential", nPartitions, *seqConsumeCount,
			(*consumeTputMb)*1024*1024,
		), verifier.NewValidatorStatus(*compacted, *validateLatestValues, *topic, nPartitions))
		workers.Add(&srw)
		workers.SetReady()

		for loopState.Next() {
			log.Info("Starting sequential read pass")
			waitErr := srw.Wait(ctx)
			if waitErr != nil {
				// Proceed around the loop, to be tolerant of e.g. kafka client
				// construct failures on unavailable cluster
				log.Warnf("Error from sequeqntial read worker: %v", err)
			}
		}
	} else if *cCount > 0 {
		var wg sync.WaitGroup
		var randomWorkers []*verifier.RandomReadWorker
		for i := 0; i < *parallelRead; i++ {
			workerCfg := verifier.NewRandomReadConfig(
				makeWorkerConfig(), fmt.Sprintf("random-%03d", i), nPartitions, *cCount,
			)
			worker := verifier.NewRandomReadWorker(workerCfg, verifier.NewValidatorStatus(*compacted, *validateLatestValues, *topic, nPartitions))
			randomWorkers = append(randomWorkers, &worker)
			workers.Add(&worker)
		}
		workers.SetReady()

		for loopState.Next() {
			for _, w := range randomWorkers {
				wg.Add(1)
				go func(worker *verifier.RandomReadWorker) {
					waitErr := worker.Wait()
					if waitErr != nil {
						// Proceed around the loop, to be tolerant of e.g. kafka client
						// construct failures on unavailable cluster
						log.Warnf("Error from random worker: %v", err)
					}
					worker.Status.Validator.Checkpoint()
					wg.Done()
				}(w)
			}
			wg.Wait()
		}
	} else if *cgReaders > 0 {
		if *loop && *cgName != "" {
			util.Die("Cannot use -loop and -consumer_group_name together")
		}

		grw := verifier.NewGroupReadWorker(
			verifier.NewGroupReadConfig(
				makeWorkerConfig(), *cgName, nPartitions, *cgReaders,
				*seqConsumeCount, (*consumeTputMb)*1024*1024), verifier.NewValidatorStatus(*compacted, *validateLatestValues, *topic, nPartitions))
		workers.Add(&grw)
		workers.SetReady()

		for loopState.Next() {
			log.Info("Starting group read pass")
			waitErr := grw.Wait(ctx)
			util.Chk(waitErr, "Consumer error: %v", err)
		}
	}

	if *remote {
		log.Info("Waiting for remote shutdown request")
		select {
		case <-signalChan:
			log.Info("Stopping on signal...")
			return
		case <-shutdownChan:
			log.Info("Remote requested shutdown, proceeding")
		}
	}

}
