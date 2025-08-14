package worker

import (
	"context"
	"math/rand"
	_ "net/http/pprof"

	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kgo"
)

type TransactionSTMConfig struct {
	abortRate          float64
	msgsPerTransaction uint
}

func NewTransactionSTMConfig(abortRate float64, msgsPerTransaction uint) TransactionSTMConfig {
	return TransactionSTMConfig{
		abortRate:          abortRate,
		msgsPerTransaction: msgsPerTransaction,
	}
}

type TransactionSTM struct {
	config TransactionSTMConfig
	client *kgo.Client
	ctx    context.Context

	activeTransaction  bool
	abortedTransaction bool
	currentMgsProduced uint
}

func NewTransactionSTM(ctx context.Context, client *kgo.Client, config TransactionSTMConfig) *TransactionSTM {
	log.Debugf("Creating TransactionSTM config = %+v", config)

	return &TransactionSTM{
		ctx:                ctx,
		config:             config,
		client:             client,
		activeTransaction:  false,
		abortedTransaction: false,
		currentMgsProduced: 0,
	}
}

func (t *TransactionSTM) TryEndTransaction() error {
	if t.activeTransaction {
		if err := t.client.Flush(t.ctx); err != nil {
			log.Errorf("Unable to flush: %v", err)
			return err
		}
		if err := t.client.EndTransaction(t.ctx, kgo.TransactionEndTry(!t.abortedTransaction)); err != nil {
			log.Errorf("Unable to end transaction: %v", err)
			return err
		}

		log.Debugf("Ended transaction early; currentMgsProduced = %d aborted = %t", t.currentMgsProduced, t.abortedTransaction)

		t.currentMgsProduced = 0
		t.activeTransaction = false
	}

	return nil
}

// Returns true iff a new transaction was started and/or a current
// transaction ended. This is to notify any producers that control
// markers will be added to a partition's log.
func (t *TransactionSTM) BeforeMessageSent() (int64, error) {
	// EndTransaction/abort and BeginTransaction will each leave
	// one control record in each partition's log.
	var addedControlMarkers int64 = 0

	if t.currentMgsProduced == t.config.msgsPerTransaction {
		if err := t.client.Flush(t.ctx); err != nil {
			log.Errorf("Unable to flush: %v", err)
			return addedControlMarkers, err
		}
		if err := t.client.EndTransaction(t.ctx, kgo.TransactionEndTry(!t.abortedTransaction)); err != nil {
			log.Errorf("Unable to end transaction: %v", err)
			return addedControlMarkers, err
		}

		log.Debugf("Ended transaction; aborted = %t", t.abortedTransaction)

		t.currentMgsProduced = 0
		t.activeTransaction = false

		addedControlMarkers += 1
	}

	// Begin new transaction if one doesn't exist
	if !t.activeTransaction {
		t.abortedTransaction = t.config.abortRate >= rand.Float64()
		t.activeTransaction = true

		if err := t.client.BeginTransaction(); err != nil {
			log.Errorf("Couldn't start a transaction: %v", err)
			return 0, err
		}

		log.Debugf("Started transaction; will abort = %t", t.abortedTransaction)

		addedControlMarkers += 1
	}

	t.currentMgsProduced += 1
	return addedControlMarkers, nil
}

func (t *TransactionSTM) InAbortedTransaction() bool {
	return t.abortedTransaction
}
