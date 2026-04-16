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

// BeforeMessageSent ends the current transaction if it has reached
// msgsPerTransaction, then begins a new one if needed. Must be called
// before each produce.
func (t *TransactionSTM) BeforeMessageSent() error {
	if t.currentMgsProduced == t.config.msgsPerTransaction {
		if err := t.client.Flush(t.ctx); err != nil {
			log.Errorf("Unable to flush: %v", err)
			return err
		}
		if err := t.client.EndTransaction(t.ctx, kgo.TransactionEndTry(!t.abortedTransaction)); err != nil {
			log.Errorf("Unable to end transaction: %v", err)
			return err
		}

		log.Debugf("Ended transaction; aborted = %t", t.abortedTransaction)

		t.currentMgsProduced = 0
		t.activeTransaction = false
	}

	if !t.activeTransaction {
		if err := t.client.BeginTransaction(); err != nil {
			log.Errorf("Couldn't start a transaction: %v", err)
			return err
		}

		t.abortedTransaction = t.config.abortRate >= rand.Float64()
		t.activeTransaction = true

		log.Debugf("Started transaction; will abort = %t", t.abortedTransaction)
	}

	t.currentMgsProduced += 1
	return nil
}

func (t *TransactionSTM) InAbortedTransaction() bool {
	return t.abortedTransaction
}

// WillEndTransaction returns true if the next call to BeforeMessageSent
// will end the current transaction (msgsPerTransaction has been reached).
func (t *TransactionSTM) WillEndTransaction() bool {
	return t.activeTransaction && t.currentMgsProduced == t.config.msgsPerTransaction
}
