// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package topic

import (
	"io"
	"io/ioutil"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/common"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka"
)

func NewProduceCommand(
	producer func(bool, int32) (sarama.SyncProducer, error),
) *cobra.Command {
	var (
		key            string
		headers        []string
		numRecords     int
		jvmPartitioner bool
		partition      int32
		timestamp      string
	)
	cmd := &cobra.Command{
		Use:   "produce <topic>",
		Short: "Produce a record from data entered in stdin.",
		Args:  common.ExactArgs(1, "topic's name is missing."),
		// We don't want Cobra printing CLI usage help if the error isn't about CLI usage.
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			prod, err := producer(jvmPartitioner, partition)
			if err != nil {
				log.Error("Unable to create the producer")
				return err
			}
			return produce(
				prod,
				numRecords,
				partition,
				headers,
				args[0],
				key,
				timestamp,
				cmd.InOrStdin(),
			)
		},
	}
	cmd.Flags().StringVarP(
		&key,
		"key",
		"k",
		"",
		"Key for the record. Currently only strings are supported.",
	)
	cmd.Flags().StringArrayVarP(
		&headers,
		"header",
		"H",
		[]string{},
		"Header in format <key>:<value>. May be used multiple times"+
			" to add more headers.",
	)
	cmd.Flags().IntVarP(
		&numRecords,
		"num",
		"n",
		1,
		"Number of records to send.",
	)
	cmd.Flags().BoolVarP(
		&jvmPartitioner,
		"jvm-partitioner",
		"j",
		false,
		"Use a JVM-compatible partitioner. If --partition is passed"+
			" with a positive value, this will be overridden and"+
			" a manual partitioner will be used.",
	)
	cmd.Flags().StringVarP(
		&timestamp,
		"timestamp",
		"t",
		"",
		"RFC3339-compliant timestamp for the record. If the value"+
			" passed can't be parsed, the current time will be used.",
	)
	cmd.Flags().Int32VarP(
		&partition,
		"partition",
		"p",
		-1,
		"Partition to produce to.",
	)
	return cmd
}

func produce(
	producer sarama.SyncProducer,
	numRecords int,
	partition int32,
	headers []string,
	topic string,
	key string,
	timestamp string,
	in io.Reader,
) error {
	log.Info(
		"Reading message... Press CTRL + D to send," +
			" CTRL + C to cancel.",
	)
	data, err := ioutil.ReadAll(in)
	if err != nil {
		log.Error("Unable to read data")
		return err
	}

	k := sarama.StringEncoder(key)

	ts := time.Now()
	if timestamp != "" {
		ts, err = time.Parse(time.RFC3339, timestamp)
		if err != nil {
			return err
		}
	}

	hs, err := parseHeaders(headers)
	if err != nil {
		return err
	}

	for i := 0; i < numRecords; i++ {
		msg := &sarama.ProducerMessage{
			Topic:     topic,
			Key:       k,
			Headers:   hs,
			Timestamp: ts,
			Value:     sarama.ByteEncoder(data),
		}
		if partition != -1 {
			msg.Partition = partition
		}
		retryConf := kafka.DefaultConfig().Producer.Retry
		part, offset, err := kafka.RetrySend(
			producer,
			msg,
			uint(retryConf.Max),
			retryConf.Backoff,
		)
		if err != nil {
			log.Error("Failed to send record")
			return err
		}

		log.Infof(
			"Sent record to partition %d at offset %d with timestamp %v.",
			part,
			offset,
			ts,
		)
		log.Debugf("Data: '%s'", string(data))
		log.Debugf("Headers: '%s'", strings.Join(headers, ", "))
	}
	return nil
}

func parseHeaders(headers []string) ([]sarama.RecordHeader, error) {
	var hs []sarama.RecordHeader
	kvs, err := parseKVs(headers)
	if err != nil {
		return hs, err
	}
	for k, v := range kvs {
		hs = append(
			hs,
			sarama.RecordHeader{
				Key:   []byte(k),
				Value: []byte(v),
			},
		)
	}
	return hs, nil
}
