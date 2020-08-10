package api

import (
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"time"
	"vectorized/pkg/kafka"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
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
		Short: "Produce a record. Reads data from stdin.",
		Args:  cobra.ExactArgs(1),
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
	for _, h := range headers {
		v := strings.SplitN(h, ":", 2)
		if len(v) != 2 {
			err := fmt.Errorf(
				"'%s' doesn't conform to the <k>:<v> format",
				h,
			)
			return hs, err
		}
		key := strings.Trim(v[0], " ")
		value := strings.Trim(v[1], " ")
		hs = append(
			hs,
			sarama.RecordHeader{
				Key:   []byte(key),
				Value: []byte(value),
			},
		)
	}
	return hs, nil
}
