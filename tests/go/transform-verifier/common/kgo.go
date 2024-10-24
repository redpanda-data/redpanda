/*
* Copyright 2023 Redpanda Data, Inc.
*
* Use of this software is governed by the Business Source License
* included in the file licenses/BSL.md
*
* As of the Change Date specified in that file, in accordance with
* the Business Source License, use of this software will be governed
* by the Apache License, Version 2.0
 */

package common

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	brokers []string
	topic   string
)

func AddFranzGoFlags(rootCmd *cobra.Command) {
	rootCmd.PersistentFlags().StringSliceVar(&brokers, "brokers", nil, "seed brokers to connect to")
	rootCmd.MarkPersistentFlagRequired("brokers")
	rootCmd.PersistentFlags().StringVar(&topic, "topic", "", "the topic to use")
	rootCmd.MarkPersistentFlagRequired("topic")
}

func RecordSize(r *kgo.Record) int {
	headerSize := 0
	for _, h := range r.Headers {
		headerSize += len(h.Key) + len(h.Value)
	}
	return len(r.Key) + len(r.Value) + headerSize
}

func FindHeader(r *kgo.Record, key string) ([]byte, error) {
	for _, h := range r.Headers {
		if h.Key == key {
			return h.Value, nil
		}
	}
	return nil, fmt.Errorf("unable to find header: %q", key)
}

func MakeSeqnoHeader(seqno uint64) kgo.RecordHeader {
	seqnoHeaderValue := make([]byte, bits.Len64(math.MaxUint64))
	binary.LittleEndian.PutUint64(seqnoHeaderValue, seqno)
	return kgo.RecordHeader{
		Key:   "seqno",
		Value: seqnoHeaderValue,
	}
}

func FindSeqnoHeader(r *kgo.Record) (uint64, error) {
	v, err := FindHeader(r, "seqno")
	if err != nil {
		return 0, err
	}
	seqno := binary.LittleEndian.Uint64(v)
	return seqno, nil
}

func NewClient(opts ...kgo.Opt) (*kgo.Client, error) {
	finalOps := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.DefaultProduceTopic(topic),
	}
	finalOps = append(finalOps, opts...)
	return kgo.NewClient(finalOps...)

}

func TopicMetadata(ctx context.Context) (kadm.TopicPartitions, error) {
	client, err := NewClient()
	if err != nil {
		return kadm.TopicPartitions{}, fmt.Errorf("unable to create client: %v", err)
	}
	adm := kadm.NewClient(client)
	defer adm.Close()
	meta, err := adm.Metadata(ctx, topic)
	if err != nil {
		return kadm.TopicPartitions{}, fmt.Errorf("unable to fetch metadata: %v", err)
	}
	topics := meta.Topics.TopicsList()
	if len(topics) != 1 {
		return kadm.TopicPartitions{}, fmt.Errorf("unable to fetch metadata: %v", err)
	}
	return topics[0], nil
}
