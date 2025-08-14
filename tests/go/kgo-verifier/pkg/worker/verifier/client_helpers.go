package verifier

import (
	"context"
	"errors"
	"time"

	"github.com/redpanda-data/redpanda/tests/go/kgo-verifier/pkg/util"
	log "github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka"
)

type OffsetResult struct {
	offset int64
	err    error
}

// Try to get offsets, with a retry loop in case any partitions are not
// in a position to respond.  This is useful to avoid terminating if e.g.
// the cluster is subject to failure injection while workload runs.
func GetOffsets(client *kgo.Client, topic string, nPartitions int32, t int64) []int64 {
	wait_t := 2 * time.Second
	combinedResult := make([]int64, nPartitions)
	haveResult := make([]bool, nPartitions)

	req := formOffsetsReq(topic, nPartitions, t)
	for {
		result := attemptGetOffsets(client, topic, nPartitions, req)
		var seenPartitions = int32(0)
		for i := 0; i < int(nPartitions); i++ {
			if result[i].err == nil {
				// update even if seen before
				combinedResult[i] = result[i].offset
				haveResult[i] = true
			}
			if haveResult[i] {
				seenPartitions += 1
			}
		}
		if seenPartitions == nPartitions {
			return combinedResult
		}
		log.Warnf(
			"Got offsets for %d/%d partitions, retrying attemptGetOffsets in %v",
			seenPartitions, nPartitions, wait_t)
		time.Sleep(wait_t)
	}
}

func formOffsetsReq(topic string, nPartitions int32, t int64) *kmsg.ListOffsetsRequest {
	log.Infof("Loading offsets for topic %s t=%d...", topic, t)

	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID = -1
	reqTopic := kmsg.NewListOffsetsRequestTopic()
	reqTopic.Topic = topic
	for i := 0; i < int(nPartitions); i++ {
		part := kmsg.NewListOffsetsRequestTopicPartition()
		part.Partition = int32(i)
		part.Timestamp = t
		reqTopic.Partitions = append(reqTopic.Partitions, part)
	}

	req.Topics = append(req.Topics, reqTopic)
	return req
}

func attemptGetOffsets(client *kgo.Client, topic string, nPartitions int32, req *kmsg.ListOffsetsRequest) []OffsetResult {
	pOffsets := make([]OffsetResult, nPartitions)
	for i := range pOffsets {
		pOffsets[i].err = errors.New("no result")
	}

	shards := client.RequestSharded(context.Background(), req)
	kafka.EachShard(req, shards, func(shard kgo.ResponseShard) {
		util.Chk(shard.Err, "kafka.EachShard called processor fn on an error result")
		resp := shard.Resp.(*kmsg.ListOffsetsResponse)
		for _, partition := range resp.Topics[0].Partitions {
			if partition.ErrorCode != 0 {
				err := kerr.ErrorForCode(partition.ErrorCode)
				pOffsets[partition.Partition].err = err
				log.Warnf("error fetching %s/%d metadata: %v", topic, partition.Partition, err)
			} else {
				pOffsets[partition.Partition] = OffsetResult{offset: partition.Offset, err: nil}
				log.Debugf("Partition %d offset %d", partition.Partition, partition.Offset)
			}
		}
	})

	return pOffsets
}
