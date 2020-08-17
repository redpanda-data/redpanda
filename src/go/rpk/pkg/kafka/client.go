package kafka

import (
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

func InitClient(brokers ...string) (sarama.Client, error) {
	saramaConf := sarama.NewConfig()
	saramaConf.Version = sarama.V2_4_0_0
	saramaConf.Producer.Return.Successes = true
	timeout := 1 * time.Second
	saramaConf.ClientID = "rpk"
	saramaConf.Admin.Timeout = timeout
	saramaConf.Metadata.Timeout = timeout
	// sarama shuffles the addresses, so there's no need to do it.
	return sarama.NewClient(brokers, saramaConf)
}

/*
 * Gets the offset of the last message that was successfully copied to all
 * of the replicas.
 */
func HighWatermarks(
	client sarama.Client, topic string, partitionIDs []int32,
) (map[int32]int64, error) {
	leaders := make(map[*sarama.Broker][]int32)

	// Get the leader for each partition
	for _, partition := range partitionIDs {
		leader, err := client.Leader(topic, partition)
		if err != nil {
			errMsg := "Unable to get available offsets for" +
				" partition without leader." +
				" Topic: '%s', partition: '%d': %v"
			return nil, fmt.Errorf(errMsg, topic, partition, err)
		}
		leaders[leader] = append(leaders[leader], partition)
	}
	wg := sync.WaitGroup{}
	wg.Add(len(leaders))

	type result struct {
		watermarks map[int32]int64
		err        error
	}

	results := make(chan result, len(leaders))

	for leader, partitionIDs := range leaders {
		req := &sarama.OffsetRequest{
			Version: int16(1),
		}

		for _, partition := range partitionIDs {
			// Request each partition's offsets
			req.AddBlock(topic, partition, int64(-1), int32(0))
		}

		// Query leaders concurrently
		go func(leader *sarama.Broker, req *sarama.OffsetRequest) {
			resp, err := leader.GetAvailableOffsets(req)
			if err != nil {
				err := fmt.Errorf("Unable to get available offsets: %v", err)
				results <- result{err: err}
				return
			}

			watermarks := make(map[int32]int64)
			for partition, block := range resp.Blocks[topic] {
				watermarks[partition] = block.Offset
			}

			results <- result{watermarks: watermarks}
			wg.Done()

		}(leader, req)

	}

	wg.Wait()
	close(results)

	watermarks := make(map[int32]int64)
	// Collect the results
	for res := range results {
		if res.err != nil {
			return nil, res.err
		}
		for partition, offset := range res.watermarks {
			watermarks[partition] = offset
		}
	}

	return watermarks, nil
}
