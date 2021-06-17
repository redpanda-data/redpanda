// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package wasm

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/ui"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka"
)

type coprocInfo struct {
	InputTopics []string `json:"input_topics"`
	Description string   `json:"description"`
}

type coprocNodeInfo struct {
	NodeId       int32                 `json:"node_id"`
	Status       string                `json:"status"`
	Coprocessors map[string]coprocInfo `json:"coprocessors"`
}

func NewListCommand(
	clientClosure func() (sarama.Client, error),
	consumerClosure func(sarama.Client) (sarama.Consumer, error),
) *cobra.Command {
	var pretty bool

	command := &cobra.Command{
		Use:   "list",
		Short: "display the current status of functions within redpanda",
		RunE: func(_ *cobra.Command, args []string) error {
			return wasmList(clientClosure, consumerClosure, pretty)
		},
	}

	command.Flags().BoolVar(&pretty, "raw", false, "Print the raw json to stdout")
	return command
}

func printResults(dataSet *map[int32]coprocNodeInfo, raw bool) {
	if raw {
		stringRep, err := json.Marshal(dataSet)
		if err != nil {
			fmt.Errorf("Internal error with list command, failed to serialize result into json: %s", err)
		} else {
			log.Info(string(stringRep))
		}
		return
	}
	t := ui.NewRpkTable(log.StandardLogger().Out)
	t.SetColWidth(80)
	t.SetAutoWrapText(false)
	unresponsiveEngines := []int32{}
	header := []string{"Coprocessor Name", "Node ID", "Input Topics", "Description"}
	t.SetHeader(header[:])
	for _, val := range *dataSet {
		if val.Status != "up" {
			unresponsiveEngines = append(unresponsiveEngines, val.NodeId)
		}
		for name, data := range val.Coprocessors {
			topics := strings.Join(data.InputTopics, ",")
			topics = "[" + topics + "]"
			row := []string{name, strconv.Itoa(int(val.NodeId)), topics, data.Description}
			t.Append(row)
		}
	}
	t.Render()
	if len(unresponsiveEngines) > 0 {
		log.Infof("Unresponsive engines: [%v]", unresponsiveEngines)
	}
}

func accrueResults(
	messages []*sarama.ConsumerMessage,
) (*map[int32]coprocNodeInfo, error) {
	allData := make(map[int32]coprocNodeInfo)
	for _, msg := range messages {
		msgKey := int32(binary.LittleEndian.Uint32(msg.Key))
		var info coprocNodeInfo
		if err := json.Unmarshal(msg.Value, &info); err != nil {
			return nil, err
		}
		if msgKey != info.NodeId {
			return nil, fmt.Errorf("Mismatched node ids, key: %d reported %d", msgKey, info.NodeId)
		}
		allData[msgKey] = info
	}
	return &allData, nil
}

func consumeStatusUpdates(
	pc sarama.PartitionConsumer, finalOffset int64,
) ([]*sarama.ConsumerMessage, error) {
	var messages []*sarama.ConsumerMessage
	var err error
	for {
		select {
		case msg := <-pc.Messages():
			messages = append(messages, msg)
			if msg.Offset >= (finalOffset - 1) {
				return messages, err
			}
		case msgerr := <-pc.Errors():
			err = msgerr.Err
			return messages, err
		}
	}
}

func wasmList(
	clientClosure func() (sarama.Client, error),
	consumerClosure func(sarama.Client) (sarama.Consumer, error),
	raw bool,
) error {
	client, err := clientClosure()
	if err != nil {
		return fmt.Errorf("Failed to make client: %s", err)
	}
	finalOffset, err := client.GetOffset(kafka.CoprocessorStatusTopic, 0, sarama.OffsetNewest)
	if err != nil {
		if raw {
			log.Info("{\"status\": \"uninitialized\"}")
		} else {
			log.Error("CoprocessorStatusTopic is uninitialized")
		}
		return nil
	}
	if finalOffset == 0 {
		if raw {
			log.Info("{}")
		} else {
			log.Info("There are no coprocessor events to list")
		}
		return nil
	}

	consumer, err := consumerClosure(client)
	if err != nil {
		return fmt.Errorf("Failed to make consumer: %s", err)
	}

	pc, err := consumer.ConsumePartition(kafka.CoprocessorStatusTopic, 0, sarama.OffsetOldest)
	if err != nil {
		return fmt.Errorf("Failed to consumer from coprocessor status topic: %s", kafka.CoprocessorStatusTopic)
	}

	messages, err := consumeStatusUpdates(pc, finalOffset)
	consumer.Close()
	if err != nil {
		return fmt.Errorf("Failed to consume from coprocessor status topic without error: %s", err)
	}
	results, err := accrueResults(messages)
	if err != nil {
		return fmt.Errorf("Failed to parse and compile result set: %s", err)
	}
	printResults(results, raw)
	return nil
}
