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
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/common"
	"golang.org/x/sync/errgroup"
)

type header struct {
	Key	string	`json:"key"`
	Value	string	`json:"value"`
}

type message struct {
	Headers		[]header	`json:"headers,omitempty"`
	Key		string		`json:"key,omitempty"`
	Message		string		`json:"message"`
	Partition	int32		`json:"partition"`
	Offset		int64		`json:"offset"`
	Timestamp	time.Time	`json:"timestamp"`
}

type consumerGroupHandler struct {
	commit		bool
	prettyPrint	bool
}

func (g *consumerGroupHandler) Setup(s sarama.ConsumerGroupSession) error {
	return nil
}

func (g *consumerGroupHandler) Cleanup(s sarama.ConsumerGroupSession) error {
	return nil
}

func (g *consumerGroupHandler) ConsumeClaim(
	s sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim,
) error {
	mu := sync.Mutex{}	// Synchronizes stdout.
	consumeMessages(claim.Messages(), nil, &mu, s.Context(), g.prettyPrint)
	return nil
}

func NewConsumeCommand(client func() (sarama.Client, error)) *cobra.Command {
	var (
		prettyPrint	bool
		offset		string
		group		string
		groupCommit	bool
		partitions	[]int32
	)
	cmd := &cobra.Command{
		Use:	"consume <topic>",
		Short:	"Consume records from a topic",
		Args:	common.ExactArgs(1, "topic's name is missing."),
		// We don't want Cobra printing CLI usage help if the error isn't about CLI usage.
		SilenceUsage:	true,
		RunE: func(cmd *cobra.Command, args []string) error {
			cl, err := client()
			if err != nil {
				log.Error("Couldn't initialize API client")
				return err
			}
			defer cl.Close()

			topic := args[0]
			if group != "" {
				return withConsumerGroup(
					cl,
					topic,
					group,
					groupCommit,
					prettyPrint,
				)
			}
			off, err := parseOffset(offset)
			if err != nil {
				log.Errorf("Couldn't parse offset: '%s'", offset)
				return err
			}
			return withoutConsumerGroup(
				cl,
				topic,
				partitions,
				off,
				prettyPrint,
			)
		},
	}

	cmd.Flags().BoolVar(
		&prettyPrint,
		"pretty-print",
		true,
		"Pretty-print the consumed messages.",
	)
	cmd.Flags().StringVar(
		&offset,
		"offset",
		"oldest",
		"Offset to start consuming. Supported values: oldest, newest",
	)
	cmd.Flags().Int32SliceVarP(
		&partitions,
		"partitions",
		"p",
		[]int32{},
		"Partitions to consume from",
	)
	cmd.Flags().StringVarP(
		&group,
		"group",
		"g",
		"",
		"Consumer Group to use for consuming",
	)
	cmd.Flags().BoolVar(
		&groupCommit,
		"commit",
		false,
		"Commit group offset after receiving messages."+
			" Works only if consuming as Consumer Group",
	)
	return cmd
}

func withConsumerGroup(
	client sarama.Client, topic, group string, commit, prettyPrint bool,
) error {
	cg, err := sarama.NewConsumerGroupFromClient(group, client)
	if err != nil {
		log.Error("Failed to create consumer group")
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	err = cg.Consume(
		ctx,
		[]string{topic},
		&consumerGroupHandler{commit, prettyPrint},
	)
	if err != nil {
		cancel()
		log.Error("Error on consume")
	}
	return err
}

func withoutConsumerGroup(
	client sarama.Client,
	topic string,
	partitions []int32,
	offset int64,
	prettyPrint bool,
) error {
	var err error
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Error("Unable to create consumer from client")
		return err
	}

	if len(partitions) == 0 {
		partitions, err = consumer.Partitions(topic)
		if err != nil {
			log.Error("Unable to get partitions")
			return err
		}
	}

	grp, ctx := errgroup.WithContext(context.Background())
	mu := sync.Mutex{}	// Synchronizes stdout.
	for _, partition := range partitions {
		partition = partition
		offset = offset
		grp.Go(func() error {
			req := &sarama.OffsetRequest{
				Version: int16(1),
			}
			req.AddBlock(topic, partition, int64(-1), int32(0))

			pc, err := consumer.ConsumePartition(topic, partition, offset)
			if err != nil {
				log.Errorf(
					"Unable to consume topic '%s', partition %d at offset %d",
					topic,
					partition,
					offset,
				)
				return err
			}

			consumeMessages(pc.Messages(), pc.Errors(), &mu, ctx, prettyPrint)

			return err

		})
	}
	return grp.Wait()
}

func consumeMessages(
	msgs <-chan *sarama.ConsumerMessage,
	errs <-chan *sarama.ConsumerError,
	mu *sync.Mutex,
	ctx context.Context,
	prettyPrint bool,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-msgs:
			handleMessage(msg, mu, prettyPrint)
		case err := <-errs:
			log.Errorf(
				"Got an error consuming topic '%s', partition %d: %v",
				err.Topic,
				err.Partition,
				err.Err,
			)
		}
	}
}

func handleMessage(
	msg *sarama.ConsumerMessage, mu *sync.Mutex, prettyPrint bool,
) {
	// Sometimes sarama will send nil messages.
	if msg == nil {
		log.Debug("Got a nil message")
		return
	}
	m := message{
		Headers:	make([]header, 0, len(msg.Headers)),
		Message:	string(msg.Value),
		Partition:	msg.Partition,
		Offset:		msg.Offset,
		Timestamp:	msg.Timestamp,
	}
	for _, h := range msg.Headers {
		m.Headers = append(
			m.Headers,
			header{
				Key:	string(h.Key),
				Value:	string(h.Value),
			},
		)
	}
	if msg.Key != nil && len(msg.Key) > 0 {
		m.Key = string(msg.Key)
	}
	var out []byte
	var err error
	if prettyPrint {
		out, err = json.MarshalIndent(m, "", " ")
	} else {
		out, err = json.Marshal(m)
	}

	if err != nil {
		out = []byte(fmt.Sprintf(
			"Couldn't format message as JSON. Partition %d, offset %d.",
			msg.Partition,
			msg.Offset,
		))
	}

	mu.Lock()
	log.Infoln(string(out))
	mu.Unlock()
}

func parseOffset(offset string) (int64, error) {
	switch offset {
	case "oldest":
		return sarama.OffsetOldest, nil
	case "newest":
		return sarama.OffsetNewest, nil
	default:
		// TODO: normally we would parse this to int64 but it's
		// difficult as we can have multiple partitions. need to
		// find a way to give offsets from CLI with a good
		// syntax.
		return sarama.OffsetNewest, nil
	}
}
