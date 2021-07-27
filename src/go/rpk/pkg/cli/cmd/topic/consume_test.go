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
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestConsumeMessages(t *testing.T) {
	tests := []struct {
		name string
		err  *sarama.ConsumerError
		msg  *sarama.ConsumerMessage
	}{
		{
			name: "it should print a message",
			msg: &sarama.ConsumerMessage{
				Key:   []byte("keyo"),
				Value: []byte("Messagio"),
			},
		},
		{
			name: "it shouldn't print a message if it's nil",
		},
		{
			name: "it should log errors",
			err: &sarama.ConsumerError{
				Topic:     "newtopic",
				Partition: 1,
				Err:       errors.New("Bad luck, yo"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			var out bytes.Buffer
			msgs := make(chan *sarama.ConsumerMessage, 1)
			errs := make(chan *sarama.ConsumerError, 1)
			ctx, _ := context.WithTimeout(context.Background(), 100*time.Millisecond)

			logrus.SetOutput(&out)
			logrus.SetLevel(logrus.DebugLevel)

			msgs <- tt.msg

			if tt.err != nil {
				errs <- tt.err
			}

			consumeMessages(msgs, errs, &sync.Mutex{}, ctx, false, false)

			if tt.err != nil {
				errMsg := fmt.Sprintf(
					"Got an error consuming topic '%s',"+
						" partition %d: %v",
					tt.err.Topic,
					tt.err.Partition,
					tt.err.Err,
				)
				require.Contains(
					st,
					out.String(),
					errMsg,
				)
			}
			if tt.msg != nil {
				require.Contains(st, out.String(), string(tt.msg.Value))
			} else {
				require.Contains(st, out.String(), "Got a nil message")
			}
		})
	}
}
