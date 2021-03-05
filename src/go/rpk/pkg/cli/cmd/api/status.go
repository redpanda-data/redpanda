// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package api

import (
	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/cluster"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/common"
)

type node struct {
	// map[topic-name][]partitions
	leaderParts  map[string][]int
	replicaParts map[string][]int
}

func NewStatusCommand(
	admin func() (sarama.ClusterAdmin, error),
) *cobra.Command {
	return common.Deprecated(
		cluster.NewInfoCommand(admin),
		"rpk cluster info",
	)
}
