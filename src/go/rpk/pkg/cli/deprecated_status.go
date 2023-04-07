// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cli

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/common"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/debug"
	"github.com/spf13/cobra"
)

func NewStatusCommand() *cobra.Command {
	return common.Deprecated(
		debug.NewInfoCommand(),
		"rpk debug info",
	)
}
