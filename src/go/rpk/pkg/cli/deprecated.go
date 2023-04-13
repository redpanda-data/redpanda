// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cli

import (
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/debug"
	"github.com/spf13/cobra"
)

func deprecateCmd(newCmd *cobra.Command, newUse string) *cobra.Command {
	newCmd.Deprecated = fmt.Sprintf("use %q instead", newUse)
	newCmd.Hidden = true
	if children := newCmd.Commands(); len(children) > 0 {
		for _, child := range children {
			deprecateCmd(child, newUse+" "+child.Name())
		}
	}
	return newCmd
}

// All architectures had `rpk status`

func newStatusCommand() *cobra.Command {
	return deprecateCmd(debug.NewInfoCommand(), "rpk debug info")
}
