// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cmd

import (
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	wasm "github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/wasm"
)

func NewWasmCommand(fs afero.Fs) *cobra.Command {
	command := &cobra.Command{
		Use:	"wasm",
		Short:	"Create and upload inline WASM engine scripts",
	}
	command.AddCommand(wasm.NewGenerateCommand(fs))
	return command
}
