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
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/common"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/wasm"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewWasmCommand(fs afero.Fs, mgr config.Manager) *cobra.Command {
	var (
		configFile     string
		brokers        []string
		user           string
		password       string
		mechanism      string
		enableTLS      bool
		certFile       string
		keyFile        string
		truststoreFile string
	)

	command := &cobra.Command{
		Use:   "wasm",
		Short: "Deploy and remove inline WASM engine scripts.",
	}
	common.AddKafkaFlags(
		command,
		&configFile,
		&user,
		&password,
		&mechanism,
		&enableTLS,
		&certFile,
		&keyFile,
		&truststoreFile,
		&brokers,
	)
	command.AddCommand(wasm.NewGenerateCommand(fs))
	command.AddCommand(wasm.NewDeployCommand(fs))
	command.AddCommand(wasm.NewRemoveCommand(fs))

	return command
}
