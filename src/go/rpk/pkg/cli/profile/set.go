// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package profile

import (
	"fmt"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newSetCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return &cobra.Command{
		Use:               "set [KEY] [VALUE]",
		Short:             "Set a field in the current rpk context",
		Args:              cobra.ExactArgs(2),
		ValidArgsFunction: validSetArgs,
		Run: func(_ *cobra.Command, args []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			y, err := cfg.ActualRpkYamlOrEmpty()
			out.MaybeDie(err, "unable to load rpk.yaml: %v", err)

			p := y.Profile(y.CurrentProfile)
			if p == nil {
				out.Die("current context %q does not exist", y.CurrentProfile)
			}
			err = config.Set(&p, args[0], args[1])
			out.MaybeDieErr(err)
			err = y.Write(fs)
			out.MaybeDieErr(err)
			fmt.Printf("Field %q updated successfully.\n", args[0])
		},
	}
}

var setPossibilities = []string{
	"kafka_api.brokers",
	"kafka_api.tls.enabled",
	"kafka_api.tls.ca_file",
	"kafka_api.tls.cert_file",
	"kafka_api.tls.key_file",
	"kafka_api.sasl.user",
	"kafka_api.sasl.password",
	"kafka_api.sasl.type",
	"admin_api.addresses",
	"admin_api.tls.enabled",
	"admin_api.tls.ca_file",
	"admin_api.tls.cert_file",
	"admin_api.tls.key_file",
}

func validSetArgs(_ *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	var possibilities []string
	for _, p := range setPossibilities {
		if strings.HasPrefix(p, toComplete) {
			possibilities = append(possibilities, p)
		}
	}
	return possibilities, cobra.ShellCompDirectiveDefault
}
