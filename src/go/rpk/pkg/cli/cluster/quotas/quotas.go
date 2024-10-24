// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package quotas

import (
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/types"
)

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "quotas",
		Aliases: []string{"quota"},
		Args:    cobra.NoArgs,
		Short:   "Manage Redpanda client quotas",
	}
	cmd.AddCommand(
		alterCommand(fs, p),
		describeCommand(fs, p),
		importCommand(fs, p),
	)
	p.InstallKafkaFlags(cmd)
	p.InstallFormatFlag(cmd)

	return cmd
}

// anyValidTypes are the types allowed in --name and --any flags.
var anyValidTypes = map[string]bool{
	// Supported by Redpanda.
	"client-id":        true,
	"client-id-prefix": true,
	// Not supported by Redpanda yet.
	"user": true,
	"ip":   true,
}

// defaultValidTypes are the types allowed in --default flag.
var defaultValidTypes = map[string]bool{
	// Supported by Redpanda.
	"client-id": true,
	// Not supported by Redpanda yet.
	"user": true,
	"ip":   true,
}

type entityData struct {
	Name string `json:"name" yaml:"name"`
	Type string `json:"type" yaml:"type"`
}

func parseEntityData(entity kadm.ClientQuotaEntity) ([]entityData, string) {
	var sb strings.Builder
	var ed []entityData
	types.Sort(entity)
	for i, e := range entity {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(e.Type)
		sb.WriteString("=")
		name := "<default>"
		if e.Name != nil {
			name = *e.Name
		}
		sb.WriteString(name)
		ed = append(ed, entityData{
			name, e.Type,
		})
	}
	return ed, sb.String()
}
