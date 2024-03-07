// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package schema

import (
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/schemaregistry"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/sr"
)

type deleteResponse struct {
	Subject string `json:"subject" yaml:"subject"`
	Version string `json:"version" yaml:"version"`
}

func newDeleteCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var sversion string
	var isPermanent bool
	cmd := &cobra.Command{
		Use:   "delete SUBJECT --schema-version {version}",
		Short: "Delete a specific schema for the given subject",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			f := p.Formatter
			if h, ok := f.Help(deleteResponse{}); ok {
				out.Exit(h)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			cl, err := schemaregistry.NewClient(fs, p)
			out.MaybeDie(err, "unable to initialize schema registry client: %v", err)

			version, err := parseVersion(sversion)
			out.MaybeDieErr(err)

			subject := args[0]
			if isPermanent {
				err = cl.DeleteSchema(cmd.Context(), subject, version, sr.SoftDelete)
				if err == nil || schemaregistry.IsSoftDeleteError(err) {
					err = cl.DeleteSchema(cmd.Context(), subject, version, sr.HardDelete)
					out.MaybeDie(err, "unable to perform hard-deletion: %v", err)
				}
				out.MaybeDie(err, "unable to perform initial soft-deletion that is required for hard-deletion: %v", err)
			} else {
				err = cl.DeleteSchema(cmd.Context(), subject, version, sr.SoftDelete)
				out.MaybeDieErr(err)
			}
			if isText, _, s, err := f.Format(deleteResponse{subject, sversion}); !isText {
				out.MaybeDie(err, "unable to print in the required format %q: %v", f.Kind, err)
				out.Exit(s)
			}
			fmt.Printf("Successfully deleted schema. Subject: %q, version: %q\n", subject, sversion)
		},
	}

	cmd.Flags().StringVar(&sversion, "schema-version", "", "Schema version to delete (latest, 0, 1...)")
	cmd.Flags().BoolVar(&isPermanent, "permanent", false, "Perform a hard (permanent) delete of the schema")
	cmd.MarkFlagRequired("schema-version")
	return cmd
}
