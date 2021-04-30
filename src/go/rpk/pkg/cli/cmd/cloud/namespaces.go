// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cloud

import (
	"fmt"
	"io"

	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/vcloud/config"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/vcloud/ui"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/vcloud/yak"
)

func NewNamespacesCommand(fs afero.Fs) *cobra.Command {
	return &cobra.Command{
		Use:     "namespaces",
		Aliases: []string{"ns"},
		Short:   "Get namespaces in your vectorized cloud",
		Long:    `List namespaces that you have created in your vectorized cloud organization.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			yakClient := yak.NewYakClient(config.NewVCloudConfigReaderWriter(fs))
			return GetNamespaces(yakClient, logrus.StandardLogger().Out)
		},
	}
}

func GetNamespaces(c yak.CloudApiClient, out io.Writer) error {
	ns, err := c.GetNamespaces()
	if _, ok := err.(yak.ErrLoginTokenMissing); ok {
		log.Info("Please run `rpk cloud login` first. ")
		return err
	}
	if err != nil {
		return err
	}

	printFormatted(ns, out)
	return nil
}

func printFormatted(ns []*yak.Namespace, out io.Writer) {
	t := ui.NewVcloudTable(out)
	t.SetHeader([]string{"id", "name", "clusters"})
	for _, n := range ns {
		t.Append([]string{
			n.Id,
			n.Name,
			fmt.Sprint(len(n.ClusterIds)),
		})
	}
	t.Render()
}
