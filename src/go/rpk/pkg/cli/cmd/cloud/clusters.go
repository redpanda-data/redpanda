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
	"errors"
	"io"
	"strconv"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/vcloud/config"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/vcloud/ui"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/vcloud/yak"
)

func NewClustersCommand(fs afero.Fs) *cobra.Command {
	var (
		namespaceName string
	)
	command := &cobra.Command{
		Use:   "clusters",
		Short: "Get clusters in given namespace",
		Long:  `List clusters that you have created in your namespace in your vectorized cloud organization.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if namespaceName == "" {
				return errors.New("please provide --namespace flag")
			}
			yakClient := yak.NewYakClient(config.NewVCloudConfigReaderWriter(fs))
			return GetClusters(yakClient, log.StandardLogger().Out, namespaceName)
		},
	}

	command.Flags().StringVarP(
		&namespaceName,
		"namespace",
		"n",
		"",
		"Namespace name from your vectorized cloud organization",
	)

	return command
}

func GetClusters(
	c yak.CloudApiClient, out io.Writer, namespaceName string,
) error {
	clusters, err := c.GetClusters(namespaceName)
	if _, ok := err.(yak.ErrLoginTokenMissing); ok {
		log.Info("Please run `rpk cloud login` first. ")
		return err
	}

	if err != nil {
		return err
	}

	printFormattedClusters(clusters, out)
	return nil
}

func printFormattedClusters(clusters []*yak.Cluster, out io.Writer) {
	t := ui.NewVcloudTable(out)
	t.SetHeader([]string{"id", "name", "ready"})
	for _, c := range clusters {
		t.Append([]string{
			c.Id,
			c.Name,
			strconv.FormatBool(c.Ready),
		})
	}
	t.Render()
}
