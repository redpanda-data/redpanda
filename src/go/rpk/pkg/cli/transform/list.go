/*
* Copyright 2023 Redpanda Data, Inc.
*
* Use of this software is governed by the Business Source License
* included in the file licenses/BSL.md
*
* As of the Change Date specified in that file, in accordance with
* the Business Source License, use of this software will be governed
* by the Apache License, Version 2.0
 */

package transform

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/redpanda-data/common-go/rpadmin"

	dataplanev1alpha1 "buf.build/gen/go/redpandadata/dataplane/protocolbuffers/go/redpanda/api/dataplane/v1alpha1"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type (
	detailedTransformMetadata struct {
		Name            string                             `json:"name" yaml:"name"`
		InputTopic      string                             `json:"input_topic" yaml:"input_topic"`
		OutputTopics    []string                           `json:"output_topics" yaml:"output_topics"`
		Environment     map[string]string                  `json:"environment" yaml:"environment"`
		Status          []rpadmin.PartitionTransformStatus `json:"status" yaml:"status"`
		CompressionMode string                             `json:"compression" yaml:"compression"`
	}
	summarizedTransformMetadata struct {
		Name            string            `json:"name" yaml:"name"`
		InputTopic      string            `json:"input_topic" yaml:"input_topic"`
		OutputTopics    []string          `json:"output_topics" yaml:"output_topics"`
		Environment     map[string]string `json:"environment" yaml:"environment"`
		Running         string            `json:"running" yaml:"running"`
		Lag             int               `json:"lag" yaml:"lag"`
		CompressionMode string            `json:"compression" yaml:"compression"`
	}
)

func newListCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var detailed bool
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List data transforms",
		Long: `List data transforms.

This command lists all data transforms in a cluster, as well as showing the
state of a individual transform processor, such as if it's errored or how many
records are pending to be processed (lag).

There is a processor assigned to each partition on the input topic, and each
processor is a separate entity that can make progress or fail independently.

The --detailed flag (-d) opts in to printing extra per-processor information.
`,
		Aliases: []string{"ls"},
		Args:    cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			f := p.Formatter
			if detailed {
				if h, ok := f.Help([]detailedTransformMetadata{}); ok {
					out.Exit(h)
				}
			} else {
				if h, ok := f.Help([]summarizedTransformMetadata{}); ok {
					out.Exit(h)
				}
			}

			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitServerlessAdmin(p)

			var l []rpadmin.TransformMetadata
			if p.FromCloud && !p.CloudCluster.IsServerless() {
				url, err := p.CloudCluster.CheckClusterURL()
				out.MaybeDie(err, "unable to get cluster information: %v", err)

				cl, err := publicapi.NewDataPlaneClientSet(url, p.CurrentAuth().AuthToken)
				out.MaybeDie(err, "unable to initialize cloud client: %v", err)

				res, err := cl.Transform.ListTransforms(cmd.Context(), connect.NewRequest(&dataplanev1alpha1.ListTransformsRequest{}))
				out.MaybeDie(err, "unable to list transforms from Cloud: %v", err)
				l = dataplaneToAdminTransformMetadata(res.Msg.Transforms)
			} else {
				api, err := adminapi.NewClient(cmd.Context(), fs, p)
				out.MaybeDie(err, "unable to initialize admin api client: %v", err)

				l, err = api.ListWasmTransforms(cmd.Context())
				out.MaybeDie(err, "unable to list transforms: %v", err)
			}

			if detailed {
				d := detailView(l)
				printDetailed(f, d, os.Stdout)
			} else {
				s := summarizedView(l)
				printSummary(f, s, os.Stdout)
			}
		},
	}
	p.InstallFormatFlag(cmd)
	cmd.Flags().BoolVarP(&detailed, "detailed", "d", false, "Print per-partition information for data transforms")
	return cmd
}

func makeEnvMap(env []rpadmin.EnvironmentVariable) map[string]string {
	out := make(map[string]string)
	for _, entry := range env {
		out[entry.Key] = entry.Value
	}
	return out
}

func summarizedView(metadata []rpadmin.TransformMetadata) (resp []summarizedTransformMetadata) {
	resp = make([]summarizedTransformMetadata, 0, len(metadata))
	for _, meta := range metadata {
		total := len(meta.Status)
		running := 0
		lag := 0
		for _, v := range meta.Status {
			if strings.ToLower(v.Status) == "running" {
				running++
			}
			lag += v.Lag
		}
		resp = append(resp, summarizedTransformMetadata{
			Name:            meta.Name,
			InputTopic:      meta.InputTopic,
			OutputTopics:    meta.OutputTopics,
			Environment:     makeEnvMap(meta.Environment),
			Running:         fmt.Sprintf("%d / %d", running, total),
			Lag:             lag,
			CompressionMode: meta.CompressionMode,
		})
	}
	return
}

func printSummary(f config.OutFormatter, s []summarizedTransformMetadata, w io.Writer) {
	if isText, _, t, err := f.Format(s); !isText {
		out.MaybeDie(err, "unable to print in the requested format %q: %v", f.Kind, err)
		fmt.Fprintf(w, "%s\n", t)
		return
	}
	tw := out.NewTableTo(w, "Name", "Input Topic", "Output Topic", "Running", "Lag")
	defer tw.Flush()
	for _, m := range s {
		tw.Print(m.Name, m.InputTopic, strings.Join(m.OutputTopics, ", "), m.Running, m.Lag)
	}
}

func detailView(metadata []rpadmin.TransformMetadata) (resp []detailedTransformMetadata) {
	resp = make([]detailedTransformMetadata, 0, len(metadata))
	for _, meta := range metadata {
		resp = append(resp, detailedTransformMetadata{
			Name:            meta.Name,
			InputTopic:      meta.InputTopic,
			OutputTopics:    meta.OutputTopics,
			Environment:     makeEnvMap(meta.Environment),
			Status:          meta.Status,
			CompressionMode: meta.CompressionMode,
		})
	}
	return
}

func printDetailed(f config.OutFormatter, d []detailedTransformMetadata, w io.Writer) {
	if isText, _, s, err := f.Format(d); !isText {
		out.MaybeDie(err, "unable to print in the requested format %q: %v", f.Kind, err)
		fmt.Fprintf(w, "%s\n", s)
		return
	}
	tw := out.NewTabWriterTo(w)
	defer tw.Flush()
	for i, m := range d {
		if i > 0 {
			tw.Line()
		}
		tw.Print(fmt.Sprintf("%s, %s → %s", m.Name, m.InputTopic, strings.Join(m.OutputTopics, ", ")))
		// add an empty column to provide an indent
		tw.Print("", "PARTITION", "NODE", "STATUS", "LAG")
		for _, p := range m.Status {
			tw.Print("", p.Partition, p.NodeID, p.Status, p.Lag)
		}
	}
}

func dataplaneToAdminTransformMetadata(transforms []*dataplanev1alpha1.TransformMetadata) []rpadmin.TransformMetadata {
	var transformMetadata []rpadmin.TransformMetadata
	for _, t := range transforms {
		var (
			status []rpadmin.PartitionTransformStatus
			envs   []rpadmin.EnvironmentVariable
		)
		if t != nil {
			for _, s := range t.Statuses {
				if s != nil {
					status = append(status, rpadmin.PartitionTransformStatus{
						NodeID:    int(s.BrokerId),
						Partition: int(s.PartitionId),
						Status:    strings.TrimPrefix(s.Status.String(), "PARTITION_STATUS_"),
						Lag:       int(s.Lag),
					})
				}
			}
			for _, e := range t.EnvironmentVariables {
				if e != nil {
					envs = append(envs, rpadmin.EnvironmentVariable{
						Key:   e.Key,
						Value: e.Value,
					})
				}
			}
			transformMetadata = append(transformMetadata, rpadmin.TransformMetadata{
				Name:         t.Name,
				InputTopic:   t.InputTopicName,
				OutputTopics: t.OutputTopicNames,
				Status:       status,
				Environment:  envs,
			})
		}
	}
	return transformMetadata
}
