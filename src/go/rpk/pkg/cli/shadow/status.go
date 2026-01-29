// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package shadow

import (
	"fmt"
	"sort"
	"strings"

	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	dataplanev1 "buf.build/gen/go/redpandadata/dataplane/protocolbuffers/go/redpanda/api/dataplane/v1"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type slStatusOptions struct {
	all      bool
	overview bool
	task     bool
	topic    bool
}

// Helper types for unified printing across AdminAPI and Dataplane API.
type linkOverview struct {
	Name  string `json:"name" yaml:"name"`
	ID    string `json:"id" yaml:"id"`
	State string `json:"state" yaml:"state"`
}

type taskStatus struct {
	Name     string `json:"name" yaml:"name"`
	BrokerID int32  `json:"broker_id" yaml:"broker_id"`
	Shard    int32  `json:"shard" yaml:"shard"`
	State    string `json:"state" yaml:"state"`
	Reason   string `json:"reason" yaml:"reason"`
}

type topicStatus struct {
	Name       string          `json:"name" yaml:"name"`
	ID         string          `json:"id,omitempty" yaml:"id,omitempty"`
	State      string          `json:"state" yaml:"state"`
	Partitions []partitionInfo `json:"partitions" yaml:"partitions"`
}

type partitionInfo struct {
	PartitionID int64 `json:"partition_id" yaml:"partition_id"`
	SrcLSO      int64 `json:"src_lso" yaml:"src_lso"`
	SrcHWM      int64 `json:"src_hwm" yaml:"src_hwm"`
	DstHWM      int64 `json:"dst_hwm" yaml:"dst_hwm"`
	Lag         int64 `json:"lag" yaml:"lag"`
}

type shadowLinkStatus struct {
	Overview                    linkOverview  `json:"overview" yaml:"overview"`
	Tasks                       []taskStatus  `json:"tasks" yaml:"tasks"`
	Topics                      []topicStatus `json:"topics" yaml:"topics"`
	SyncedShadowTopicProperties []string      `json:"synced_shadow_topic_properties" yaml:"synced_shadow_topic_properties"`
}

func newStatusCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var opts slStatusOptions
	cmd := &cobra.Command{
		Use:   "status [LINK_NAME]",
		Args:  cobra.ExactArgs(1),
		Short: "Get the status of a Redpanda Shadow Link",
		Long: `Get the status of a Redpanda Shadow Link.

This command shows the current status of a Shadow Link, including the overall
state, task statuses, and per-topic replication progress. Use this command to
monitor replication health and track how closely shadow topics follow the source
cluster.

By default, the command displays all status sections. Use the flags to display
specific sections such as overview, task status, or topic status. When using
this command with the --format json/yaml flag, we default to all sections.
`,
		Example: `
Display the status of a Shadow Link:
  rpk shadow status my-shadow-link

Display specific sections:
  rpk shadow status my-shadow-link --print-overview --print-topic
`,
		Run: func(cmd *cobra.Command, args []string) {
			f := p.Formatter
			if h, ok := f.Help(shadowLinkStatus{}); ok {
				out.Exit(h)
			}
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load rpk config: %v", err)
			prof := cfg.VirtualProfile()
			config.CheckExitServerlessAdmin(prof)

			linkName := args[0]
			opts.defaultOrAll()
			var statusResponse shadowLinkStatus
			if prof.CheckFromCloud() {
				cl, err := publicapi.DataplaneClientFromRpkProfile(prof)
				out.MaybeDie(err, "unable to initialize cloud API client: %v", err)

				link, err := cl.ShadowLink.GetShadowLink(cmd.Context(), connect.NewRequest(&dataplanev1.GetShadowLinkRequest{
					Name: linkName,
				}))
				out.MaybeDie(err, "unable to get shadow link: %v", err)

				topicList, err := cl.ListAllShadowLinkTopics(cmd.Context(), linkName)
				out.MaybeDie(err, "unable to list shadow link topics: %v", err)

				statusResponse = fromDataplaneShadowLink(link.Msg.GetShadowLink(), topicList)
			} else {
				cl, err := adminapi.NewClient(cmd.Context(), fs, prof)
				out.MaybeDie(err, "unable to initialize admin client: %v", err)

				link, err := cl.ShadowLinkService().GetShadowLink(cmd.Context(), connect.NewRequest(&adminv2.GetShadowLinkRequest{
					Name: linkName,
				}))
				out.MaybeDie(err, "unable to get Redpanda Shadow Link status %q: %v", linkName, handleConnectError(err, "get", linkName))
				statusResponse = fromAdminV2ShadowLink(link.Msg.GetShadowLink())
			}
			if isText, _, s, err := f.Format(statusResponse); !isText {
				out.MaybeDie(err, "unable to print in the required format %q: %v", f.Kind, err)
				fmt.Println(s)
				return
			}
			printShadowLinkStatus(statusResponse, opts)
		},
	}
	cmd.Flags().BoolVarP(&opts.overview, "print-overview", "o", false, "Print the overview section")
	cmd.Flags().BoolVarP(&opts.task, "print-task", "k", false, "Print the task status section")
	cmd.Flags().BoolVarP(&opts.topic, "print-topic", "t", false, "Print the detailed topic status section")
	cmd.Flags().BoolVarP(&opts.all, "print-all", "a", false, "Print all sections")

	p.InstallFormatFlag(cmd)
	return cmd
}

// If no flags are set, default to overview and client sections.
func (o *slStatusOptions) defaultOrAll() {
	// We currently default to all sections until we have more fields to show.
	if o.all || (!o.overview && !o.task && !o.topic) {
		o.overview, o.task, o.topic = true, true, true
	}
}

func printShadowLinkStatus(status shadowLinkStatus, opts slStatusOptions) {
	const (
		secOverview = "Overview"
		secTasks    = "Tasks"
		secTopics   = "Topics"
	)

	sections := out.NewSections(
		out.ConditionalSectionHeaders(map[string]bool{
			secOverview: opts.overview,
			secTasks:    opts.task,
			secTopics:   opts.topic,
		})...,
	)

	sections.Add(secOverview, func() {
		printStatusOverview(status.Overview)
	})

	sections.Add(secTasks, func() {
		printStatusTasks(status.Tasks)
	})

	sections.Add(secTopics, func() {
		printStatusTopics(status.Topics, status.SyncedShadowTopicProperties)
	})
}

func printStatusOverview(overview linkOverview) {
	tw := out.NewTabWriter()
	defer tw.Flush()
	tw.Print("NAME", overview.Name)
	tw.Print("UID", overview.ID)
	tw.Print("STATE", overview.State)
}

func printStatusTasks(tasks []taskStatus) {
	if len(tasks) == 0 {
		fmt.Println("No tasks to display right now.")
		return
	}
	t := out.NewTable("Name", "Broker_ID", "Shard", "State", "Reason")
	defer t.Flush()
	for _, task := range tasks {
		t.Print(task.Name, task.BrokerID, task.Shard, task.State, task.Reason)
	}
}

func printStatusTopics(topics []topicStatus, props []string) {
	if len(topics) == 0 {
		fmt.Println("No topics are being shadowed.")
		return
	}
	for _, topic := range topics {
		var topicID string
		if topic.ID != "" {
			topicID = fmt.Sprintf(", ID: %v", topic.ID)
		}
		fmt.Printf("Name: %s%s, State: %s\n", topic.Name, topicID, topic.State)
		printStatusPartitionTable(topic.Partitions)
		fmt.Println()
	}
	if len(props) > 0 {
		fmt.Println("Synced Shadow Topic Properties:")
		for _, prop := range props {
			fmt.Printf("  - %s\n", prop)
		}
	}
}

func printStatusPartitionTable(partitions []partitionInfo) {
	t := out.NewTable("", "Partition", "SRC_LSO", "SRC_HWM", "DST_HWM", "Lag")
	defer t.Flush()
	for _, p := range partitions {
		t.Print("", p.PartitionID, p.SrcLSO, p.SrcHWM, p.DstHWM, p.Lag)
	}
}

// fromAdminV2ShadowLink converts an adminv2.ShadowLink to the unified shadowLinkStatus.
func fromAdminV2ShadowLink(link *adminv2.ShadowLink) shadowLinkStatus {
	status := link.GetStatus()
	var state string
	if status != nil {
		state = strings.TrimPrefix(status.GetState().String(), "SHADOW_LINK_STATE_")
	}

	result := shadowLinkStatus{
		Overview: linkOverview{
			Name:  link.GetName(),
			ID:    link.GetUid(),
			State: state,
		},
		SyncedShadowTopicProperties: status.GetSyncedShadowTopicProperties(),
	}

	// Convert tasks
	for _, task := range status.GetTaskStatuses() {
		result.Tasks = append(result.Tasks, taskStatus{
			Name:     task.GetName(),
			BrokerID: task.GetBrokerId(),
			Shard:    task.GetShard(),
			State:    strings.TrimPrefix(task.GetState().String(), "TASK_STATE_"),
			Reason:   task.GetReason(),
		})
	}

	shadowTopics := status.GetShadowTopics()
	sort.Slice(shadowTopics, func(i, j int) bool {
		return shadowTopics[i].GetName() < shadowTopics[j].GetName()
	})
	// Convert topics
	for _, topic := range shadowTopics {
		ts := topicStatus{
			Name:  topic.GetName(),
			ID:    topic.GetTopicId(),
			State: strings.TrimPrefix(topic.GetStatus().GetState().String(), "SHADOW_TOPIC_STATE_"),
		}
		pi := topic.GetStatus().GetPartitionInformation()
		sort.SliceStable(pi, func(i, j int) bool {
			return pi[i].GetPartitionId() < pi[j].GetPartitionId()
		})
		for _, p := range pi {
			ts.Partitions = append(ts.Partitions, partitionInfo{
				PartitionID: p.GetPartitionId(),
				SrcLSO:      p.GetSourceLastStableOffset(),
				SrcHWM:      p.GetSourceHighWatermark(),
				DstHWM:      p.GetHighWatermark(),
				Lag:         p.GetSourceHighWatermark() - p.GetHighWatermark(),
			})
		}
		result.Topics = append(result.Topics, ts)
	}

	return result
}

// fromDataplaneShadowLink converts dataplane API responses to the unified shadowLinkStatus.
func fromDataplaneShadowLink(link *dataplanev1.ShadowLink, topicDetails []*dataplanev1.ShadowTopic) shadowLinkStatus {
	result := shadowLinkStatus{
		Overview: linkOverview{
			Name:  link.GetName(),
			ID:    link.GetUid(),
			State: strings.TrimPrefix(link.GetState().String(), "SHADOW_LINK_STATE_"),
		},
		SyncedShadowTopicProperties: link.GetSyncedShadowTopicProperties(),
	}

	// Convert tasks
	for _, task := range link.GetTasksStatus() {
		result.Tasks = append(result.Tasks, taskStatus{
			Name:     task.GetName(),
			BrokerID: task.GetBrokerId(),
			Shard:    task.GetShard(),
			State:    strings.TrimPrefix(task.GetState().String(), "TASK_STATE_"),
			Reason:   task.GetReason(),
		})
	}

	sort.Slice(topicDetails, func(i, j int) bool {
		return topicDetails[i].GetTopicName() < topicDetails[j].GetTopicName()
	})
	// Convert topics from the detailed topic responses
	for _, topicResp := range topicDetails {
		ts := topicStatus{
			Name:  topicResp.GetTopicName(),
			ID:    topicResp.GetTopicId(),
			State: strings.TrimPrefix(topicResp.GetState().String(), "SHADOW_TOPIC_STATE_"),
		}
		pi := topicResp.GetPartitions()
		sort.SliceStable(pi, func(i, j int) bool {
			return pi[i].GetPartitionId() < pi[j].GetPartitionId()
		})
		for _, p := range pi {
			ts.Partitions = append(ts.Partitions, partitionInfo{
				PartitionID: p.GetPartitionId(),
				SrcLSO:      p.GetSourceLastStableOffset(),
				SrcHWM:      p.GetSourceHighWatermark(),
				DstHWM:      p.GetHighWatermark(),
				Lag:         p.GetSourceHighWatermark() - p.GetHighWatermark(),
			})
		}
		result.Topics = append(result.Topics, ts)
	}

	return result
}
