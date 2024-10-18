// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package topic

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/redpanda-data/common-go/rpadmin"

	"github.com/docker/go-units"
	"github.com/hashicorp/go-multierror"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
)

type Status struct {
	Partition   int32
	CloudStatus rpadmin.CloudStorageStatus
}

const (
	humanSize = "size"
	humanTime = "time"
)

func newDescribeStorageCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		all     bool
		human   bool
		offset  bool
		size    bool
		summary bool
		syncF   bool // Named syncF (flag) to avoid collision with sync package.
	)
	cmd := &cobra.Command{
		Use:   "describe-storage [TOPIC]",
		Short: "Describe the topic storage status",
		Long:  helpDescribeStorage,
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			cl, err := kafka.NewAdmin(fs, p)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer cl.Close()

			adminCl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			topic := args[0]
			listed, err := cl.ListTopicsWithInternal(cmd.Context(), topic)
			out.MaybeDie(err, "unable to list topic %q metadata: %v", topic, err)
			listed.EachError(func(d kadm.TopicDetail) {
				out.Die("unable to discover the partitions on topic %q: %v", d.Topic, d.Err)
			})
			topicMeta, ok := listed[topic]
			if !ok || topicMeta.Err != nil {
				out.Die("error requesting topic metadata for %q: %v", topic, err)
			}

			// Get the cloud storage status concurrently for each partition.
			var (
				report []Status
				grp    multierror.Group
				mu     sync.Mutex
			)
			for _, pd := range topicMeta.Partitions {
				pd := pd
				grp.Go(func() error {
					st, err := adminCl.CloudStorageStatus(context.Background(), topic, strconv.Itoa(int(pd.Partition)))
					if err != nil {
						return fmt.Errorf("unable to request cloud storage status of topic %q, partition %v. make sure you have tiered storage to use this command: %v", topic, pd.Partition, err)
					}
					mu.Lock()
					report = append(report, Status{pd.Partition, st})
					mu.Unlock()
					return nil
				})
			}
			if err := grp.Wait(); err != nil {
				out.Die(err.Error())
			}
			if len(report) == 0 {
				out.Die("error: could not get any status report from cloud storage.")
			}
			sort.Slice(report, func(i, j int) bool {
				return report[i].Partition < report[j].Partition
			})

			// By default, if neither are specified, we opt in to
			// the size section only
			if !summary && !size && !syncF && !offset {
				summary, size = true, true
			}
			if all {
				summary, size, syncF, offset = true, true, true, true
			}

			const (
				secSummary = "SUMMARY"
				secOffsets = "OFFSETS"
				secSize    = "SIZE"
				secSync    = "SYNC"
			)
			sections := out.NewMaybeHeaderSections(
				out.ConditionalSectionHeaders(map[string]bool{
					secSummary: summary,
					secOffsets: offset,
					secSize:    size,
					secSync:    syncF,
				})...,
			)

			sections.Add(secSummary, func() {
				tw := out.NewTabWriter()
				defer tw.Flush()
				tw.PrintColumn("NAME", topic)
				if topicMeta.IsInternal {
					tw.PrintColumn("INTERNAL", topicMeta.IsInternal)
				}
				tw.PrintColumn("PARTITIONS", len(topicMeta.Partitions))
				if len(topicMeta.Partitions) > 0 {
					p0 := topicMeta.Partitions.Sorted()[0]
					tw.PrintColumn("REPLICAS", len(p0.Replicas))
				}
				// We can safely pick the value of the first partition since
				// the cloud storage mode doesn't change between partitions of
				// the same topic.
				tw.PrintColumn("CLOUD-STORAGE-MODE", report[0].CloudStatus.CloudStorageMode)

				lastUpload := getLastUpload(report)
				tw.PrintColumn("LAST-UPLOAD", humanReadable(lastUpload, human, humanTime))
			})

			sections.Add(secOffsets, func() {
				tw := out.NewTable("PARTITION", "CLOUD-START", "CLOUD-LAST", "LOCAL-START", "LOCAL-LAST")
				defer tw.Flush()
				for _, r := range report {
					tw.Print(
						r.Partition,
						r.CloudStatus.CloudLogStartOffset,
						r.CloudStatus.CloudLogLastOffset,
						r.CloudStatus.LocalLogStartOffset,
						r.CloudStatus.LocalLogLastOffset,
					)
				}
			})

			sections.Add(secSize, func() {
				tw := out.NewTable("PARTITION", "CLOUD-BYTES", "LOCAL-BYTES", "TOTAL-BYTES", "CLOUD-SEGMENTS", "LOCAL-SEGMENTS")
				defer tw.Flush()
				for _, r := range report {
					tw.Print(
						r.Partition,
						humanReadable(r.CloudStatus.CloudLogBytes, human, humanSize),
						humanReadable(r.CloudStatus.LocalLogBytes, human, humanSize),
						humanReadable(r.CloudStatus.TotalLogBytes, human, humanSize),
						r.CloudStatus.CloudLogSegmentCount,
						r.CloudStatus.LocalLogSegmentCount,
					)
				}
			})

			sections.Add(secSync, func() {
				isRrr := report[0].CloudStatus.CloudStorageMode == "read_replica"
				headers := []string{"PARTITION", "LAST-SEGMENT-UPLOAD", "LAST-MANIFEST-UPLOAD", "METADATA-UPDATE-PENDING"}
				if isRrr {
					headers = append(headers, "LAST-MANIFEST-SYNC")
				}
				tw := out.NewTable(headers...)
				defer tw.Flush()
				for _, r := range report {
					row := []any{
						r.Partition,
						humanReadable(r.CloudStatus.MsSinceLastSegmentUpload, human, humanTime),
						humanReadable(r.CloudStatus.MsSinceLastManifestUpload, human, humanTime),
						r.CloudStatus.MetadataUpdatePending,
					}
					// If isRrr is true, MsSinceLastManifestSync should be there, this
					// nil check is possibly redundant, but we want to avoid a
					// nil pointer dereference.
					if isRrr && r.CloudStatus.MsSinceLastManifestSync != nil {
						row = append(row, humanReadable(*r.CloudStatus.MsSinceLastManifestSync, human, humanTime))
					}
					tw.Print(row...)
				}
			})
		},
	}
	p.InstallAdminFlags(cmd)
	p.InstallSASLFlags(cmd)

	cmd.Flags().BoolVarP(&all, "print-all", "a", false, "Print all cloud storage status")
	cmd.Flags().BoolVarP(&summary, "print-summary", "s", false, "Print the summary section")
	cmd.Flags().BoolVarP(&size, "print-size", "z", false, "Print the log size section")
	cmd.Flags().BoolVarP(&syncF, "print-sync", "y", false, "Print the sync section")
	cmd.Flags().BoolVarP(&offset, "print-offset", "o", false, "Print the offset section")
	cmd.Flags().BoolVarP(&human, "human-readable", "H", false, "Print the times and bytes in a human-readable form")

	return cmd
}

func getLastUpload(st []Status) int {
	latest := st[0].CloudStatus.MsSinceLastManifestUpload
	for _, s := range st {
		if s.CloudStatus.MsSinceLastManifestUpload < latest {
			latest = s.CloudStatus.MsSinceLastManifestUpload
		}
		if s.CloudStatus.MsSinceLastSegmentUpload < latest {
			latest = s.CloudStatus.MsSinceLastSegmentUpload
		}
	}
	return latest
}

func humanReadable(v int, h bool, dataType string) any {
	if !h {
		return v
	}
	switch dataType {
	case humanSize:
		return units.HumanSize(float64(v))
	case humanTime:
		return units.HumanDuration(time.Duration(v)*time.Millisecond) + " ago"
	default:
		panic("using human readable with an unsupported type")
	}
}

const helpDescribeStorage = `Describe the topic storage status.

This commands prints detailed information about the cloud storage status of a
given topic, the information is divided in 4 sections:

SUMMARY

The summary section contains general information about the topic, the cloud
storage mode (one of disabled, write_only, read_only, full, and read_replica),
and the delta in milliseconds since the last upload of either the partition
manifest or a segment.

OFFSET

The offset section contains the start and last offsets (inclusive) per
partition of data available in both the cloud and on local disk.

SIZE

The size section contains the total bytes per partition in the cloud and on
local disk, the total size of the log of each partition (excluding cloud and
local overlap), and the number of segments in the cloud and on local disk. The
cloud segment count does not include segments queued for deletion.

SYNC

The sync section contains the state of cloud synchronization: milliseconds
since the last upload of the partition manifest, milliseconds since the last
segment upload, milliseconds since the last manifest sync (for read replicas),
and whether the remote metadata has a pending update to include all uploaded
segments.
`
