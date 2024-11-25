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
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
)

func newTrimPrefixCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		fromFile   string
		noConfirm  bool
		offset     string
		partitions []int32
	)
	cmd := &cobra.Command{
		Use:     "trim-prefix [TOPIC]",
		Aliases: []string{"trim"},
		Args:    cobra.MaximumNArgs(1),
		Short:   "Trim records from topics",
		Long: `Trim records from topics

This command allows you to trim records from topics, to trim the topics Redpanda
sets the LogStartOffset for partitions to the requested offset. All segments
whose base offset is less then the requested offset are deleted, and any records
within the segment before the requested offset can no longer be read.

The --offset/-o flag allows you to indicate which index you want to set the
partition's low watermark (start offset) to. It can be a single integer value
denoting the offset or a timestamp if you prefix the offset with an '@'. You may
select which partition you want to trim the offset from with the --partitions/-p
flag.

The --from-file option allows to trim the offsets specified in a text file with
the following format:
    [TOPIC] [PARTITION] [OFFSET]
    [TOPIC] [PARTITION] [OFFSET]
    ...
or the equivalent keyed JSON/YAML file.

EXAMPLES

Trim records in 'foo' topic to offset 120 in partition 1
    rpk topic trim-prefix foo --offset 120 --partitions 1

Trim records in all partitions of topic foo previous to an specific timestamp
    rpk topic trim-prefix foo -o "@1622505600"

Trim records from a JSON file
    rpk topic trim-prefix --from-file /tmp/to_trim.json
`,
		Run: func(cmd *cobra.Command, args []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			var o kadm.Offsets
			if fromFile != "" {
				o, err = parseOffsetFile(fs, fromFile, args)
				out.MaybeDie(err, "unable to parse file %q: %v", fromFile, err)
				if len(o) == 0 {
					out.Die("offset file is empty")
				}
			} else {
				if len(args) == 0 {
					out.Die("Error: required arg 'topic' not set\n%v", cmd.UsageString())
				}
				if offset == "" {
					out.Die("Error: required flag 'offset' not set\n%v", cmd.UsageString())
				}
				topic := args[0]
				o, err = parseOffsetArgs(cmd.Context(), adm, topic, offset, partitions)
				out.MaybeDieErr(err)
				if len(o) == 0 {
					out.Die("topic %q does not exists", topic)
				}
			}
			if !noConfirm {
				err := printDeleteRecordRequest(cmd.Context(), adm, o)
				out.MaybeDie(err, "unable to print trimming request: %v", err)
				fmt.Println()
				confirmed, err := out.Confirm("Confirm deletion of all data before the new start offsets?")
				out.MaybeDie(err, "unable to confirm: %v", err)
				if !confirmed {
					out.Exit("Trimming canceled.")
				}
				fmt.Println()
			}
			drr, err := adm.DeleteRecords(cmd.Context(), o)
			out.MaybeDie(err, "unable to trim records: %v", err)
			ok := printDeleteRecordResponse(drr)
			if !ok { // This means that at least 1 row contained an error.
				os.Exit(1)
			}
		},
	}

	cmd.Flags().StringVarP(&offset, "offset", "o", "", "Offset to set the partition's start offset to (end, 47, @<timestamp>)")
	cmd.Flags().Int32SliceVarP(&partitions, "partitions", "p", nil, "Comma-separated list of partitions to trim records from (default to all)")
	cmd.Flags().StringVarP(&fromFile, "from-file", "f", "", "File of topic/partition/offset for which to trim offsets for")
	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "Disable confirmation prompt")

	cmd.MarkFlagsMutuallyExclusive("from-file", "offset")
	cmd.MarkFlagsMutuallyExclusive("from-file", "partitions")

	return cmd
}

// parseOffsetArgs creates the kadm.Offsets based on the passed args via rpk
// flags. It checks if the topic exists and defaults to all partitions in the
// case that partitions were not provided.
func parseOffsetArgs(ctx context.Context, adm *kadm.Client, topic, offset string, partitions []int32) (kadm.Offsets, error) {
	var (
		listedOffsets kadm.ListedOffsets
		parsedOffset  int64
		err           error
	)
	switch {
	case strings.HasPrefix(offset, "@"):
		offset = offset[1:] // strip @
		_, startAt, end, _, err := parseTimestampBasedOffset(offset, time.Now())
		if err != nil {
			return nil, err
		} else if end {
			return nil, errors.New("invalid offset '@end'; use '--offset end' if you want to trim all")
		}
		listedOffsets, err = adm.ListOffsetsAfterMilli(ctx, startAt.UnixMilli(), topic)
		if err == nil {
			err = listedOffsets.Error()
		}
		if err != nil {
			return nil, fmt.Errorf("unable to list offsets after milli %d: %v", startAt.UnixMilli(), err)
		}
	case offset == "end":
		listedOffsets, err = adm.ListEndOffsets(ctx, topic)
		if err == nil {
			err = listedOffsets.Error()
		}
		if err != nil {
			return nil, fmt.Errorf("unable to list end offset for topic %q: %v", topic, err)
		}
	default:
		parsedOffset, err = strconv.ParseInt(offset, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing offset %q: %v", offset, err)
		}
		// If no partitions are provided, we list the topic and use every
		// partition.
		if len(partitions) == 0 {
			topicDetails, err := adm.ListTopics(ctx, topic)
			if err == nil {
				err = topicDetails.Error()
			}
			if err != nil {
				return nil, fmt.Errorf("unable to list topic %q metadata: %v", topic, err)
			}
			topicDetails.EachError(func(d kadm.TopicDetail) {
				err = fmt.Errorf("unable to list the topic %q: %v", topic, d.Err)
			})
			if err != nil {
				return nil, err
			}
			topicDetails.EachPartition(func(pd kadm.PartitionDetail) {
				partitions = append(partitions, pd.Partition)
			})
		}
	}
	// Same logic as above, but in this case we use the listed offset.
	if len(partitions) == 0 {
		listedOffsets.Each(func(lo kadm.ListedOffset) {
			partitions = append(partitions, lo.Partition)
		})
	}
	o := make(kadm.Offsets)
	for _, p := range partitions {
		if listedOffsets != nil {
			l, ok := listedOffsets.Lookup(topic, p)
			if !ok {
				return nil, fmt.Errorf("unable to find offset %q for topic %q in partition %v: %v", offset, topic, p, err)
			}
			if l.Err != nil {
				return nil, l.Err
			}
			parsedOffset = l.Offset
		}
		o.Add(kadm.Offset{
			Topic:     topic,
			Partition: p,
			At:        parsedOffset,
		})
	}
	return o, nil
}

// parseOffsetFile parse the given file whether it's a json, yaml or
// space-delimited text. File content: topic | partition | offset.
func parseOffsetFile(fs afero.Fs, file string, args []string) (kadm.Offsets, error) {
	parsed, err := out.ParseFileArray[struct {
		Topic     string `yaml:"topic" json:"topic"`
		Partition int32  `yaml:"partition" json:"partition"`
		Offset    int64  `yaml:"offset" json:"offset"`
	}](fs, file)
	if err != nil {
		// If we fail to parse a 3-column file, we attempt to parse as 2-column.
		parsed, err := out.ParseFileArray[struct {
			Partition int32 `yaml:"partition" json:"partition"`
			Offset    int64 `yaml:"offset" json:"offset"`
		}](fs, file)
		o := make(kadm.Offsets)
		if err != nil {
			return nil, err
		}
		if len(args) == 0 {
			return nil, fmt.Errorf("unable to get the topic; please provide it via arguments to this command or in the file provided")
		}
		for _, element := range parsed {
			o.Add(kadm.Offset{
				Topic:     args[0],
				Partition: element.Partition,
				At:        element.Offset,
			})
		}
		return o, nil
	}
	o := make(kadm.Offsets)
	for _, element := range parsed {
		if len(args) > 0 && args[0] != element.Topic {
			return nil, fmt.Errorf("unable to parse the topic; the file contains the topic %q that is different of the argument provided %q", element.Topic, args[0])
		}
		o.Add(kadm.Offset{
			Topic:     element.Topic,
			Partition: element.Partition,
			At:        element.Offset,
		})
	}
	return o, nil
}

// printDeleteRecordRequest prints the passed kadm.Offsets (used for the
// deleteRecords request). It finds the start offset so the user can see what's
// the change that is being done, it also finds the hwm for when the user passes
// the '--offset end' flag.
func printDeleteRecordRequest(ctx context.Context, adm *kadm.Client, o kadm.Offsets) (rerr error) {
	// We get the start Offset for every topic.
	listStartOffsets, err := adm.ListStartOffsets(ctx, o.TopicsSet().Topics()...)
	if err == nil {
		err = listStartOffsets.Error()
	}
	if err != nil {
		return fmt.Errorf("unable to list start offset for the topic(s) %v: %v", o.TopicsSet().Topics(), err)
	}
	tw := out.NewTable("TOPIC", "PARTITION", "PRIOR-START-OFFSET", "NEW-START-OFFSET")
	o.Each(func(offset kadm.Offset) {
		lso, ok := listStartOffsets.Lookup(offset.Topic, offset.Partition)
		if !ok {
			rerr = fmt.Errorf("unable to find the start offset for the topic %q and partition %v", offset.Topic, offset.Partition)
			return
		}
		tw.Print(offset.Topic, offset.Partition, lso.Offset, offset.At)
	})
	if rerr == nil {
		tw.Flush()
	}
	return rerr
}

func printDeleteRecordResponse(drr kadm.DeleteRecordsResponses) (ok bool) {
	tw := out.NewTable("TOPIC", "PARTITION", "NEW-START-OFFSET", "ERROR")
	defer tw.Flush()
	ok = true
	drr.Each(func(r kadm.DeleteRecordsResponse) {
		if r.Err != nil {
			tw.Print(r.Topic, r.Partition, "-", r.Err)
			ok = false
		} else {
			tw.Print(r.Topic, r.Partition, r.LowWatermark, "")
		}
	})
	return
}
