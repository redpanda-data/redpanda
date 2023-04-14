// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package group

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

func newSeekCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		to             string
		toGroup        string
		toFile         string
		topics         []string
		allowNewTopics bool
	)

	cmd := &cobra.Command{
		Use:   "seek [GROUP] --to (start|end|timestamp) --to-group ... --topics ...",
		Short: "Modify a group's current offsets",
		Long: `Modify a group's current offsets.

This command allows you to modify a group's offsets. Sometimes, you may need to
rewind a group if you had a mistaken deploy, or fast-forward a group if it is
falling behind on messages that can be skipped.

The --to option allows you to seek to the start of partitions, end of
partitions, or after a specific timestamp. The default is to seek any topic
previously committed. Using --topics allows to you set commits for only the
specified topics; all other commits will remain untouched. Topics with no
commits will not be committed unless allowed with --allow-new-topics.

The --to-group option allows you to seek to commits that are in another group.
This is a merging operation: if g1 is consuming topics A and B, and g2 is
consuming only topic B, "rpk group seek g1 --to-group g2" will update g1's
commits for topic B only. The --topics flag can be used to further narrow which
topics are updated. Unlike --to, all non-filtered topics are committed, even
topics not yet being consumed, meaning --allow-new-topics is not needed.

The --to-file option allows to seek to offsets specified in a text file with
the following format:
    [TOPIC] [PARTITION] [OFFSET]
    [TOPIC] [PARTITION] [OFFSET]
    ...
Each line contains the topic, the partition, and the offset to seek to. As with
the prior options, --topics allows filtering which topics are updated. Similar
to --to-group, all non-filtered topics are committed, even topics not yet being
consumed, meaning --allow-new-topics is not needed.

The --to, --to-group, and --to-file options are mutually exclusive. If you are
not authorized to describe or read some topics used in a group, you will not be
able to modify offsets for those topics.

EXAMPLES

Seek group G to June 1st, 2021:
    rpk group seek g --to 1622505600
    or, rpk group seek g --to 1622505600000
    or, rpk group seek g --to 1622505600000000000
Seek group X to the commits of group Y topic foo:
    rpk group seek X --to-group Y --topics foo
Seek group G's topics foo, bar, and biz to the end:
    rpk group seek G --to end --topics foo,bar,biz
Seek group G to the beginning of a topic it was not previously consuming:
    rpk group seek G --to start --topics foo --allow-new-topics
`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p, cfg)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			var n int
			for _, f := range []string{to, toGroup, toFile} {
				if f != "" {
					n++
				}
			}
			switch {
			case n == 0:
				out.Die("Must specify one --to flag.")
			case n == 1:
			default:
				out.Die("Cannot specify multiple --to flags.")
			}

			tset := make(map[string]bool)
			for _, topic := range topics {
				tset[topic] = true
			}

			group := args[0]

			seek(fs, adm, group, to, toGroup, toFile, tset, allowNewTopics)
		},
	}

	cmd.Flags().StringVar(&to, "to", "", "Where to seek (start, end, unix second | millisecond | nanosecond)")
	cmd.Flags().StringVar(&toGroup, "to-group", "", "Seek to the commits of another group")
	cmd.Flags().StringVar(&toFile, "to-file", "", "Seek to offsets as specified in the file")
	cmd.Flags().StringSliceVar(&topics, "topics", nil, "Only seek these topics, if any are specified")
	cmd.Flags().BoolVar(&allowNewTopics, "allow-new-topics", false, "Allow seeking to new topics not currently consumed (implied with --to-group or --to-file)")

	return cmd
}

func parseSeekFile(
	fs afero.Fs, file string, topics map[string]bool,
) (kadm.Offsets, error) {
	f, err := fs.Open(file)
	if err != nil {
		return nil, fmt.Errorf("unable to open %q: %v", file, err)
	}

	o := make(kadm.Offsets)
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := s.Text()
		if len(line) == 0 {
			continue
		}
		fields := strings.Split(line, " ")
		if len(fields) == 1 {
			fields = strings.Split(line, "\t")
		}
		if len(fields) != 3 {
			return nil, fmt.Errorf("unable to split line %q by space or tab into three fields", line)
		}

		topic := fields[0]
		partition, err := strconv.ParseInt(fields[1], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("unable to parse partition on line %q: %v", line, err)
		}
		offset, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("unable to parse offset on line %q: %v", line, err)
		}

		o.Add(kadm.Offset{
			Topic:       topic,
			Partition:   int32(partition),
			At:          offset,
			LeaderEpoch: -1,
		})
	}
	if err := s.Err(); err != nil {
		return nil, fmt.Errorf("file %q scan error: %v", file, err)
	}

	// If we have a filter, keep only what is in the filter.
	if len(topics) > 0 {
		o.KeepFunc(func(o kadm.Offset) bool { return topics[o.Topic] })
	}

	return o, nil
}

func seekFetch(
	adm *kadm.Client, group string, topics map[string]bool,
) kadm.Offsets {
	resps, err := adm.FetchOffsets(context.Background(), group)
	if err == nil {
		err = resps.Error()
	}
	out.MaybeDie(err, "unable to fetch offsets for %q: %v", group, err)
	// If we have a filter, keep only what is in the filter.
	if len(topics) > 0 {
		resps.KeepFunc(func(o kadm.OffsetResponse) bool { return topics[o.Topic] })
	}
	return resps.Offsets()
}

func seek(
	fs afero.Fs,
	adm *kadm.Client,
	group string,
	to string,
	toGroup string,
	toFile string,
	topics map[string]bool,
	allowNewTopics bool,
) {
	current := seekFetch(adm, group, topics)
	var commitTo kadm.Offsets
	if toFile != "" {
		var err error
		commitTo, err = parseSeekFile(fs, toFile, topics)
		out.MaybeDieErr(err)
	} else if toGroup != "" {
		commitTo = seekFetch(adm, toGroup, topics)
	} else { // --to, we need to list offsets currently used, as well as any extra
		tps := current.TopicsSet()
		for topic := range topics {
			if _, exists := tps[topic]; !exists && !allowNewTopics {
				out.Die("Cannot commit new topic %q without --allow-new-topics.", topic)
			}
			tps[topic] = map[int32]struct{}{} // ensure exists
		}
		topics := tps.Topics()
		var listed kadm.ListedOffsets
		if len(topics) > 0 {
			var err error
			switch to {
			case "start":
				listed, err = adm.ListStartOffsets(context.Background(), topics...)
			case "end":
				listed, err = adm.ListEndOffsets(context.Background(), topics...)
			default:
				var milli int64
				milli, err = strconv.ParseInt(to, 10, 64)
				out.MaybeDie(err, "unable to parse millisecond %q: %v", to, err)
				switch len(to) {
				case 10: // e.g. "1622505600"; sec to milli
					milli *= 1000
				case 13: // e.g. "1622505600000", already in milli
				case 19: // e.g. "1622505600000000000"; nano to milli
					milli /= 1e6
				default:
					out.Die("--to timestamp %q is not a second, nor a millisecond, nor a nanosecond", to)
				}
				listed, err = adm.ListOffsetsAfterMilli(context.Background(), milli, topics...)
			}
			if err == nil { // ListOffsets can return ShardErrors, but we want to be entirely successful
				err = listed.Error()
			}
			out.MaybeDie(err, "unable to list all offsets successfully: %v", err)
		}
		commitTo = listed.Offsets()
	}

	// Finally, we commit.
	committed, err := adm.CommitOffsets(context.Background(), group, commitTo)
	out.MaybeDie(err, "unable to commit offsets: %v", err)

	useErr := committed.Error() != nil
	headers := []string{"topic", "partition", "prior-offset", "current-offset"}
	if useErr {
		headers = append(headers, "error")
	}
	tw := out.NewTable(headers...)
	defer tw.Flush()
	for _, c := range committed.Sorted() {
		s := seekCommit{c.Topic, c.Partition, -1, -1}
		if o, exists := current.Lookup(c.Topic, c.Partition); exists {
			s.Prior = o.At
		}
		if o, exists := commitTo.Lookup(c.Topic, c.Partition); exists {
			s.Current = o.At
		}
		se := seekCommitErr{c.Topic, c.Partition, -1, -1, ""}
		if c.Err != nil {
			// Redpanda / Kafka send UnknownMemberID when issuing OffsetCommit
			// if the group is not empty. This error is unclear to end users, so
			// we remap it here.
			if errors.Is(c.Err, kerr.UnknownMemberID) {
				se.Error = "INVALID_OPERATION: seeking a non-empty group is not allowed."
			} else {
				se.Error = c.Err.Error()
			}
		}
		if useErr {
			tw.PrintStructFields(se)
		} else {
			tw.PrintStructFields(s)
		}
	}
}

type seekCommit struct {
	Topic     string
	Partition int32
	Prior     int64
	Current   int64
}

type seekCommitErr struct {
	Topic     string
	Partition int32
	Prior     int64
	Current   int64
	Error     string
}
