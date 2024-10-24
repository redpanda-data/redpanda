/*
* Copyright 2024 Redpanda Data, Inc.
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
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

var (
	minTime = time.Unix(0, 0).UTC()
	maxTime = time.Unix(math.MaxInt64, math.MaxInt64).UTC()
)

func newLogsCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var follow bool
	var head, tail int
	var since, until timequery
	since.time = minTime
	until.time = maxTime
	cmd := &cobra.Command{
		Use:     "logs NAME",
		Aliases: []string{"log"},
		Short:   "View logs for a transform",
		Long: `View logs for a transform.

Data transform's STDOUT and STDERR are captured during runtime and written to 
an internally managed topic _redpanda.transform_logs.
This command outputs logs for a single transform over a period of time and 
printing them to STDOUT. The logs can be printed in various formats.

By default, only logs that have been emitted are displayed.
Use the --follow flag to stream new logs continuously.

FILTERING

The --head and --tail flags are mutually exclusive and limit the number of log
entries from the beginning or end of the range, respectively.

The --since and --until flags define a time range. Use one of both flags to
limit the log output to a desired period of time.

Both flags accept values in the following formats:
    
    now                   the current time, useful for --since=now
    13 digits             parsed as a Unix millisecond
    9 digits              parsed as a Unix second
    YYYY-MM-DD            parsed as a day, UTC
    YYYY-MM-DDTHH:MM:SSZ  parsed as RFC3339, UTC; fractional seconds optional (.MMM)
    -dur                  a negative duration from now
    dur                   a positive duration from now

Durations are parsed simply:

    3ms    three milliseconds
    10s    ten seconds
    9m     nine minutes
    1h     one hour
    1m3ms  one minute and three milliseconds

For example,

    --since=-1h   reads logs within the last hour
    --until=-30m  reads logs prior to 30 minutes ago

The following command reads logs between noon and 1pm on March 12th:

    rpk transform logs my-transform --since=2024-03-12T12:00:00Z --until=2024-03-12T13:00:00Z

FORMATTING

Logs can be displayed in a variety of formats using --format.

The default --format=text prints the log record's body line by line.

When --format=wide is specified, the output includes a prefix that is the
date of the log line and a level for the record. The INFO level corresponds 
to being emitted on the transform's STDOUT, while the WARN level is used
for STDERR.

The --format=json flag emits logs in the JSON encoded version of 
the Open Telemetry LogRecord protocol buffer.`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			f := p.Formatter
			if h, ok := f.Help([]rawLogEvent{}); ok {
				out.Exit(h)
			}

			transformName := args[0]

			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			admin, err := kafka.NewAdmin(fs, p)
			out.MaybeDie(err, "unable to create admin client: %v", err)

			topics, err := admin.ListTopics(cmd.Context(), "_redpanda.transform_logs")
			out.MaybeDie(err, "unable to get logs topic: %v", err)
			if len(topics.TopicsList()) == 0 {
				out.Die("unable to find logs topic - is Redpanda on the right version with Data Transforms enabled?")
			}
			topic := topics.TopicsList()[0]
			partition := computeLogPartition(transformName, topic)

			startOffset := int64(-2) // NewOffset.At treats this as start
			if !since.time.Equal(minTime) {
				offsets, err := admin.ListOffsetsAfterMilli(
					cmd.Context(),
					since.time.UnixMilli(),
					topic.Topic,
				)
				out.MaybeDie(err, "unable to list offsets: %v", err)
				offset, ok := offsets.Lookup(topic.Topic, partition)
				if !ok {
					out.Die("unable to list offset for partition: %d", partition)
				}
				startOffset = offset.Offset
			}
			maxOffset := int64(math.MaxInt64)
			if !follow {
				offsets, err := admin.ListCommittedOffsets(cmd.Context(), topic.Topic)
				out.MaybeDie(err, "unable to list offsets: %v", err)
				offset, ok := offsets.Lookup(topic.Topic, partition)
				if !ok {
					out.Die("unable to list offset for partition: %d", partition)
				}
				// The max offset here is exclusive of the last record, so minus one so that we stop once we hit that last record.
				maxOffset = offset.Offset - 1
				// If we are starting after our max offset we need to short circuit
				if !until.time.Equal(maxTime) {
					untilOffsets, err := admin.ListOffsetsAfterMilli(
						cmd.Context(),
						until.time.UnixMilli(),
						topic.Topic,
					)
					out.MaybeDie(err, "unable to list offsets: %v", err)
					untilOffset, ok := untilOffsets.Lookup(topic.Topic, partition)
					if !ok {
						out.Die("unable to list offset for partition: %d", partition)
					}
					maxOffset = min(maxOffset, untilOffset.Offset)
				}
			}
			zap.L().Sugar().Debugf("reading logs topic with bounds [%v, %v] on partition %d", startOffset, maxOffset, partition)
			if startOffset >= maxOffset {
				return
			}

			consumeStart := map[string]map[int32]kgo.Offset{
				topic.Topic: {
					partition: kgo.NewOffset().At(startOffset),
				},
			}
			client, err := kafka.NewFranzClient(fs, p, kgo.ConsumePartitions(consumeStart))
			out.MaybeDie(err, "unable to create client: %v", err)

			sigs := make(chan os.Signal, 2)
			signal.Notify(sigs, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
			donePrinting := make(chan struct{})
			var output io.Writer = os.Stdout
			// kgo only supports consuming in a single direction per client, the simplest way to get
			// the last N elements is to just read them all, and buffer the last N
			if tail > 0 {
				tw := &tailWriter{w: output, limit: tail}
				defer tw.Close()
				output = tw
			}
			go func() {
				defer close(donePrinting)
				printLogs(
					cmd.Context(),
					&clientLogSource{client},
					logsQuery{
						transformName: transformName,
						maxOffset:     maxOffset,
						limit:         head,
					},
					output,
					f,
				)
			}()

			select {
			case <-sigs:
			case <-donePrinting:
			}

			doneClose := make(chan struct{})
			go func() {
				defer close(doneClose)
				client.Close()
			}()

			select {
			case <-sigs:
			case <-doneClose:
			}
		},
	}
	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "Specify if the logs should be streamed")
	cmd.Flags().IntVar(&head, "head", 0, "The number of log entries to fetch from the start")
	cmd.Flags().IntVar(&tail, "tail", 0, "The number of log entries to fetch from the end")
	cmd.MarkFlagsMutuallyExclusive("head", "tail")
	// Tail and follow cannot be used together, if you're streaming forever it doesn't make since to get the "last" N logs.
	cmd.MarkFlagsMutuallyExclusive("tail", "follow")
	cmd.Flags().Var(&since, "since", "Start reading logs after this time (now, -10m, 2024-02-10)")
	cmd.Flags().Var(&until, "until", "Read logs up unto this time (-1h, 2024-02-10T13:00:00Z)")
	p.InstallFormatFlag(cmd)
	return cmd
}

func computeLogPartition(name string, tp kadm.TopicPartitions) int32 {
	// Use the murmur2 hash function to compute the partition
	p := kgo.StickyKeyPartitioner(nil)
	r := kgo.KeyStringRecord(name, "")
	return int32(p.ForTopic(tp.Topic).Partition(r, len(tp.Partitions)))
}

type timequery struct {
	time time.Time
}

func (tq *timequery) Set(s string) (err error) {
	tq.time, err = parseTimeQuery(s, time.Now())
	return
}

func (*timequery) Type() string {
	return "timequery"
}

func (tq *timequery) String() string {
	// Return empty string for defaults
	if tq.time.Equal(minTime) || tq.time.Equal(maxTime) {
		return ""
	}
	return tq.time.String()
}

func parseTimeQuery(s string, now time.Time) (time.Time, error) {
	switch {
	case strings.ToLower(s) == "now":
		return now.UTC(), nil
	// 13 digits is a millisecond.
	case regexp.MustCompile(`^\d{13}$`).MatchString(s):
		n, _ := strconv.ParseInt(s, 10, 64)
		return time.UnixMilli(n).UTC(), nil

	// 10 digits is a unix second.
	case regexp.MustCompile(`^\d{10}$`).MatchString(s):
		n, _ := strconv.ParseInt(s, 10, 64)
		return time.UnixMilli(n * 1000).UTC(), nil

	// YYYY-MM-DD
	case regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`).MatchString(s):
		return time.ParseInLocation("2006-01-02", s, time.UTC)

	// Z marks the end of an RFC3339.
	case strings.HasSuffix(s, "Z"):
		layout := "2006-01-02T15:04:05" // RFC3339 before the fractional second
		if dotNines := len(s) - len(layout) - 1; dotNines > 0 {
			layout += "." + strings.Repeat("9", dotNines-1)
		}
		layout += "Z"
		at, err := time.ParseInLocation(layout, s, time.UTC)
		if err != nil {
			return at, fmt.Errorf("unable to parse RFC3339 timestamp in %q: %v", s, err)
		}
		return at, err

	// This is either a duration or an error.
	default:
		negate := strings.HasPrefix(s, "-")
		if negate {
			s = s[1:]
		}
		var rel time.Duration
		rel, err := time.ParseDuration(s)
		if err != nil {
			return now, fmt.Errorf("unable to parse duration in %q", s)
		}
		if negate {
			rel = -rel
		}
		return now.Add(rel).UTC(), nil
	}
}

type RecordIter interface {
	Done() bool
	Next() *kgo.Record
}

type logSource interface {
	PollFetches(ctx context.Context) (RecordIter, error)
}

type clientLogSource struct {
	cl *kgo.Client
}

func (c *clientLogSource) PollFetches(ctx context.Context) (RecordIter, error) {
	fetches := c.cl.PollFetches(ctx)
	if fetches.IsClientClosed() {
		return nil, kgo.ErrClientClosed
	}
	fetches.EachError(func(_ string, _ int32, err error) {
		fmt.Fprintf(os.Stderr, "ERR: %v\n", err)
	})
	return fetches.RecordIter(), nil
}

type logsQuery struct {
	transformName string
	maxOffset     int64
	limit         int
}

type attributeValue struct {
	IntValue    int    `json:"intValue"`
	StringValue string `json:"stringValue"`
}

func (av attributeValue) MarshalJSON() ([]byte, error) {
	if len(av.StringValue) > 0 {
		return json.Marshal(map[string]string{"stringValue": av.StringValue})
	}
	return json.Marshal(map[string]int{"intValue": av.IntValue})
}

type rawLogEvent struct {
	Body struct {
		StringValue string `json:"stringValue"`
	} `json:"body"`
	TimeUnixNano   uint64 `json:"timeUnixNano"`
	SeverityNumber int    `json:"severityNumber"`
	Attributes     []struct {
		Key   string         `json:"key"`
		Value attributeValue `json:"value"`
	}
}

type logEventMode = int

const (
	logEventModeEmpty = iota
	logEventModeShort
	logEventModeWide
)

type logEvent struct {
	body    string
	time    time.Time
	warning bool
	mode    logEventMode
}

func printLogs(ctx context.Context, src logSource, q logsQuery, output io.Writer, f config.OutFormatter) error {
	numRecords := 0
	current := logEvent{
		body:    "",
		warning: false,
		mode:    logEventModeEmpty,
	}
	flush := func(force bool) error {
		if current.mode == logEventModeEmpty {
			return nil
		}
		write := func(line string) (int, error) { return fmt.Fprintf(output, "%s\n", line) }
		if current.mode == logEventModeWide {
			level := "INFO"
			if current.warning {
				level = "WARN"
			}
			write = func(line string) (int, error) {
				return fmt.Fprintf(output, "%s %s %s\n", current.time.UTC().Format("Jan 02 15:04:05"), level, line)
			}
		}
		for {
			advance, line := scanLine(current.body, force)
			if advance < 0 {
				return nil
			}
			current.body = current.body[advance:]
			if _, err := write(line); err != nil {
				return err
			}
			if advance == 0 || len(current.body) == 0 {
				current.mode = logEventModeEmpty
				return nil
			}
		}
	}
	for {
		zap.L().Sugar().Debugf("polling for log entries")
		it, err := src.PollFetches(ctx)
		if errors.Is(err, kgo.ErrClientClosed) {
			return flush(true)
		}
		for !it.Done() {
			r := it.Next()
			zap.L().Sugar().Debugf("seeing log entry: %s", r.Value)
			if r.Key == nil || string(r.Key) != q.transformName {
				if r.Offset >= q.maxOffset {
					return flush(true)
				}
				continue
			}
			numRecords += 1
			var evt rawLogEvent
			err := json.Unmarshal(r.Value, &evt)
			out.MaybeDie(err, "unable to parse log event: %v", err)
			isShort, isLong, formatted, err := f.Format(&evt)
			out.MaybeDie(err, "unable to format log event: %v", evt)
			if !isShort && !isLong {
				fmt.Fprintf(output, "%s\n", formatted)
				// Check if the limits are reached.
				// No need to flush, json/yaml output doesn't buffer.
				if q.limit > 0 && numRecords >= q.limit {
					return nil
				}
				if r.Offset >= q.maxOffset {
					return nil
				}
				continue
			}
			warning := evt.SeverityNumber > 9
			if warning != current.warning {
				if err := flush(true); err != nil {
					return err
				}
			}
			current.mode = logEventModeShort
			if isLong {
				current.mode = logEventModeWide
			}
			current.warning = warning
			current.body += evt.Body.StringValue
			current.time = time.Unix(0, int64(evt.TimeUnixNano))
			if err := flush(false); err != nil {
				return err
			}
			// Limit is reached, we're done
			if q.limit > 0 && numRecords >= q.limit {
				return flush(true)
			}
			if r.Offset >= q.maxOffset {
				return flush(true)
			}
		}
	}
}

func scanLine(data string, atEOF bool) (advance int, line string) {
	if atEOF && len(data) == 0 {
		return 0, ""
	}

	dropCR := func(data string) string {
		if len(data) > 0 && data[len(data)-1] == '\r' {
			return data[0 : len(data)-1]
		}
		return data
	}

	if i := strings.IndexByte(data, '\n'); i >= 0 {
		// We have a full newline-terminated line.
		return i + 1, dropCR(data[0:i])
	}
	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), dropCR(data)
	}
	// Request more data.
	return -1, ""
}

type tailWriter struct {
	w        io.Writer
	buffered list.List
	limit    int
}

func (w *tailWriter) Write(p []byte) (n int, err error) {
	cpy := make([]byte, len(p))
	_ = copy(cpy, p)
	w.buffered.PushBack(cpy)
	// Keep only N elements in the buffer
	for w.buffered.Len() > w.limit {
		w.buffered.Remove(w.buffered.Front())
	}
	return len(p), nil
}

func (w *tailWriter) Close() error {
	for e := w.buffered.Front(); e != nil; e = e.Next() {
		if _, err := w.w.Write(e.Value.([]byte)); err != nil {
			return err
		}
	}
	return nil
}
