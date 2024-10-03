// Copyright 2021 Redpanda Data, Inc.
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
	"encoding/json"
	"errors"
	"fmt"
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
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/schemaregistry"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/serde"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
	"go.uber.org/zap"
)

type consumer struct {
	partitions []int32
	regex      bool

	rack     string
	group    string
	balancer string

	fetchMaxBytes          int32
	fetchMaxWait           time.Duration
	fetchMaxPartitionBytes int32
	readCommitted          bool
	printControl           bool

	f        *kgo.RecordFormatter // if not json
	num      int
	pretty   bool // specific to -f json
	metaOnly bool // specific to -f json

	resetOffset kgo.Offset // defaults to NoResetOffset, can be start or end

	useSchemaRegistry []string // schema registry options
	decodeKey         bool
	decodeVal         bool

	// If an end offset is specified, we immediately look up where we will
	// end and quit rpk when we hit the end.
	partEnds   map[string]map[int32]int64
	partStarts map[string]map[int32]int64

	cl   *kgo.Client
	srCl *sr.Client
}

func newConsumeCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		c      consumer
		offset string
		format string
	)

	cmd := &cobra.Command{
		Use:   "consume TOPICS...",
		Short: "Consume records from topics",
		Long:  helpConsume,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, topics []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p)
			out.MaybeDie(err, "unable to initialize admin kafka client: %v", err)

			// We fail if the topic does not exist.
			if !c.regex {
				listed, err := adm.ListTopics(cmd.Context(), topics...)
				out.MaybeDie(err, "unable to check topic existence: %v", err)
				listed.EachError(func(d kadm.TopicDetail) {
					if errors.Is(d.Err, kerr.UnknownTopicOrPartition) {
						out.Die("unable to consume topic %q: %v", d.Topic, d.Err.Error())
					}
				})
			}

			err = c.parseOffset(offset, topics, adm)
			out.MaybeDie(err, "invalid --offset %q: %v", offset, err)
			if allEmpty := c.filterEmptyPartitions(); allEmpty {
				return
			}

			opts, err := c.intoOptions(topics)
			out.MaybeDieErr(err)

			if format != "json" {
				c.f, err = kgo.NewRecordFormatter(format)
				out.MaybeDie(err, "invalid --format: %v", err)
			}

			sigs := make(chan os.Signal, 2)
			signal.Notify(sigs, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

			c.cl, err = kafka.NewFranzClient(fs, p, opts...)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)

			if len(c.useSchemaRegistry) > 0 {
				err = c.formatSchemaRegistryFlag()
				out.MaybeDie(err, "unable to parse --use-schema-registry flag: %v", err)
				c.srCl, err = schemaregistry.NewClient(fs, p)
				out.MaybeDie(err, "unable to initialize schema registry client: %v", err)
			}

			doneConsume := make(chan struct{})
			go func() {
				defer close(doneConsume)
				c.consume(cmd.Context())
				c.cl.LeaveGroup()
			}()

			select {
			case <-sigs:
			case <-doneConsume:
			}

			doneClose := make(chan struct{})
			go func() {
				defer close(doneClose)
				c.cl.Close()
			}()

			select {
			case <-sigs:
			case <-doneClose:
			}
		},
	}

	cmd.Flags().StringVarP(&offset, "offset", "o", "start", "Offset to consume from / to (start, end, 47, +2, -3)")
	cmd.Flags().Int32SliceVarP(&c.partitions, "partitions", "p", nil, "Comma delimited list of specific partitions to consume")
	cmd.Flags().BoolVarP(&c.regex, "regex", "r", false, "Parse topics as regex; consume any topic that matches any expression")

	cmd.Flags().StringVarP(&c.group, "group", "g", "", "Group to use for consuming (incompatible with -p)")
	cmd.Flags().StringVarP(&c.balancer, "balancer", "b", "cooperative-sticky", "Group balancer to use if group consuming (range, roundrobin, sticky, cooperative-sticky)")

	cmd.Flags().Int32Var(&c.fetchMaxBytes, "fetch-max-bytes", 1<<20, "Maximum amount of bytes per fetch request per broker")
	cmd.Flags().DurationVar(&c.fetchMaxWait, "fetch-max-wait", 5*time.Second, "Maximum amount of time to wait when fetching from a broker before the broker replies")
	cmd.Flags().Int32Var(&c.fetchMaxPartitionBytes, "fetch-max-partition-bytes", 1<<20, "Maximum amount of bytes that will be consumed for a single partition per fetch request")
	cmd.Flags().BoolVar(&c.readCommitted, "read-committed", false, "Opt in to reading only committed offsets")
	cmd.Flags().BoolVar(&c.printControl, "print-control-records", false, "Opt in to printing control records")

	cmd.Flags().StringVarP(&format, "format", "f", "json", "Output format (see --help for details)")
	cmd.Flags().IntVarP(&c.num, "num", "n", 0, "Quit after consuming this number of records (0 is unbounded)")
	cmd.Flags().BoolVar(&c.pretty, "pretty-print", true, "Pretty print each record over multiple lines (for -f json)")
	cmd.Flags().BoolVar(&c.metaOnly, "meta-only", false, "Print all record info except the record value (for -f json)")

	cmd.Flags().StringVar(&c.rack, "rack", "", "Rack to use for consuming, which opts into follower fetching")

	cmd.Flags().StringSliceVar(&c.useSchemaRegistry, "use-schema-registry", []string{}, "If present, rpk will decode the key and the value with the schema registry. Also accepts use-schema-registry=key or use-schema-registry=value")
	// Deprecated.
	cmd.Flags().BoolVar(new(bool), "commit", false, "")
	cmd.Flags().MarkDeprecated("commit", "Group consuming always commits")

	cmd.Flags().Lookup("use-schema-registry").NoOptDefVal = "key,value"

	return cmd
}

func (c *consumer) consume(ctx context.Context) {
	var (
		buf   []byte
		n     int
		marks []*kgo.Record
		done  bool
	)
	serdeCache := make(map[int]*serde.Serde)
	for !done {
		fs := c.cl.PollFetches(ctx)
		if fs.IsClientClosed() {
			return
		}

		fs.EachError(func(t string, p int32, err error) {
			fmt.Fprintf(os.Stderr, "ERR: topic %s partition %d: %v\n", t, p, err)
		})

		marks = marks[:0]
		fs.EachPartition(func(p kgo.FetchTopicPartition) {
			// If we are done, we finished in a prior partition
			// and just need to exit EachPartition.
			if done {
				return
			}
			pend, hasEnd := c.getPartitionEnd(p.Topic, p.Partition)
			if hasEnd && pend < 0 {
				return // reached end, still draining client
			}

			for _, r := range p.Records {
				var toStdErr bool
				if r.Key != nil && c.decodeKey {
					decKey, err := handleDecode(ctx, c.srCl, r.Key, serdeCache)
					if err != nil {
						zap.L().Sugar().Warnf("unable to decode record key %v with schema registry: %v\n", r.Key, err)
						toStdErr = true
					} else {
						r.Key = decKey
					}
				}
				if r.Value != nil && c.decodeVal {
					decVal, err := handleDecode(ctx, c.srCl, r.Value, serdeCache)
					if err != nil {
						zap.L().Sugar().Warnf("unable to decode record value %v with schema registry: %v\n", r.Value, err)
						toStdErr = true
					} else {
						r.Value = decVal
					}
				}
				if !r.Attrs.IsControl() || c.printControl {
					if c.f == nil {
						c.writeRecordJSON(r, toStdErr)
					} else {
						buf = c.f.AppendPartitionRecord(buf[:0], &p.FetchPartition, r)
						if toStdErr {
							os.Stderr.Write(buf)
						} else {
							os.Stdout.Write(buf)
						}
					}
				}

				// Track this record to be "marked" once this loop
				// is over.
				marks = append(marks, r)
				n++

				if done = c.num > 0 && n >= c.num; done {
					return
				}

				if hasEnd && r.Offset >= pend-1 {
					done = c.markPartitionEnded(p.Topic, p.Partition)
					return
				}
			}
		})

		// Before we poll, we mark everything we just processed to be
		// available for autocommitting.
		c.cl.MarkCommitRecords(marks...)
	}
}

func (c *consumer) writeRecordJSON(r *kgo.Record, toStdErr bool) {
	type Header struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	var valuePtr *string
	if r.Value != nil {
		valueStr := string(r.Value)
		valuePtr = &valueStr
	}

	m := struct {
		Topic     string   `json:"topic"`
		Key       string   `json:"key,omitempty"`
		Value     *string  `json:"value,omitempty"`
		ValueSize *int     `json:"value_size,omitempty"` // non-nil if --meta-only
		Headers   []Header `json:"headers,omitempty"`
		Timestamp int64    `json:"timestamp"` // millis

		Partition int32 `json:"partition"`
		Offset    int64 `json:"offset"`
	}{
		Topic:     r.Topic,
		Key:       string(r.Key),
		Value:     valuePtr,
		Headers:   make([]Header, 0, len(r.Headers)),
		Timestamp: r.Timestamp.UnixNano() / 1e6,

		Partition: r.Partition,
		Offset:    r.Offset,
	}

	if c.metaOnly {
		size := len(r.Value)
		m.Value = nil
		m.ValueSize = &size
	}

	for _, h := range r.Headers {
		m.Headers = append(m.Headers, Header{
			Key:   h.Key,
			Value: string(h.Value),
		})
	}

	// We are marshaling a simple type defined just above; this type
	// cannot cause a marshal error.
	var out []byte
	if c.pretty {
		out, _ = json.MarshalIndent(m, "", "  ")
	} else {
		out, _ = json.Marshal(m)
	}
	if toStdErr {
		os.Stderr.Write(out)
		os.Stderr.Write(newline)
	} else {
		os.Stdout.Write(out)
		os.Stdout.Write(newline)
	}
}

var newline = []byte("\n")

func (c *consumer) parseOffset(
	offset string, topics []string, adm *kadm.Client,
) error {
	if strings.HasPrefix(offset, "@") { // timestamp offset; strip @
		offset = offset[1:]
		return c.parseTimeOffset(offset, topics, adm)
	}

	start, end, rel, atStart, atEnd, hasEnd, currentEnd, err := parseFromToOffset(offset)
	if err != nil {
		return fmt.Errorf("unable to parse offset: %v", err)
	}

	if atStart {
		c.resetOffset = kgo.NewOffset().AtStart().Relative(rel)
	} else if atEnd {
		c.resetOffset = kgo.NewOffset().AtEnd().Relative(rel)
	} else {
		c.resetOffset = kgo.NewOffset().At(start)
	}

	// If we have no defined end, we can return now and just use our reset
	// offset to start consuming. If we have a defined end, then we need to
	// (a) list the start offsets to know what to consume from,
	// (b) list the end offsets to define what to consume to,
	// (c) filter partitions that have nothing to consume.
	if !(hasEnd || currentEnd) {
		return nil
	}

	lstart, err := adm.ListStartOffsets(context.Background(), topics...)
	if err != nil {
		return fmt.Errorf("unable to list start offsets: %v", err)
	}
	lend, err := adm.ListEndOffsets(context.Background(), topics...)
	if err != nil {
		return fmt.Errorf("unable to list end offsets: %v", err)
	}

	c.setParts(&c.partStarts, lstart, func(o int64) int64 {
		if !atStart && o < start {
			o = start
		}
		return o
	})
	c.setParts(&c.partEnds, lend, func(o int64) int64 {
		if !currentEnd && o > end {
			o = end
		}
		return o
	})
	return nil
}

// Setting partStarts and partEnds is identical, so we use a small closure to
// capture the logic: if partitions are specified, we keep only those,
// otherwise we keep all partitions. The optional offsetFn can be used to
// override the listed offset (bound to the start, end).
func (c *consumer) setParts(
	mp *map[string]map[int32]int64,
	l kadm.ListedOffsets,
	offsetFn func(int64) int64,
) {
	if offsetFn == nil {
		offsetFn = func(o int64) int64 { return o }
	}
	toffsets := make(map[string]map[int32]int64)
	*mp = toffsets
	for t, lps := range l {
		poffsets := make(map[int32]int64)
		toffsets[t] = poffsets
		for _, p := range c.partitions {
			if l, exists := lps[p]; exists {
				poffsets[p] = offsetFn(l.Offset)
			}
		}
		if len(c.partitions) == 0 {
			for p, l := range lps {
				poffsets[p] = offsetFn(l.Offset)
			}
		}
	}
}

// The returns:
//
// - atStart:    consuming from the start, we might have an end (other bools)
// - atEnd:      consuming at the *current* end; no end
// - hasEnd:     consume until the returned end number
// - currentEnd: consume until the *current* end offset
//
// - if !atStart && !atEnd, then we consume at the returned start number
// - rel is used for relative offsets from atStart or atEnd.
func parseFromToOffset(
	o string,
) (
	start, end, rel int64,
	atStart, atEnd, hasEnd, currentEnd bool,
	err error,
) {
	switch {
	case o == "start" || o == "oldest":
		atStart = true
		return
	case o == "end" || o == "newest":
		atEnd = true
		return
	case o == ":end":
		atStart, currentEnd = true, true
		return
	case strings.HasPrefix(o, "+"): // +3, relative to current start offset
		atStart = true
		rel, err = strconv.ParseInt(o[1:], 10, 64)
		return
	case strings.HasPrefix(o, "-"): // -3, relative to the end offset
		atEnd = true
		rel, err = strconv.ParseInt(o, 10, 64)
		return
	}

	// oo, oo:, and :oo
	if start, err = strconv.ParseInt(o, 10, 64); err == nil {
		return //nolint:nilerr // false positive with naked returns
	} else if strings.HasSuffix(o, ":") {
		start, err = strconv.ParseInt(o[:len(o)-1], 10, 64)
		return
	} else if strings.HasPrefix(o, ":") {
		end, err = strconv.ParseInt(o[1:], 10, 64)
		atStart = true
		hasEnd = true
		return
	}

	// This must be either oo:oo or oo-oo, where oo can be "end".
	halves := strings.SplitN(o, ":", 2)
	if len(halves) != 2 {
		halves = strings.SplitN(o, "-", 2)
		if len(halves) != 2 {
			err = errors.New("cannot parse")
			return
		}
	}

	hasEnd = true
	if start, err = strconv.ParseInt(halves[0], 10, 64); err == nil {
		if end, err = strconv.ParseInt(halves[1], 10, 64); err != nil && halves[1] == "end" {
			hasEnd, currentEnd, err = false, true, nil
			return
		}
	}
	if end <= start {
		err = fmt.Errorf("end %d <= start %d", end, start)
	}
	return
}

// Parses the first or second timestamp in a timestamp based offset.
//
// The expressions below all end in (?::|$), meaning, a non capturing group for
// the first time followed by a colon or the second time at the end.
func parseTimestampBasedOffset(
	half string, baseTimestamp time.Time,
) (length int, at time.Time, end bool, fromTimestamp bool, err error) {
	switch {
	// 13 digits is a millisecond.
	case regexp.MustCompile(`^\d{13}(?::|$)`).MatchString(half):
		length = 13
		n, _ := strconv.ParseInt(half[:length], 10, 64)
		at = time.Unix(0, n*1e6)
		fromTimestamp = true
		return

	// 10 digits is a unix second.
	case regexp.MustCompile(`^\d{10}(?::|$)`).MatchString(half):
		length = 10
		n, _ := strconv.ParseInt(half[:length], 10, 64)
		at = time.Unix(n, 0)
		fromTimestamp = true
		return

	// YYYY-MM-DD
	case regexp.MustCompile(`^\d{4}-\d{2}-\d{2}(?::|$)`).MatchString(half):
		length = 10
		at, _ = time.ParseInLocation("2006-01-02", half[:length], time.UTC)
		fromTimestamp = true
		return

	// Z marks the end of an RFC3339; we try to parse it and if it
	// fails, input is invalid.
	case strings.IndexByte(half, 'Z') != -1:
		length = strings.IndexByte(half, 'Z') + 1
		layout := "2006-01-02T15:04:05" // RFC3339 before the fractional second
		if dotNines := length - len(layout) - 1; dotNines > 0 {
			layout += "." + strings.Repeat("9", dotNines-1)
		}
		layout += "Z"
		at, err = time.ParseInLocation(layout, half[:length], time.UTC)
		if err != nil {
			err = fmt.Errorf("unable to parse RFC3339 timestamp in %q: %v", half, err)
		}
		fromTimestamp = true
		return

	case half == "end": // t2
		length = 3
		end = true
		return

	// This is either a duration or an error.
	default:
		if colon := strings.IndexByte(half, ':'); colon != -1 {
			half = half[:colon]
		}
		length = len(half)
		negate := strings.HasPrefix(half, "-")
		if negate {
			half = half[1:]
		}
		var rel time.Duration
		rel, err = time.ParseDuration(half)
		if err != nil {
			err = fmt.Errorf("unable to parse duration in %q", half)
		}
		if negate {
			rel = -rel
		}
		at = baseTimestamp.Add(rel).UTC()
		return
	}
}

// parseTimeOffset is a bit more complicated, because our start & end
// timestamps can have colons in them (RFC3339). Rather than parse the
// whole string at a time, we parse each half individually.
//
// If the first half is a timestamp, then if the second half is a duration,
// we parse relative to the first half. If both are durations, we parse
// relative to now.
func (c *consumer) parseTimeOffset(
	offset string, topics []string, adm *kadm.Client,
) error {
	c.resetOffset = kgo.NewOffset().AtStart() // default to start; likely overridden below
	var (
		length        int
		startAt       time.Time
		endAt         time.Time
		end           bool
		fromTimestamp bool
		err           error
	)
	if !strings.HasPrefix(offset, ":") {
		length, startAt, end, fromTimestamp, err = parseTimestampBasedOffset(offset, time.Now())
		if err != nil {
			return err
		} else if end {
			return errors.New("invalid offset @end")
		}

		// If there are no offsets after the requested milli, we get
		// the default offset -1, which below in NewOffset().At(-1)
		// actually coincidentally maps to AtEnd(). So it all works.
		lstart, err := adm.ListOffsetsAfterMilli(context.Background(), startAt.UnixMilli(), topics...)
		if err == nil {
			err = lstart.Error()
		}
		if err != nil {
			return fmt.Errorf("unable to list offsets after milli %d: %v", startAt.UnixMilli(), err)
		}

		c.setParts(&c.partStarts, lstart, nil)
		offset = offset[length:]
		if offset == "" || offset == ":" { // requesting from a time onward; e.g., @1m:
			return nil
		}
	} else {
		lstart, err := adm.ListStartOffsets(context.Background(), topics...)
		if err == nil {
			err = lstart.Error()
		}
		if err != nil {
			return fmt.Errorf("unable to list start offsets: %v", err)
		}
		c.setParts(&c.partStarts, lstart, nil)
	}
	offset = offset[1:] // strip start:end delimiting colon

	if fromTimestamp {
		length, endAt, end, _, err = parseTimestampBasedOffset(offset, startAt)
	} else {
		length, endAt, end, _, err = parseTimestampBasedOffset(offset, time.Now())
	}

	if err != nil {
		return err
	} else if len(offset) != length {
		return fmt.Errorf("invalid trailing content %q afer offset", offset[length:])
	}
	var lend kadm.ListedOffsets
	if end {
		lend, err = adm.ListEndOffsets(context.Background(), topics...)
		if err == nil {
			err = lend.Error()
		}
		if err != nil {
			return fmt.Errorf("unable to list end offsets: %v", err)
		}
	} else {
		lend, err = adm.ListOffsetsAfterMilli(context.Background(), endAt.UnixMilli(), topics...)
		if err == nil {
			err = lend.Error()
		}
		if err != nil {
			return fmt.Errorf("unable to list offsets after milli %d: %v", endAt.UnixMilli(), err)
		}
	}
	c.setParts(&c.partEnds, lend, nil)

	// If there was no start offset (just "consume from beginning"), then
	// we want to list offsets to see where to start from and filter empty
	// partitions.
	if c.partEnds == nil {
		lstart, err := adm.ListStartOffsets(context.Background(), topics...)
		if err == nil {
			err = lstart.Error()
		}
		if err != nil {
			return fmt.Errorf("unable to list start offsets: %v", err)
		}
		c.setParts(&c.partStarts, lstart, nil)
	}
	return nil
}

// If we have defined end offsets, we load start offsets and filter empty
// partitions. If all partitions are empty, we avoid consuming.
//
// Depending on our order of requests and pathological timing, end could
// come before the start; in this case we also filter the partition.
func (c *consumer) filterEmptyPartitions() (allEmpty bool) {
	if c.partEnds == nil {
		return false
	}
	starts, ends := c.partStarts, c.partEnds
	for lt, lps := range starts {
		rps, exists := ends[lt]
		if exists {
			for lp, lat := range lps {
				rat, exists := rps[lp]
				if exists && rat <= lat {
					delete(rps, lp)
					delete(lps, lp)
				}
			}
		}
		if len(rps) == 0 {
			delete(ends, lt)
		}
		if len(lps) == 0 {
			delete(starts, lt)
		}
	}
	return len(c.partEnds) == 0 && len(c.partStarts) == 0
}

// If an end offset was specified, we check where the end is of each partition
// we consume. If the topic or partition no longer exists, we reached the end
// of the partition and are still draining what was buffered in the client.
func (c *consumer) getPartitionEnd(t string, p int32) (pend int64, has bool) {
	if c.partEnds == nil {
		return -1, false
	}
	defer func() {
		if pend == -1 {
			c.markPartitionEnded(t, p) // just in case this was not paused
		}
	}()
	tends := c.partEnds[t]
	if tends == nil {
		return -1, true
	}
	pend, has = tends[p]
	if !has {
		return -1, true
	}
	return pend, true
}

// If an end offset was specified and we reach that offset for a partition, we
// mark it done, pause it from ever being fetched again, and remove it from our
// ends map. If the ends map is now empty, we are done consuming.
func (c *consumer) markPartitionEnded(t string, p int32) (done bool) {
	c.cl.PauseFetchPartitions(map[string][]int32{t: {p}})
	delete(c.partEnds[t], p)
	if len(c.partEnds[t]) == 0 {
		delete(c.partEnds, t)
	}
	return len(c.partEnds) == 0
}

func (c *consumer) intoOptions(topics []string) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.ConsumeResetOffset(c.resetOffset),
	}

	if c.group != "" {
		if len(c.partitions) != 0 {
			return nil, errors.New("invalid flags: only one of --partitions and --group can be specified")
		} else if c.partEnds != nil {
			return nil, errors.New("invalid flags: group consuming is not supported with consume-until-end offsets")
		}
		opts = append(opts, kgo.ConsumerGroup(c.group))
		opts = append(opts, kgo.AutoCommitMarks())
	}
	if c.rack != "" {
		opts = append(opts, kgo.Rack(c.rack))
	}

	// If we have ends, we have to consume control records because a
	// control record might be the end.
	if c.partEnds != nil || c.printControl {
		opts = append(opts, kgo.KeepControlRecords())
	}

	switch {
	// If we have a defined end, then we always load what we start at and
	// filter for only partitions we want to consume.
	case c.partStarts != nil:
		offsets := make(map[string]map[int32]kgo.Offset)
		for t, psStart := range c.partStarts {
			ps := make(map[int32]kgo.Offset)
			offsets[t] = ps
			for p, start := range psStart {
				ps[p] = kgo.NewOffset().At(start)
			}
		}
		opts = append(opts, kgo.ConsumePartitions(offsets))

	// If no partitions were specified, we want to consume topics directly.
	// If we did not specify an end offset, then we just consume them.
	case len(c.partitions) == 0:
		opts = append(opts, kgo.ConsumeTopics(topics...))

	// Partitions were specified and there is no end: we create our consume
	// offsets from our reset offset.
	default:
		offsets := make(map[string]map[int32]kgo.Offset)
		for _, t := range topics {
			ps := make(map[int32]kgo.Offset, len(c.partitions))
			for _, p := range c.partitions {
				ps[p] = c.resetOffset
			}
			offsets[t] = ps
		}
		opts = append(opts, kgo.ConsumePartitions(offsets))
	}

	if c.regex {
		opts = append(opts, kgo.ConsumeRegex())
	}

	switch c.balancer {
	case "range":
		opts = append(opts, kgo.Balancers(kgo.RangeBalancer()))
	case "roundrobin":
		opts = append(opts, kgo.Balancers(kgo.RoundRobinBalancer()))
	case "sticky":
		opts = append(opts, kgo.Balancers(kgo.StickyBalancer()))
	case "cooperative-sticky":
		opts = append(opts, kgo.Balancers(kgo.CooperativeStickyBalancer()))
	case "":
		if len(c.group) == 0 {
			return nil, errors.New("--balancer is required when using --group")
		}
	default:
		return nil, fmt.Errorf("unrecognized --balancer %q", c.balancer)
	}

	opts = append(opts,
		kgo.FetchMaxBytes(c.fetchMaxBytes),
		kgo.FetchMaxWait(c.fetchMaxWait),
		kgo.FetchMaxPartitionBytes(c.fetchMaxPartitionBytes),
	)
	if c.readCommitted {
		opts = append(opts, kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	}

	return opts, nil
}

func (c *consumer) formatSchemaRegistryFlag() error {
	if len(c.useSchemaRegistry) == 0 {
		// We don't expect to hit this, but better be safe.
		return errors.New("schema registry flag is empty")
	}
	for _, f := range c.useSchemaRegistry {
		switch {
		case strings.HasPrefix("value", f):
			c.decodeVal = true
		case strings.HasPrefix("key", f):
			c.decodeKey = true
		default:
			return fmt.Errorf("unsupported decode type %q", f)
		}
	}
	return nil
}

func handleDecode(ctx context.Context, cl *sr.Client, record []byte, serdeCache map[int]*serde.Serde) ([]byte, error) {
	var serdeHeader sr.ConfluentHeader
	id, toDecode, err := serdeHeader.DecodeID(record)
	if err != nil {
		return nil, errors.New("unable to decode the ID")
	}
	var rpkSerde *serde.Serde
	if cachedSerde, ok := serdeCache[id]; ok {
		rpkSerde = cachedSerde
	} else {
		schema, err := cl.SchemaByID(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("unable to get schema with index %v from the schema registry: %v", id, err)
		}
		rpkSerde, err = serde.NewSerde(ctx, cl, &schema, id, "")
		if err != nil {
			return nil, err
		}
		serdeCache[id] = rpkSerde
	}

	return rpkSerde.DecodeRecord(toDecode)
}

const helpConsume = `Consume records from topics.

Consuming records reads from any amount of input topics, formats each record
according to --format, and prints them to STDOUT. The output formatter
understands a wide variety of formats.

The default output format "--format json" is a special format that outputs each
record as JSON. There may be more single-word-no-escapes formats added later.
Outside of these special formats, formatting follows the rules described below.

Formatting output is based on percent escapes and modifiers. Slashes can be
used for common escapes:

    \t \n \r \\ \xNN

prints tabs, newlines, carriage returns, slashes, or hex encoded characters.p

Percent encoding prints record fields, fetch partition fields, or extra values:

    %t    topic
    %T    topic length
    %k    key
    %K    key length
    %v    value
    %V    value length
    %h    begin the header specification
    %H    number of headers
    %p    partition
    %o    offset
    %e    leader epoch
    %d    timestamp (formatting described below)
    %a    record attributes (formatting described below)
    %x    producer id
    %y    producer epoch

    %[    partition log start offset
    %|    partition last stable offset
    %]    partition high watermark

    %%    percent sign
    %{    left brace
    %}    right brace

    %i    the number of records formatted

MODIFIERS

Text and numbers can be formatted in many different ways, and the default
format can be changed within brace modifiers. %v prints a value, while %v{hex}
prints the value hex encoded. %T prints the length of a topic in ascii, while
%T{big8} prints the length of the topic as an eight byte big endian.

All modifiers go within braces following a percent-escape.

NUMBERS

Formatting number values can have the following modifiers:

     ascii       print the number as ascii (default)

     hex64       sixteen hex characters
     hex32       eight hex characters
     hex16       four hex characters
     hex8        two hex characters
     hex4        one hex character

     big64       eight byte big endian number
     big32       four byte big endian number
     big16       two byte big endian number
     big8        alias for byte

     little64    eight byte little endian number
     little32    four byte little endian number
     little16    two byte little endian number
     little8     alias for byte

     byte        one byte number
     bool        "true" if the number is non-zero, "false" if the number is zero

All numbers are truncated as necessary per the modifier. Printing %V{byte} for
a length 256 value will print a single null, whereas printing %V{big8} would
print the bytes 1 and 0.

When writing number sizes, the size corresponds to the size of the raw values,
not the size of encoded values. "%T% t{hex}" for the topic "foo" will print
"3 666f6f", not "6 666f6f".

TIMESTAMPS

By default, the timestamp field is printed as a millisecond number value. In
addition to the number modifiers above, timestamps can be printed with either
Go formatting or strftime formatting:

    %d{go[2006-01-02T15:04:05Z07:00]}
    %d{strftime[%F]}

An arbitrary amount of brackets (or braces, or # symbols) can wrap your date
formatting:

    %d{strftime### [%F] ###}

The above will print " [YYYY-MM-DD] ", while the surrounding three # on each
side are used to wrap the formatting. Further details on Go time formatting can
be found at https://pkg.go.dev/time, while further details on strftime
formatting can be read by checking "man strftime".

ATTRIBUTES

Each record (or batch of records) has a set of possible attributes. Internally,
these are packed into bit flags. Printing an attribute requires first selecting
which attribute you want to print, and then optionally specifying how you want
it to be printed:

     %a{compression}
     %a{compression;number}
     %a{compression;big64}
     %a{compression;hex8}

Compression is by default printed as text ("none", "gzip", ...). Compression
can be printed as a number with ";number", where number is any number
formatting option described above. No compression is 0, gzip is 1, etc.

     %a{timestamp-type}
     %a{timestamp-type;big64}

The record's timestamp type is printed as -1 for very old records (before
timestamps existed), 0 for client generated timestamps, and 1 for broker
generated timestamps. Number formatting can be controlled with ";number".

     %a{transactional-bit}
     %a{transactional-bit;bool}

Prints 1 if the record a part of a transaction or 0 if it is not.
Number formatting can be controlled with ";number".

     %a{control-bit}
     %a{control-bit;bool}

Prints 1 if the record is a commit marker or 0 if it is not.
Number formatting can be controlled with ";number".

TEXT

Text fields without modifiers default to writing the raw bytes. Alternatively,
there are the following modifiers:

    %t{hex}
    %k{base64}
    %v{base64raw}
    %v{unpack[<bBhH>iIqQc.$]}

The hex modifier hex encodes the text, the base64 modifier base64 encodes the
text with standard encoding, and the base64raw modifier encodes the text with
raw standard encoding. The unpack modifier has a further internal
specification, similar to timestamps above:

    x    pad character (does not parse input)
    <    switch what follows to little endian
    >    switch what follows to big endian

    b    signed byte
    B    unsigned byte
    h    int16  ("half word")
    H    uint16 ("half word")
    i    int32
    I    uint32
    q    int64  ("quad word")
    Q    uint64 ("quad word")

    c    any character
    .    alias for c
    s    consume the rest of the input as a string
    $    match the end of the line (append error string if anything remains)

Unpacking text can allow translating binary input into readable output. If a
value is a big-endian uint32, %v will print the raw four bytes, while
%v{unpack[>I]} will print the number in as ascii. If unpacking exhausts the
input before something is unpacked fully, an error message is appended to the
output.

HEADERS

Headers are formatted with percent encoding inside of the modifier:

    %h{ %k=%v{hex} }

will print all headers with a space before the key and after the value, an
equals sign between the key and value, and with the value hex encoded. Header
formatting actually just parses the internal format as a record format, so all
of the above rules about %K, %V, text, and numbers apply.

VALUES

Values for consumed records can be omitted by using the '--meta-only' flag.
Tombstone records (records with a 'null' value) have their value omitted
from the JSON output by default. All other records, including those with
an empty-string value (""), will have their values printed.

EXAMPLES

A key and value, separated by a space and ending in newline:
    -f '%k %v\n'
A key length as four big endian bytes, and the key as hex:
    -f '%K{big32}%k{hex}'
A little endian uint32 and a string unpacked from a value:
    -f '%v{unpack[is$]}'

OFFSETS

The --offset flag allows for specifying where to begin consuming, and
optionally, where to stop consuming. The literal words "start" and "end"
specify consuming from the start and the end.

    start     consume from the beginning
    end       consume from the end
    :end      consume until the current end
    +oo       consume oo after the current start offset
    -oo       consume oo before the current end offset
    oo        consume after an exact offset
    oo:       alias for oo
    :oo       consume until an exact offset
    o1:o2     consume from exact offset o1 until exact offset o2
    @t        consume starting from a given timestamp
    @t:       alias for @t
    @:t       consume until a given timestamp
    @t1:t2    consume from timestamp t1 until timestamp t2

There are a few options for timestamps, with each option being evaluated
until one succeeds:

    13 digits             parsed as a unix millisecond
    9 digits              parsed as a unix second
    YYYY-MM-DD            parsed as a day, UTC
    YYYY-MM-DDTHH:MM:SSZ  parsed as RFC3339, UTC; fractional seconds optional (.MMM)
    end                   for t2 in @t1:t2, the current end of the partition
    -dur                  a negative duration from now or from a timestamp
    dur                   a positive duration from now or from a timestamp

Durations can be relative to the current time or relative to a timestamp.
If a duration is used for t1, that duration is relative to now.
If a duration is used for t2, if t1 is a timestamp, then t2 is relative to t1.
If a duration is used for t2, if t1 is a duration, then t2 is relative to now.

Durations are parsed simply:

    3ms    three milliseconds
    10s    ten seconds
    9m     nine minutes
    1h     one hour
    1m3ms  one minute and three milliseconds

For example,

    -o @2022-02-14:1h   consume 1h of time on Valentine's Day 2022
    -o @-48h:-24h       consume from 2 days ago to 1 day ago
    -o @-1m:end         consume from 1m ago until now
    -o @:-1hr           consume from the start until an hour ago
`
