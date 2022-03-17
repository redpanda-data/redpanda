// Copyright 2021 Vectorized, Inc.
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

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
)

type consumer struct {
	partitions []int32
	regex      bool

	group    string
	balancer string

	fetchMaxBytes int32
	fetchMaxWait  time.Duration
	readCommitted bool

	f        *kgo.RecordFormatter // if not json
	num      int
	pretty   bool // specific to -f json
	metaOnly bool // specific to -f json

	// from parseOffset
	offset kgo.Offset
	start  int64 // if non-negative, start each partition here
	end    int64 // if non-negative, end each partition here

	cl *kgo.Client
}

func NewConsumeCommand(fs afero.Fs) *cobra.Command {
	var (
		c      consumer
		offset string
		format string
	)

	cmd := &cobra.Command{
		Use:   "consume TOPICS...",
		Short: "Consume records from topics.",
		Long:  helpConsume,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			err := c.parseOffset(offset)
			out.MaybeDie(err, "invalid --offset: %v", err)

			opts, err := c.intoOptions(args)
			out.MaybeDieErr(err)

			if format != "json" {
				c.f, err = kgo.NewRecordFormatter(format)
				out.MaybeDie(err, "invalid --format: %v", err)
			}

			sigs := make(chan os.Signal, 2)
			signal.Notify(sigs, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			c.cl, err = kafka.NewFranzClient(fs, p, cfg, opts...)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)

			doneConsume := make(chan struct{})
			go func() {
				defer close(doneConsume)
				c.consume()
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

	cmd.Flags().StringVarP(&c.group, "group", "g", "", "group to use for consuming (incompatible with -p)")
	cmd.Flags().StringVarP(&c.balancer, "balancer", "b", "cooperative-sticky", "Group balancer to use if group consuming (range, roundrobin, sticky, cooperative-sticky)")

	cmd.Flags().Int32Var(&c.fetchMaxBytes, "fetch-max-bytes", 1<<20, "Maximum amount of bytes per fetch request per broker")
	cmd.Flags().DurationVar(&c.fetchMaxWait, "fetch-max-wait", 5*time.Second, "Maximum amount of time to wait when fetching from a broker before the broker replies")
	cmd.Flags().BoolVar(&c.readCommitted, "read-committed", false, "Opt in to reading only committed offsets")

	cmd.Flags().StringVarP(&format, "format", "f", "json", "Output format (see --help for details)")
	cmd.Flags().IntVarP(&c.num, "num", "n", 0, "Quit after consuming this number of records (0 is unbounded)")
	cmd.Flags().BoolVar(&c.pretty, "pretty-print", true, "Pretty print each record over multiple lines (for -f json)")
	cmd.Flags().BoolVar(&c.metaOnly, "meta-only", false, "Print all record info except the record value (for -f json)")

	// Deprecated.
	cmd.Flags().BoolVar(new(bool), "commit", false, "")
	cmd.Flags().MarkDeprecated("commit", "group consuming always commits")

	return cmd
}

func (c *consumer) consume() {
	var buf []byte
	var n int
	var marks []*kgo.Record
	for {
		fs := c.cl.PollFetches(context.Background())
		if fs.IsClientClosed() {
			return
		}

		fs.EachError(func(t string, p int32, err error) {
			fmt.Fprintf(os.Stderr, "ERR: topic %s partition %d: %v", t, p, err)
		})

		marks = marks[:0]
		for _, f := range fs {
			for _, t := range f.Topics {
				for _, p := range t.Partitions {
					for _, r := range p.Records {
						if c.f == nil {
							c.writeRecordJSON(r)
						} else {
							buf = c.f.AppendPartitionRecord(buf[:0], &p, r)
							os.Stdout.Write(buf)
						}

						// Track this record to be "marked" once this loop
						// is over.
						marks = append(marks, r)
						n++

						// If this pushes us to the --num flag,
						// we return. We mark any seen records so that
						// LeaveGroup will autocommit properly.
						if c.num > 0 && n >= c.num {
							c.cl.MarkCommitRecords(marks...)
							return
						}
					}
				}
			}
		}

		// Before we poll, we mark everything we just processed to be
		// available for autocommitting.
		c.cl.MarkCommitRecords(marks...)
	}
}

func (c *consumer) writeRecordJSON(r *kgo.Record) {
	type Header struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	m := struct {
		Topic     string   `json:"topic"`
		Key       string   `json:"key,omitempty"`
		Value     string   `json:"value,omitempty"`
		ValueSize *int     `json:"value_size,omitempty"` // non-nil if --meta-only
		Headers   []Header `json:"headers,omitempty"`
		Timestamp int64    `json:"timestamp"` // millis

		Partition int32 `json:"partition"`
		Offset    int64 `json:"offset"`
	}{
		Topic:     r.Topic,
		Key:       string(r.Key),
		Value:     string(r.Value),
		Headers:   make([]Header, 0, len(r.Headers)),
		Timestamp: r.Timestamp.UnixNano() / 1e6,

		Partition: r.Partition,
		Offset:    r.Offset,
	}

	if c.metaOnly {
		size := len(m.Value)
		m.Value = ""
		m.ValueSize = &size
	}

	for _, h := range r.Headers {
		m.Headers = append(m.Headers, Header{
			Key:   string(h.Key),
			Value: string(h.Value),
		})
	}

	// We are marshalling a simple type defined just above; this type
	// cannot cause a marshal error.
	var out []byte
	if c.pretty {
		out, _ = json.MarshalIndent(m, "", "  ")
	} else {
		out, _ = json.Marshal(m)
	}
	os.Stdout.Write(out)
	os.Stdout.Write(newline)
}

var newline = []byte("\n")

func (c *consumer) parseOffset(offset string) error {
	c.start, c.end = -1, -1
	o := kgo.NewOffset()
	defer func() { c.offset = o }()

	switch {
	case offset == "start" || offset == "oldest":
		o = o.AtStart()

	case offset == "end" || offset == "newest":
		o = o.AtEnd()

	case strings.HasPrefix(offset, "+"):
		v, err := strconv.Atoi(offset[1:])
		if err != nil {
			return fmt.Errorf("unable to parse relative start offset %q: %v", offset, err)
		}
		o = o.AtStart().Relative(int64(v))

	case strings.HasPrefix(offset, "-"):
		v, err := strconv.Atoi(offset[1:])
		if err != nil {
			return fmt.Errorf("unable to parse relative end offset in %q: %v", offset, err)
		}
		o = o.AtEnd().Relative(int64(-v))

	default:
		match := regexp.MustCompile(`^(\d+)(?:-(\d+))?$`).FindStringSubmatch(offset)
		if len(match) == 0 {
			return fmt.Errorf("unable to parse exact offset or range offsets in %q", offset)
		}
		c.start, _ = strconv.ParseInt(match[1], 10, 64)
		if match[2] != "" {
			c.end, _ = strconv.ParseInt(match[2], 10, 64)
		}
		o = o.At(c.start)
	}

	return nil
}

func (c *consumer) intoOptions(topics []string) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.ConsumeResetOffset(c.offset),
	}

	// We can consume either as a group, or partitions directly, not both.
	if len(c.partitions) == 0 {
		opts = append(opts, kgo.ConsumeTopics(topics...))
		if len(c.group) > 0 {
			opts = append(opts, kgo.ConsumerGroup(c.group))
			opts = append(opts, kgo.AutoCommitMarks())
		}
	} else {
		if len(c.group) != 0 {
			return nil, errors.New("invalid flags: only one of --partitions and --group can be specified")
		}
		offsets := make(map[string]map[int32]kgo.Offset)
		for _, topic := range topics {
			partOffsets := make(map[int32]kgo.Offset, len(c.partitions))
			for _, partition := range c.partitions {
				partOffsets[partition] = c.offset
			}
			offsets[topic] = partOffsets
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
	)
	if c.readCommitted {
		opts = append(opts, kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	}

	return opts, nil
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
    %v    topic
    %V    value length
    %h    begin the header specification
    %H    number of headers
    %p    partition
    %o    offset
    %e    leader epoch
    %d    timestamp (formatting described below)
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

TEXT

Text fields without modifiers default to writing the raw bytes. Alternatively,
there are the following modifiers:

    %t{hex}
    %k{base64}
    %v{unpack[<bBhH>iIqQc.$]}

The hex modifier hex encodes the text, and the base64 modifier base64 encodes
the text with standard encoding. The unpack modifier has a further internal
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

EXAMPLES

A key and value, separated by a space and ending in newline:
    -f '%k %v\n'
A key length as four big endian bytes, and the key as hex:
    -f '%K{big32}%k{hex}'
A little endian uint32 and a string unpacked from a value:
    -f '%v{unpack[is$]}'

MISC

The --offset flag allows for specifying where to begin consuming, and
optionally, where to stop consuming:

    start    consume from the beginning
    end      consume from the end
    +NNN     consume NNN after the start offset
    -NNN     consume NNN before the end offset
    N1-N2    consume from N1 to N2

If consuming a range of offsets, rpk does not currently quit after it has
consumed all partitions through the end range.
`
