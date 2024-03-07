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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestTimeQueryFlag(t *testing.T) {
	// Reference time:
	// 1648021901749
	// 2022-03-23 07:51:41.749796309 +0000 UTC
	referenceTime := time.UnixMilli(1648021901749).UTC()
	testcases := map[string]time.Time{
		"1648021901749":            referenceTime,
		"1648021901":               referenceTime.Truncate(time.Second),
		"2022-03-23":               time.Date(2022, 0o3, 23, 0, 0, 0, 0, time.UTC),
		"2022-03-23T07:51:41.749Z": referenceTime,
		"2022-03-23T07:51:41Z":     referenceTime.Truncate(time.Second),
		"2022-03-23T07:51:41.Z":    referenceTime.Truncate(time.Second),
		"-48h":                     referenceTime.Add(-48 * time.Hour),
		"-3m":                      referenceTime.Add(-3 * time.Minute),
		"7m":                       referenceTime.Add(7 * time.Minute),
		"now":                      referenceTime,
	}
	for str, want := range testcases {
		got, err := parseTimeQuery(str, referenceTime)
		require.NoError(t, err, "unable to parse time query")
		require.Equal(t, got, want, "string=%q", str)
	}
	errors := []string{
		"",
		"2d",
		"-2d",
		"2022-03-23T07:51:41.",
		"2022-03-23T07:51:41",
		"2022-03-23T07:51:41.749",
		"-",
		"-2",
	}
	for _, str := range errors {
		_, err := parseTimeQuery(str, referenceTime)
		require.Error(t, err)
	}
}

func TestTailWriter(t *testing.T) {
	buf := &bytes.Buffer{}
	tw := &tailWriter{w: buf, limit: 10}
	for i := 1; i < 99; i += 1 {
		_, err := fmt.Fprintf(tw, "%d ", i)
		require.NoError(t, err)
	}
	require.NoError(t, tw.Close())
	require.Equal(t, "89 90 91 92 93 94 95 96 97 98 ", buf.String())
}

type logOutput struct {
	warn bool
	body string
}

func info(body string) logOutput {
	return logOutput{warn: false, body: body}
}

func warn(body string) logOutput {
	return logOutput{warn: true, body: body}
}

type testcase struct {
	query logsQuery
	input []logOutput
	short string
	wide  string
}

type testLogSource struct {
	logs   []logOutput
	polled bool
}

func (s *testLogSource) PollFetches(context.Context) (RecordIter, error) {
	if s.polled {
		return nil, kgo.ErrClientClosed
	}
	s.polled = true
	return &testRecordIter{logs: s.logs, ts: time.UnixMilli(0)}, nil
}

type testRecordIter struct {
	current int
	logs    []logOutput
	ts      time.Time
}

func (it *testRecordIter) Done() bool {
	return it.current >= len(it.logs)
}

func (it *testRecordIter) Next() *kgo.Record {
	log := it.logs[it.current]
	raw := rawLogEvent{
		TimeUnixNano:   uint64(it.ts.UnixNano()),
		SeverityNumber: 9,
	}
	raw.Body.StringValue = log.body
	if log.warn {
		raw.SeverityNumber = 13
	}
	val, err := json.Marshal(&raw)
	if err != nil {
		panic(err)
	}
	r := kgo.Record{
		Key:       []byte("foo"),
		Value:     val,
		Timestamp: it.ts,
		Offset:    int64(it.current),
	}
	it.current += 1
	it.ts = it.ts.Add(time.Second)
	return &r
}

func TestLogPrinting(t *testing.T) {
	testcases := []testcase{
		{
			input: []logOutput{
				info("foo\n"),
				warn("bar\n"),
				info("baz\n"),
			},
			short: `foo
bar
baz
`,
			wide: `Jan 01 00:00:00 INFO foo
Jan 01 00:00:01 WARN bar
Jan 01 00:00:02 INFO baz
`,
		},
		{
			input: []logOutput{
				info("foo"),
				info("bar\n"),
				info("baz"),
			},
			short: `foobar
baz
`,
			wide: `Jan 01 00:00:01 INFO foobar
Jan 01 00:00:02 INFO baz
`,
		},
		{
			query: logsQuery{
				maxOffset: 2,
			},
			input: []logOutput{
				info("foo"),
				info("bar"),
			},
			short: `foobar
`,
			wide: `Jan 01 00:00:01 INFO foobar
`,
		},
		{
			input: []logOutput{
				info("foo\nbar\nbaz\nqux\n"),
			},
			short: `foo
bar
baz
qux
`,
			wide: `Jan 01 00:00:00 INFO foo
Jan 01 00:00:00 INFO bar
Jan 01 00:00:00 INFO baz
Jan 01 00:00:00 INFO qux
`,
		},
		{
			query: logsQuery{
				// Name does not match
				transformName: "bar",
			},
			input: []logOutput{
				warn("boom"),
				warn("bam"),
				warn("wham"),
			},
			short: ``,
			wide:  ``,
		},
	}
	for _, tc := range testcases {
		// normalize the query so the test cases are less verbose
		if tc.query.transformName == "" {
			tc.query.transformName = "foo"
		}
		if tc.query.maxOffset == 0 {
			tc.query.maxOffset = math.MaxInt64
		}
		buf := &bytes.Buffer{}
		err := printLogs(context.Background(), &testLogSource{logs: tc.input}, tc.query, buf, config.OutFormatter{Kind: "short"})
		require.NoError(t, err)
		require.Equal(t, tc.short, buf.String(), "short output")

		buf.Reset()
		err = printLogs(context.Background(), &testLogSource{logs: tc.input}, tc.query, buf, config.OutFormatter{Kind: "wide"})
		require.NoError(t, err)
		require.Equal(t, tc.wide, buf.String(), "wide output")
	}
}
