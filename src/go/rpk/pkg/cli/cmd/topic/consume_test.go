package topic

import (
	"testing"
	"time"
)

func TestParseFromToOffset(t *testing.T) {
	for i, test := range []struct {
		in string

		start      int64
		end        int64
		rel        int64
		atStart    bool
		atEnd      bool
		hasEnd     bool
		currentEnd bool
		expErr     bool
	}{
		{
			expErr: true,
		},

		{
			in:      "start",
			atStart: true,
		},

		{
			in:      "oldest",
			atStart: true,
		},

		{
			in:    "end",
			atEnd: true,
		},

		{
			in:    "newest",
			atEnd: true,
		},

		{
			in:         ":end",
			atStart:    true,
			currentEnd: true,
		},

		{
			in:      "+3",
			rel:     3,
			atStart: true,
		},

		{
			in:     "+a",
			expErr: true,
		},

		{
			in:    "-3",
			rel:   -3,
			atEnd: true,
		},

		{
			in:     "-a",
			expErr: true,
		},

		{
			in:    "35",
			start: 35,
		},

		{
			in:    "35:",
			start: 35,
		},

		{
			in:      ":23",
			end:     23,
			atStart: true,
			hasEnd:  true,
		},

		{
			in:     "90:91",
			start:  90,
			end:    91,
			hasEnd: true,
		},

		{
			in:     "90-91",
			start:  90,
			end:    91,
			hasEnd: true,
		},

		{
			in:         "90:end",
			start:      90,
			currentEnd: true,
		},

		{
			in:         "90-end",
			start:      90,
			currentEnd: true,
		},

		{
			in:     "90:39",
			expErr: true,
		},

		{
			in:     "90:a",
			expErr: true,
		},
	} {
		start, end, rel, atStart, atEnd, hasEnd, currentEnd, err := parseFromToOffset(test.in)
		gotErr := err != nil

		if gotErr != test.expErr {
			t.Errorf("#%d: got err? %v (%v), exp err? %v", i, gotErr, err, test.expErr)
		}
		if gotErr || test.expErr {
			continue
		}

		if start != test.start {
			t.Errorf("#%d: got start? %v, exp start? %v", i, start, test.start)
		}
		if end != test.end {
			t.Errorf("#%d: got end? %v, exp end? %v", i, end, test.end)
		}
		if rel != test.rel {
			t.Errorf("#%d: got rel? %v, exp rel? %v", i, rel, test.rel)
		}
		if atStart != test.atStart {
			t.Errorf("#%d: got atStart? %v, exp atStart? %v", i, atStart, test.atStart)
		}
		if atEnd != test.atEnd {
			t.Errorf("#%d: got atEnd? %v, exp atEnd? %v", i, atEnd, test.atEnd)
		}
		if hasEnd != test.hasEnd {
			t.Errorf("#%d: got hasEnd? %v, exp hasEnd? %v", i, hasEnd, test.hasEnd)
		}
		if currentEnd != test.currentEnd {
			t.Errorf("#%d: got currentEnd? %v, exp currentEnd? %v", i, currentEnd, test.currentEnd)
		}
	}
}

func TestParseConsumeTimestamp(t *testing.T) {
	// Reference time:
	// 1648021901749
	// 2022-03-23 07:51:41.749796309 +0000 UTC
	for i, test := range []struct {
		in string

		length int
		at     time.Time
		end    bool
		expErr bool
	}{
		{
			in:     "",
			expErr: true,
		},

		{
			in:     "1648021901749:asdf",
			length: 13,
			at:     time.Unix(0, 1648021901749*1e6),
		},

		{
			in:     "1648021901:asdf",
			length: 10,
			at:     time.Unix(1648021901, 0),
		},

		{
			in:     "2022-03-24:asdf",
			length: 10,
			at:     time.Unix(1648080000, 0),
		},

		{ // full millis
			in:     "2022-03-23T07:51:41.749Z:",
			length: 24,
			at:     time.Unix(0, 1648021901749*1e6),
		},

		{ // no milli
			in:     "2022-03-23T07:51:41Z",
			length: 20,
			at:     time.Unix(0, 1648021901000*1e6),
		},

		{ // one decimal
			in:     "2022-03-23T07:51:41.7Z:asdf",
			length: 22,
			at:     time.Unix(0, 1648021901700*1e6),
		},

		{ // just decimal
			in:     "2022-03-23T07:51:41.Z",
			length: 21,
			at:     time.Unix(0, 1648021901000*1e6),
		},

		{ // err
			in:     "-03-23T07:51:41.Z",
			expErr: true,
		},

		{
			in:     "end",
			length: 3,
			end:    true,
		},

		{
			in:     "3h:",
			length: 2,
			at:     time.Now().Add(3 * time.Hour),
		},

		{
			in:     "-3m",
			length: 3,
			at:     time.Now().Add(-3 * time.Minute),
		},
	} {
		test.at = test.at.UTC()
		l, at, end, err := parseConsumeTimestamp(test.in)

		gotErr := err != nil
		if gotErr != test.expErr {
			t.Errorf("#%d: got err? %v (%v), exp err? %v", i, gotErr, err, test.expErr)
		}
		if gotErr || test.expErr {
			continue
		}

		if l != test.length {
			t.Errorf("#%d: got length %d != exp length %d", i, l, test.length)
		}

		// For timestamps, we use a 1min bounds because we could have
		// relative durations from now.
		if at.Before(test.at) && test.at.Sub(at) > time.Minute ||
			test.at.Before(at) && at.Sub(test.at) > time.Minute {
			t.Errorf("#%d: got at %v not within 1min of %v", i, at, test.at)
		}

		if end != test.end {
			t.Errorf("#%d: got end %v != exp end %v", i, end, test.end)
		}
	}
}
