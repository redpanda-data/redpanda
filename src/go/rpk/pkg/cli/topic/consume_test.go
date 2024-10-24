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
		in               string
		bothDuration     bool
		length           int
		startAt          time.Time
		endAt            time.Time
		end              bool
		expErrFirstHalf  bool
		expErrSecondHalf bool
	}{
		{ // 0
			in:              "",
			expErrFirstHalf: true,
		},

		{ // 1. 13 digits:13 digits
			in:      "1648021901749:1748021901749",
			length:  13,
			startAt: time.Unix(0, 1648021901749*1e6).UTC(),
			endAt:   time.Unix(0, 1748021901749*1e6).UTC(),
		},

		{ // 2. 10 digits:10 digits
			in:      "1648021901:1748021901",
			length:  10,
			startAt: time.Unix(1648021901, 0).UTC(),
			endAt:   time.Unix(1748021901, 0).UTC(),
		},

		{ // 3. date:date
			in:      "2022-03-24:2022-03-25",
			length:  10,
			startAt: time.Unix(1648080000, 0).UTC(),
			endAt:   time.Unix(1648080000, 0).Add(24 * time.Hour).UTC(),
		},

		{ // 4. full millis:full millis
			in:      "2022-03-23T07:51:41.749Z:2022-03-23T07:52:41.749Z",
			length:  24,
			startAt: time.Unix(0, 1648021901749*1e6).UTC(),
			endAt:   time.Unix(0, 1648021901749*1e6).Add(1 * time.Minute).UTC(),
		},

		{ // 5. no milli:no milli
			in:      "2022-03-23T07:51:41Z:2022-03-23T07:52:41Z",
			length:  20,
			startAt: time.Unix(0, 1648021901000*1e6).UTC(),
			endAt:   time.Unix(0, 1648021901000*1e6).Add(1 * time.Minute).UTC(),
		},

		{ // 6. one decimal:one decimal
			in:      "2022-03-23T07:51:41.7Z:2022-03-23T07:52:41.7Z",
			length:  22,
			startAt: time.Unix(0, 1648021901700*1e6),
			endAt:   time.Unix(0, 1648021901700*1e6).Add(1 * time.Minute).UTC(),
		},

		{ // 7. just decimal
			in:      "2022-03-23T07:51:41.Z",
			length:  21,
			startAt: time.Unix(0, 1648021901000*1e6),
		},

		{ // 8. just decimal:duration
			in:      "2022-03-23T07:51:41.Z:1h",
			length:  21,
			startAt: time.Unix(0, 1648021901000*1e6).UTC(),
			endAt:   time.Unix(0, 1648021901000*1e6).Add(1 * time.Hour).UTC(),
		},

		{ // 9. duration:duration
			in:           "-48h:-24h",
			bothDuration: true,
			length:       4,
			startAt:      time.Now().Add(-48 * time.Hour).UTC(),
			endAt:        time.Now().Add(-24 * time.Hour).UTC(),
		},

		{ // 10 error
			in:              "-03-23T07:51:41.Z",
			expErrFirstHalf: true,
		},

		{ // 11
			in:     "end",
			length: 3,
			end:    true,
		},

		{ // 12
			in:      "3h:",
			length:  2,
			startAt: time.Now().Add(3 * time.Hour).UTC(),
		},

		{ // 13
			in:      "-3m",
			length:  3,
			startAt: time.Now().Add(-3 * time.Minute).UTC(),
		},
		{ // 14 error
			in:               "1648021901749:asdf",
			length:           13,
			startAt:          time.Unix(0, 1648021901749*1e6),
			expErrSecondHalf: true,
		},
	} {
		var (
			l             int
			offset        string
			startAt       time.Time
			endAt         time.Time
			end           bool
			fromTimestamp bool
			err           error
			gotErr        bool
		)
		test.startAt = test.startAt.UTC()
		l, startAt, end, fromTimestamp, err = parseTimestampBasedOffset(test.in, time.Now())

		gotErr = err != nil
		if gotErr != test.expErrFirstHalf {
			t.Errorf("#%d: got err? %v (%v), exp err? %v", i, gotErr, err, test.expErrFirstHalf)
		}
		if gotErr || test.expErrFirstHalf {
			continue
		}

		if l != test.length {
			t.Errorf("#%d: got length %d != exp length %d", i, l, test.length)
		}

		// For timestamps, we use a 1min bounds because we could have
		// relative durations from now.
		if startAt.Before(test.startAt) && test.startAt.Sub(startAt) > time.Minute ||
			test.startAt.Before(startAt) && startAt.Sub(test.startAt) > time.Minute {
			t.Errorf("#%d: got at %v not within 1min of %v", i, startAt, test.startAt)
		}

		if end != test.end {
			t.Errorf("#%d: got end %v != exp end %v", i, end, test.end)
		}

		offset = test.in[l:]
		if offset == "" || offset == ":" { // requesting from a time onward; e.g., @1m:
			continue
		}

		if fromTimestamp {
			_, endAt, _, _, err = parseTimestampBasedOffset(offset[1:], startAt)
		} else {
			_, endAt, _, _, err = parseTimestampBasedOffset(offset[1:], time.Now())
		}

		gotErr = err != nil
		if gotErr != test.expErrSecondHalf {
			t.Errorf("#%d: got err? %v (%v), exp err? %v", i, gotErr, err, test.expErrSecondHalf)
		}
		if gotErr || test.expErrSecondHalf {
			continue
		}

		if test.bothDuration {
			if startAt.Sub(test.startAt) > time.Second {
				t.Errorf("#%d: got startAt %s != exp startAt %s\n", i, startAt, test.startAt)
			}
			if endAt.Sub(test.endAt) > time.Second {
				t.Errorf("#%d: got endAt %s != exp endAt %s\n", i, endAt, test.endAt)
			}
		} else if !endAt.Equal(test.endAt) {
			t.Errorf("#%d: got endAt %s != exp endAt %s\n", i, endAt, test.endAt)
		}
	}
}
