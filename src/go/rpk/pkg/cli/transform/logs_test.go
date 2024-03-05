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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func offset(t time.Time) kgo.Offset {
	return kgo.NewOffset().AfterMilli(t.UTC().UnixMilli())
}

func TestTimeQueryFlag(t *testing.T) {
	// Reference time:
	// 1648021901749
	// 2022-03-23 07:51:41.749796309 +0000 UTC
	referenceTime := time.UnixMilli(1648021901749).UTC()
	testcases := map[string]time.Time{
		"1648021901749":            referenceTime,
		"1648021901":               referenceTime.Truncate(time.Second),
		"2022-03-23":               time.Date(2022, 03, 23, 0, 0, 0, 0, time.UTC),
		"2022-03-23T07:51:41.749Z": referenceTime,
		"2022-03-23T07:51:41Z":     referenceTime.Truncate(time.Second),
		"2022-03-23T07:51:41.Z":    referenceTime.Truncate(time.Second),
		"-48h":                     referenceTime.Add(-48 * time.Hour),
		"-3m":                      referenceTime.Add(-3 * time.Minute),
		"7m":                       referenceTime.Add(7 * time.Minute),
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
