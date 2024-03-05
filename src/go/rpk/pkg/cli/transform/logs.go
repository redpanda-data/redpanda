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
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"
)

func newLogsCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var follow bool
	var head, tail int
	var since, until timequery
	since.offset = kgo.NewOffset().AtStart()
	since.offset = kgo.NewOffset().At(math.MaxInt64)
	cmd := &cobra.Command{
		Use:     "logs [NAME]",
		Aliases: []string{"log"},
		Short:   "View logs for a transform",
		Args:    cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			_, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "unable to load config: %v", err)
			// TODO(rockwood): Implement the command
		},
	}
	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "Specify if the logs should be streamed")
	cmd.Flags().IntVar(&head, "head", 0, "The number of log entries to fetch from the start")
	cmd.Flags().IntVar(&tail, "tail", 0, "The number of log entries to fetch from the end")
	cmd.MarkFlagsMutuallyExclusive("head", "tail")
	cmd.Flags().Var(&since, "since", "Start reading logs after this time")
	cmd.Flags().Var(&until, "until", "Read logs up unto this time")
	p.InstallFormatFlag(cmd)
	return cmd
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
	return tq.time.String()
}

func parseTimeQuery(s string, now time.Time) (time.Time, error) {
	switch {
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
			return kgo.NewOffset(), fmt.Errorf("unable to parse duration in %q", s)
		}
		if negate {
			rel = -rel
		}
		at := now.Add(rel).UTC()
		return kgo.NewOffset().AfterMilli(at.UnixMilli()), nil
	}
}
