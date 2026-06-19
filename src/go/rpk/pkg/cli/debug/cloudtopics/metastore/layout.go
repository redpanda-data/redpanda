// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package metastore

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"

	cloudtopicsv1 "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/debug/cloudtopics/metastore/internal/cloudtopicsv1"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newLayoutCommand(fs afero.Fs, params *config.Params) *cobra.Command {
	var partitionFlag string
	cmd := &cobra.Command{
		Use:   "layout",
		Short: "Show a per-partition LSM layout summary",
		Long: `Show one row per Cloud Topics metastore partition with
its memtable sizes, total size, and per-LSM-level file counts and
total bytes.`,
		Run: func(cmd *cobra.Command, _ []string) {
			runLayout(cmd.Context(), fs, params, partitionFlag)
		},
	}
	cmd.Flags().StringVar(&partitionFlag, "partition", "all",
		"Metastore partitions to query: \"all\", a single id, a comma list, or a range (e.g. 0,2-4)")
	params.InstallFormatFlag(cmd)
	return cmd
}

func runLayout(ctx context.Context, fs afero.Fs, params *config.Params, partitionFlag string) {
	results := collectStats(ctx, fs, params, partitionFlag)

	if !params.Formatter.IsText() {
		printStructured(&params.Formatter, results)
		return
	}

	renderLayoutTable(os.Stdout, results)
}

// collectStats sets up the admin transport, expands the --partition flag,
// and fans GetDatabaseStats out to the resolved partitions. Used by both
// layout and files.
func collectStats(ctx context.Context, fs afero.Fs, params *config.Params, partitionFlag string) []partitionResult {
	t := loadAdminTransport(ctx, fs, params)
	parts, err := parsePartitionList(partitionFlag, t.NumMSParts)
	out.MaybeDie(err, "%v", err)
	return runDatabaseStatsBroadcast(ctx, t, parts)
}

// renderLayoutTable writes the per-partition summary table to w. The number of
// level columns is auto-sized to the deepest level present in any successful
// result.
func renderLayoutTable(w io.Writer, results []partitionResult) {
	headers := []string{"PART", "MEMTABLE", "IMM", "TOTAL"}
	mx := maxLevel(results)
	for L := int32(0); L <= mx; L++ {
		headers = append(headers, fmt.Sprintf("L%d#", L), fmt.Sprintf("L%d-SIZE", L))
	}
	tw := out.NewTableTo(w, headers...)
	defer tw.Flush()

	for _, r := range results {
		if r.Err != nil {
			row := []string{strconv.FormatUint(uint64(r.Partition), 10), "ERROR", "ERROR", "ERROR"}
			for L := int32(0); L <= mx; L++ {
				row = append(row, "-", "-")
			}
			tw.PrintStrings(row...)
			continue
		}
		s := r.Stats
		row := []string{
			strconv.FormatUint(uint64(r.Partition), 10),
			formatBytesIEC(s.ActiveMemtableBytes),
			formatBytesIEC(s.ImmutableMemtableBytes),
			formatBytesIEC(s.TotalSizeBytes),
		}
		byLevel := levelBytesByNumber(s.Levels)
		for L := int32(0); L <= mx; L++ {
			lf, ok := byLevel[L]
			if !ok {
				row = append(row, "0", "0 B")
				continue
			}
			row = append(row, strconv.Itoa(lf.count), formatBytesIEC(lf.size))
		}
		tw.PrintStrings(row...)
	}

	// Footer: per-partition error messages.
	var hasErr bool
	for _, r := range results {
		if r.Err != nil {
			hasErr = true
			break
		}
	}
	if hasErr {
		tw.Flush()
		fmt.Fprintln(w)
		for _, r := range results {
			if r.Err != nil {
				fmt.Fprintf(w, "partition %d: %v\n", r.Partition, r.Err)
			}
		}
	}
}

type levelAgg struct {
	count int
	size  uint64
}

func levelBytesByNumber(levels []*cloudtopicsv1.LsmLevel) map[int32]levelAgg {
	m := map[int32]levelAgg{}
	for _, l := range levels {
		var sum uint64
		for _, f := range l.Files {
			sum += f.SizeBytes
		}
		m[l.LevelNumber] = levelAgg{count: len(l.Files), size: sum}
	}
	return m
}

func maxLevel(results []partitionResult) int32 {
	mx := int32(-1)
	for _, r := range results {
		if r.Stats == nil {
			continue
		}
		for _, l := range r.Stats.Levels {
			if l.LevelNumber > mx {
				mx = l.LevelNumber
			}
		}
	}
	return mx
}

// formatBytesIEC renders a byte count using IEC units (KiB, MiB, GiB...).
// One decimal place below 10, no decimals above.
func formatBytesIEC(n uint64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	units := []string{"KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}
	div, exp := uint64(unit), 0
	for x := n / unit; x >= unit && exp < len(units)-1; x /= unit {
		div *= unit
		exp++
	}
	val := float64(n) / float64(div)
	if val < 10 {
		return fmt.Sprintf("%.1f %s", val, units[exp])
	}
	return fmt.Sprintf("%.0f %s", val, units[exp])
}

// printStructured is the --format json|yaml path shared by layout and files.
func printStructured(f *config.OutFormatter, results []partitionResult) {
	// Byte counts are emitted as JSON numbers. The metastore is unlikely to
	// report sizes beyond 2^53, but consumers that need exact large uint64s
	// should prefer the proto's string-encoded representation.
	type outFile struct {
		Epoch           uint64 `json:"epoch" yaml:"epoch"`
		ID              uint64 `json:"id" yaml:"id"`
		SizeBytes       uint64 `json:"sizeBytes" yaml:"size_bytes"`
		SmallestKeyInfo string `json:"smallestKeyInfo" yaml:"smallest_key_info"`
		LargestKeyInfo  string `json:"largestKeyInfo" yaml:"largest_key_info"`
	}
	type outLevel struct {
		LevelNumber int32     `json:"levelNumber" yaml:"level_number"`
		Files       []outFile `json:"files" yaml:"files"`
	}
	type outPartition struct {
		Partition              uint32     `json:"partition" yaml:"partition"`
		Error                  string     `json:"error,omitempty" yaml:"error,omitempty"`
		ActiveMemtableBytes    uint64     `json:"activeMemtableBytes" yaml:"active_memtable_bytes"`
		ImmutableMemtableBytes uint64     `json:"immutableMemtableBytes" yaml:"immutable_memtable_bytes"`
		TotalSizeBytes         uint64     `json:"totalSizeBytes" yaml:"total_size_bytes"`
		Levels                 []outLevel `json:"levels,omitempty" yaml:"levels,omitempty"`
	}

	items := make([]outPartition, 0, len(results))
	for _, r := range results {
		item := outPartition{Partition: r.Partition}
		if r.Err != nil {
			item.Error = r.Err.Error()
		} else if r.Stats != nil {
			item.ActiveMemtableBytes = r.Stats.ActiveMemtableBytes
			item.ImmutableMemtableBytes = r.Stats.ImmutableMemtableBytes
			item.TotalSizeBytes = r.Stats.TotalSizeBytes
			for _, l := range r.Stats.Levels {
				ol := outLevel{LevelNumber: l.LevelNumber}
				for _, fl := range l.Files {
					ol.Files = append(ol.Files, outFile{
						Epoch:           fl.Epoch,
						ID:              fl.Id,
						SizeBytes:       fl.SizeBytes,
						SmallestKeyInfo: fl.SmallestKeyInfo,
						LargestKeyInfo:  fl.LargestKeyInfo,
					})
				}
				item.Levels = append(item.Levels, ol)
			}
		}
		items = append(items, item)
	}
	isShort, _, formatted, err := f.Format(items)
	out.MaybeDie(err, "unable to print in the requested format %q: %v", f.Kind, err)
	if !isShort {
		fmt.Println(formatted)
	}
}
