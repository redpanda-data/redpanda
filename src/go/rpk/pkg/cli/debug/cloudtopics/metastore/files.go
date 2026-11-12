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
	"sort"
	"strconv"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newFilesCommand(fs afero.Fs, params *config.Params) *cobra.Command {
	var partitionFlag, levelFlag string
	cmd := &cobra.Command{
		Use:   "files",
		Short: "List SST files across metastore partitions",
		Long: `List every SST file (LsmFile) across the queried metastore
partitions, sorted by partition, level, epoch (descending), and id.`,
		Run: func(cmd *cobra.Command, _ []string) {
			levelSet, err := parseLevelList(levelFlag)
			out.MaybeDie(err, "%v", err)
			runFiles(cmd.Context(), fs, params, partitionFlag, levelSet)
		},
	}
	cmd.Flags().StringVar(&partitionFlag, "partition", "all",
		"Metastore partitions to query: \"all\", a single id, a comma list, or a range")
	cmd.Flags().StringVar(&levelFlag, "level", "all",
		"LSM levels to include: \"all\", a single level, a comma list, or a range")
	params.InstallFormatFlag(cmd)
	return cmd
}

func runFiles(ctx context.Context, fs afero.Fs, params *config.Params, partitionFlag string, levelSet map[int32]bool) {
	results := collectStats(ctx, fs, params, partitionFlag)

	if !params.Formatter.IsText() {
		if levelSet != nil {
			for i := range results {
				if results[i].Stats == nil {
					continue
				}
				filtered := results[i].Stats.Levels[:0]
				for _, l := range results[i].Stats.Levels {
					if levelSet[l.LevelNumber] {
						filtered = append(filtered, l)
					}
				}
				results[i].Stats.Levels = filtered
			}
		}
		printStructured(&params.Formatter, results)
		return
	}

	renderFilesTable(os.Stdout, results, levelSet)
}

// renderFilesTable writes one row per SST file. levelSet=nil means no filter.
// Sort order: PART asc, LEVEL asc, EPOCH desc, ID asc.
func renderFilesTable(w io.Writer, results []partitionResult, levelSet map[int32]bool) {
	type row struct {
		part, level int32
		epoch, id   uint64
		size        uint64
		smallest    string
		largest     string
	}

	var rows []row
	for _, r := range results {
		if r.Stats == nil {
			continue
		}
		for _, l := range r.Stats.Levels {
			if levelSet != nil && !levelSet[l.LevelNumber] {
				continue
			}
			for _, fl := range l.Files {
				rows = append(rows, row{
					part:     int32(r.Partition),
					level:    l.LevelNumber,
					epoch:    fl.Epoch,
					id:       fl.Id,
					size:     fl.SizeBytes,
					smallest: fl.SmallestKeyInfo,
					largest:  fl.LargestKeyInfo,
				})
			}
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].part != rows[j].part {
			return rows[i].part < rows[j].part
		}
		if rows[i].level != rows[j].level {
			return rows[i].level < rows[j].level
		}
		if rows[i].epoch != rows[j].epoch {
			return rows[i].epoch > rows[j].epoch
		}
		return rows[i].id < rows[j].id
	})

	tw := out.NewTableTo(w, "PART", "LEVEL", "EPOCH", "ID", "SIZE", "SMALLEST", "LARGEST")
	defer tw.Flush()
	for _, r := range rows {
		tw.PrintStrings(
			strconv.FormatInt(int64(r.part), 10),
			fmt.Sprintf("L%d", r.level),
			strconv.FormatUint(r.epoch, 10),
			strconv.FormatUint(r.id, 10),
			formatBytesIEC(r.size),
			r.smallest,
			r.largest,
		)
	}

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
