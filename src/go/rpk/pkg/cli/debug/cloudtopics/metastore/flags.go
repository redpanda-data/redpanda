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
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// parsePartitionList resolves the --partition flag into a sorted, deduplicated
// list of partition IDs. The input accepts "all", "" (treated as "all"), a
// single index ("0"), a comma list ("0,3"), a range ("1-4"), or any mix.
// count is the total number of metastore partitions; entries are bounds-checked
// against [0, count).
func parsePartitionList(in string, count uint32) ([]uint32, error) {
	trimmed := strings.TrimSpace(in)
	if trimmed == "" || trimmed == "all" {
		out := make([]uint32, count)
		for i := range out {
			out[i] = uint32(i)
		}
		return out, nil
	}
	seen := map[uint32]struct{}{}
	for _, raw := range strings.Split(trimmed, ",") {
		tok := strings.TrimSpace(raw)
		if tok == "" {
			return nil, fmt.Errorf("empty partition token in %q", in)
		}
		if strings.Contains(tok, "-") {
			parts := strings.SplitN(tok, "-", 2)
			lo, errLo := strconv.ParseUint(strings.TrimSpace(parts[0]), 10, 32)
			hi, errHi := strconv.ParseUint(strings.TrimSpace(parts[1]), 10, 32)
			if errLo != nil || errHi != nil {
				return nil, fmt.Errorf("invalid partition range %q", tok)
			}
			if lo > hi {
				return nil, fmt.Errorf("invalid partition range %q: start > end", tok)
			}
			if uint32(lo) >= count {
				return nil, fmt.Errorf("partition %d out of range [0, %d)", lo, count)
			}
			if uint32(hi) >= count {
				return nil, fmt.Errorf("partition %d out of range [0, %d)", hi, count)
			}
			for i := lo; i <= hi; i++ {
				seen[uint32(i)] = struct{}{}
			}
			continue
		}
		n, err := strconv.ParseUint(tok, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid partition token %q", tok)
		}
		if uint32(n) >= count {
			return nil, fmt.Errorf("partition %d out of range [0, %d)", n, count)
		}
		seen[uint32(n)] = struct{}{}
	}
	out := make([]uint32, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// parseLevelList resolves the --level flag into a set of LSM level numbers.
// Returns nil for "all" / "" (no filter). LSM levels are non-negative.
func parseLevelList(in string) (map[int32]bool, error) {
	trimmed := strings.TrimSpace(in)
	if trimmed == "" || trimmed == "all" {
		return nil, nil
	}
	out := map[int32]bool{}
	for _, raw := range strings.Split(trimmed, ",") {
		tok := strings.TrimSpace(raw)
		if tok == "" {
			return nil, fmt.Errorf("empty level token in %q", in)
		}
		if strings.Contains(tok, "-") {
			parts := strings.SplitN(tok, "-", 2)
			loS, hiS := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
			if loS == "" || hiS == "" {
				return nil, fmt.Errorf("invalid level token %q", tok)
			}
			lo, errLo := strconv.ParseUint(loS, 10, 31)
			hi, errHi := strconv.ParseUint(hiS, 10, 31)
			if errLo != nil || errHi != nil {
				return nil, fmt.Errorf("invalid level range %q", tok)
			}
			if lo > hi {
				return nil, fmt.Errorf("invalid level range %q: start > end", tok)
			}
			for i := lo; i <= hi; i++ {
				out[int32(i)] = true
			}
			continue
		}
		n, err := strconv.ParseUint(tok, 10, 31)
		if err != nil {
			return nil, fmt.Errorf("invalid level token %q", tok)
		}
		out[int32(n)] = true
	}
	return out, nil
}
