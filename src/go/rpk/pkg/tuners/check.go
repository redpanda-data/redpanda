// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package tuners

import (
	"fmt"
	"path/filepath"
	"sort"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/redpanda"
	"github.com/spf13/afero"
	"go.uber.org/zap"
)

func Check(
	fs afero.Fs, conf *config.Config, timeout time.Duration,
) ([]CheckResult, error) {
	var results []CheckResult
	ioConfigFile := redpanda.GetIOConfigPath(filepath.Dir(conf.FileLocation()))
	checkersMap, err := RedpandaCheckers(fs, ioConfigFile, conf, timeout)
	if err != nil {
		return results, err
	}

	// We use a sorted list of the checker's ID present in the checkersMap to
	// run in a consistent order.
	var ids []int
	for id := range checkersMap {
		ids = append(ids, int(id))
	}
	sort.Ints(ids)

	for _, id := range ids {
		checkers := checkersMap[CheckerID(id)]
		for _, c := range checkers {
			zap.L().Sugar().Debugf("Starting checker %q", c.GetDesc())
			result := c.Check()
			if result.Err != nil {
				if c.GetSeverity() == Fatal {
					return results, fmt.Errorf("fatal error during checker %q execution: %v", c.GetDesc(), result.Err)
				}
				fmt.Printf("System check %q failed with non-fatal error %q\n", c.GetDesc(), result.Err)
			}
			zap.L().Sugar().Debugf("Finished checker %q; result %+v", c.GetDesc(), result)
			results = append(results, *result)
		}
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Desc < results[j].Desc })
	return results, nil
}
