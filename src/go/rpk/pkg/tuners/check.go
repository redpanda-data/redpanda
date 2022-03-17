// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package tuners

import (
	"path/filepath"
	"sort"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/redpanda"
)

func Check(
	fs afero.Fs, conf *config.Config, timeout time.Duration,
) ([]CheckResult, error) {
	var results []CheckResult
	ioConfigFile := redpanda.GetIOConfigPath(filepath.Dir(conf.ConfigFile))
	checkersMap, err := RedpandaCheckers(fs, ioConfigFile, conf, timeout)
	if err != nil {
		return results, err
	}

	for _, checkers := range checkersMap {
		for _, c := range checkers {
			result := c.Check()
			if result.Err != nil {
				if c.GetSeverity() == Fatal {
					return results, result.Err
				}
				log.Warnf("System check '%s' failed with non-fatal error '%s'", c.GetDesc(), result.Err)
			}
			log.Debugf("Checker '%s' result %+v", c.GetDesc(), result)
			results = append(results, *result)
		}
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Desc < results[j].Desc })
	return results, nil
}
