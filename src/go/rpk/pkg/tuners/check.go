package tuners

import (
	"path/filepath"
	"sort"
	"time"
	"vectorized/pkg/config"
	"vectorized/pkg/redpanda"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

func Check(
	fs afero.Fs,
	configFile string,
	conf *config.Config,
	timeout time.Duration,
) ([]CheckResult, error) {
	var results []CheckResult
	ioConfigFile := redpanda.GetIOConfigPath(filepath.Dir(configFile))
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
