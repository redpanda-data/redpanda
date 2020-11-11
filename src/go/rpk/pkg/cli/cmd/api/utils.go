package api

import (
	"fmt"
	"strings"
)

func parseKVs(kvs []string) (map[string]*string, error) {
	m := map[string]*string{}
	for _, s := range kvs {
		kv := strings.SplitN(s, ":", 2)
		if len(kv) != 2 {
			err := fmt.Errorf(
				"'%s' doesn't conform to the <k>:<v> format",
				s,
			)
			return m, err
		}
		key := strings.Trim(kv[0], " ")
		value := strings.Trim(kv[1], " ")
		m[key] = &value
	}
	return m, nil
}
