// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package topic

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
