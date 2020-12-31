// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package api

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
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

// exactArgs makes sure exactly n arguments are passed, if not, a custom error
// err is returned back. This is so we can return more contextually friendly errors back
// to users.
func exactArgs(n int, err string) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		if len(args) != n {
			return fmt.Errorf(err + "\n\n" + cmd.UsageString())
		}
		return nil
	}
}
