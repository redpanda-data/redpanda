// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package utils

import (
	"fmt"
	"regexp"
	"strings"
)

// RegexListedItems returns items that match the 'expressions' from the 'list'.
func RegexListedItems(list, expressions []string) ([]string, error) {
	var compiled []*regexp.Regexp
	for _, expression := range expressions {
		if !strings.HasPrefix(expression, "^") {
			expression = "^" + expression
		}
		if !strings.HasSuffix(expression, "$") {
			expression += "$"
		}
		re, err := regexp.Compile(expression)
		if err != nil {
			return nil, fmt.Errorf("unable to compile regex %q: %w", expression, err)
		}
		compiled = append(compiled, re)
	}

	var matched []string
	for _, re := range compiled {
		remaining := list[:0]
		for _, item := range list {
			if re.MatchString(item) {
				matched = append(matched, item)
			} else {
				remaining = append(remaining, item)
			}
		}
		list = remaining
		if len(list) == 0 {
			break
		}
	}
	return matched, nil
}
