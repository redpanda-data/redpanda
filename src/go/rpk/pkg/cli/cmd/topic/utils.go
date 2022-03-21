// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package topic

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
)

func parseKVs(in []string) (map[string]string, error) {
	kvs := make(map[string]string)
	for _, pair := range in {
		colon := strings.IndexByte(pair, ':')
		equal := strings.IndexByte(pair, '=')

		delim := "="
		if colon != -1 {
			if equal == -1 || colon < equal {
				delim = ":"
			}
		}

		kv := strings.SplitN(pair, delim, 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("unable to find key=value pair in %q", pair)
		}

		k := strings.TrimSpace(kv[0])
		v := strings.TrimSpace(kv[1])

		if len(k) == 0 {
			return nil, fmt.Errorf("empty key in pair %q", pair)
		}
		if len(v) == 0 {
			return nil, fmt.Errorf("empty value in pair %q", pair)
		}
		kvs[k] = v
	}
	return kvs, nil
}

func regexTopics(adm *kadm.Client, expressions []string) ([]string, error) {
	// Now we list all topics to match against our expressions.
	topics, err := adm.ListTopics(context.Background())
	if err != nil {
		return nil, fmt.Errorf("unable to list topics: %w", err)
	}

	return regexListedTopics(topics.Names(), expressions)
}

func regexListedTopics(topics, expressions []string) ([]string, error) {
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
		remaining := topics[:0]
		for _, t := range topics {
			if re.MatchString(t) {
				matched = append(matched, t)
			} else {
				remaining = append(remaining, t)
			}
		}
		topics = remaining
		if len(topics) == 0 {
			break
		}
	}
	return matched, nil
}
