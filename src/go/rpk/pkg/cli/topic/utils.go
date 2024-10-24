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
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
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
	topics, err := adm.ListTopicsWithInternal(context.Background())
	if err != nil {
		return nil, fmt.Errorf("unable to list topics: %w", err)
	}

	return utils.RegexListedItems(topics.Names(), expressions)
}
