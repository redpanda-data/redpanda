// Copyright 2026 Redpanda Data, Inc.
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

	"github.com/twmb/franz-go/pkg/kadm"
)

// Helpers for resolving the Schema Registry context a topic's schemas live in. Shared by
// `rpk topic produce` (encoding) and `rpk topic consume` (decoding): both resolve schemas in
// the context bound to the topic so ids are looked up in the same namespace the broker uses.

// topicSchemaRegistryContextConfig is the topic config that binds a topic to a Schema
// Registry context, used for per-topic schema resolution (e.g. by the Iceberg translator).
const topicSchemaRegistryContextConfig = "redpanda.schema.registry.context"

// topicConfigSchemaContext returns the value of the redpanda.schema.registry.context topic
// config from the given configs, or "" if it is unset.
func topicConfigSchemaContext(configs []kadm.Config) string {
	for _, c := range configs {
		if c.Key == topicSchemaRegistryContextConfig {
			return c.MaybeValue()
		}
	}
	return ""
}

// schemaContextForTopic fetches the topic's configured Schema Registry context via
// DescribeTopicConfigs. It returns ("", nil) when the topic has no context configured, and a
// non-nil error when the config could not be determined so the caller can decide whether to
// bail rather than silently falling back to the default context (and possibly the wrong schema).
func schemaContextForTopic(ctx context.Context, adm *kadm.Client, topic string) (string, error) {
	if topic == "" {
		return "", nil
	}
	rcs, err := adm.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		return "", err
	}
	rc, err := rcs.On(topic, nil)
	if err != nil {
		return "", err
	}
	if rc.Err != nil {
		return "", rc.Err
	}
	return topicConfigSchemaContext(rc.Configs), nil
}

// effectiveSchemaContext chooses the Schema Registry context for a topic: the explicit flag
// value when the user set --schema-context (an empty value forces the default context),
// otherwise the topic's configured context.
func effectiveSchemaContext(flagChanged bool, flagValue, topicContext string) string {
	ctx := topicContext
	if flagChanged {
		ctx = flagValue
	}
	// "." names the default (root) context, same as an empty value. Normalize it to "" so the
	// SR client omits the /contexts/ prefix: passing "." through yields a "/contexts/." path
	// whose "." segment gets normalized away, producing a malformed request.
	if ctx == "." {
		ctx = ""
	}
	return ctx
}
