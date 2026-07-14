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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
)

func TestTopicConfigSchemaContext(t *testing.T) {
	strptr := func(s string) *string { return &s }
	for _, tc := range []struct {
		name string
		cfgs []kadm.Config
		exp  string
	}{
		{"nil configs", nil, ""},
		{"unset", []kadm.Config{{Key: "cleanup.policy", Value: strptr("delete")}}, ""},
		{"set", []kadm.Config{{Key: "redpanda.schema.registry.context", Value: strptr(".prod")}}, ".prod"},
		{"present but nil value", []kadm.Config{{Key: "redpanda.schema.registry.context", Value: nil}}, ""},
		{"among other configs", []kadm.Config{
			{Key: "cleanup.policy", Value: strptr("delete")},
			{Key: "redpanda.schema.registry.context", Value: strptr(".staging")},
			{Key: "redpanda.iceberg.mode", Value: strptr("value_schema_id_prefix")},
		}, ".staging"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.exp, topicConfigSchemaContext(tc.cfgs))
		})
	}
}

func TestEffectiveSchemaContext(t *testing.T) {
	tests := []struct {
		name       string
		flagSet    bool
		flagValue  string
		topicValue string
		exp        string
	}{
		{"flag wins over topic", true, ".flag", ".topic", ".flag"},
		{"flag empty forces default", true, "", ".topic", ""},
		{"topic used when flag unset", false, ".ignored-when-unset", ".topic", ".topic"},
		{"topic empty when flag unset", false, ".ignored-when-unset", "", ""},
		{"root context normalizes from flag", true, ".", ".topic", ""},
		{"root context normalizes from topic", false, "", ".", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.exp, effectiveSchemaContext(tt.flagSet, tt.flagValue, tt.topicValue))
		})
	}
}
