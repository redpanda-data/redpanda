// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package configuration_test

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"

	"github.com/go-logr/logr"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/configuration"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/stretchr/testify/assert"
)

func TestComputePatch(t *testing.T) {
	tests := []struct {
		name        string
		apply       map[string]interface{}
		current     map[string]interface{}
		lastApplied map[string]interface{}
		invalid     []string
		expected    configuration.CentralConfigurationPatch
	}{
		{
			name: "simple",
			apply: map[string]interface{}{
				"a": "b",
			},
			current: map[string]interface{}{
				"c": "d",
			},
			lastApplied: map[string]interface{}{},
			expected: configuration.CentralConfigurationPatch{
				Upsert: map[string]interface{}{
					"a": "b",
				},
				Remove: []string{},
			},
		},
		{
			name: "remove dangling",
			apply: map[string]interface{}{
				"a": "b",
			},
			current: map[string]interface{}{
				"c": "d",
			},
			lastApplied: map[string]interface{}{"c": "xx", "x": "xx"},
			expected: configuration.CentralConfigurationPatch{
				Upsert: map[string]interface{}{
					"a": "b",
				},
				Remove: []string{"c"},
			},
		},
		{
			name: "remove dangling invalid",
			apply: map[string]interface{}{
				"a": "b",
			},
			current: map[string]interface{}{
				"c": "d",
			},
			lastApplied: map[string]interface{}{"c": "xx", "x": "xx", "f": "invalid", "g": "invalid2"},
			invalid:     []string{"g", "f", "e"},
			expected: configuration.CentralConfigurationPatch{
				Upsert: map[string]interface{}{
					"a": "b",
				},
				Remove: []string{"c", "f", "g"},
			},
		},
		{
			name: "upsert mismatches only",
			apply: map[string]interface{}{
				"a": "b",
				"c": "x",
			},
			current: map[string]interface{}{
				"a": "b",
				"c": "d",
			},
			lastApplied: map[string]interface{}{"a": "xx", "c": "xx"},
			expected: configuration.CentralConfigurationPatch{
				Upsert: map[string]interface{}{
					"c": "x",
				},
				Remove: []string{},
			},
		},
		{
			name: "support type conversion",
			apply: map[string]interface{}{
				"a": "b",
				"c": 23,
			},
			current: map[string]interface{}{
				"a": "b",
				"c": "23",
			},
			lastApplied: map[string]interface{}{"a": "xx", "c": "xx"},
			expected: configuration.CentralConfigurationPatch{
				Upsert: map[string]interface{}{},
				Remove: []string{},
			},
		},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("ComputePatch-%s", tc.name), func(t *testing.T) {
			compPatch := configuration.ThreeWayMerge(logr.Discard(), tc.apply, tc.current, tc.lastApplied, tc.invalid, nil)
			assert.Equal(t, tc.expected.Upsert, compPatch.Upsert, "Upsert does not match")
			assert.Equal(t, tc.expected.Remove, compPatch.Remove, "Remove list does not match")
		})
	}
}

func TestString(t *testing.T) {
	p := configuration.CentralConfigurationPatch{
		Upsert: map[string]interface{}{"c": "d", "a": "b"},
		Remove: []string{"x", "e", "f"},
	}
	assert.Equal(t, "+a +c -e -f -x", p.String())
}

//nolint:funlen // it's a table test
func TestPropertyEquality(t *testing.T) {
	var nilPointer *int
	tests := []struct {
		v1        interface{}
		v2        interface{}
		metadata  admin.ConfigPropertyMetadata
		different bool
	}{
		{
			v1: "astring",
			v2: "astring",
		},
		{
			v1: 536870912,
			v2: "536870912",
		},
		{
			v1: 536870912,
			v2: "536870912",
			metadata: admin.ConfigPropertyMetadata{
				Type: "integer",
			},
		},
		{
			v1: 536870912.0,
			v2: "536870912",
		},
		{
			v1: 536870912.0,
			v2: 536870912,
		},
		{
			v1: json.Number("536870912"),
			v2: 536870912,
		},
		{
			v1: json.Number("0.9999"),
			v2: 0.9999,
		},
		{
			v1: math.Pi,
			v2: 3.14159265359,
		},
		{
			v1: []string{"a", "b", "c"},
			v2: []string{"a", "b", "c"},
		},
		{
			v1: []string{},
			v2: []string{},
		},
		{
			v1: nil,
			v2: nil,
		},
		{
			v1: nilPointer,
			v2: nil,
		},
		{
			v1: true,
			v2: true,
		},
		{
			v1: "true",
			v2: true,
		},
		{
			v1: false,
			v2: "false",
		},
		{
			v1:        true,
			v2:        false,
			different: true,
		},
		{
			v1:        "astring",
			v2:        "astring2",
			different: true,
		},
		{
			v1:        536870912.1,
			v2:        "536870912",
			different: true,
		},
		{
			v1:        []string{"a", "b", "c"},
			v2:        []string{"a", "c", "b"},
			different: true,
		},
		{
			v1: 0.4999999999,
			v2: 0.5,
			metadata: admin.ConfigPropertyMetadata{
				Type: "number",
			},
		},
	}
	for i := range tests {
		tc := tests[i]
		t.Run(fmt.Sprintf("TestEquality-%d", i), func(t *testing.T) {
			res := configuration.PropertiesEqual(logr.Discard(), tc.v1, tc.v2, tc.metadata)
			assert.Equal(t, !tc.different, res)
		})
	}
}
