// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package utils_test

import (
	"testing"

	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func TestTemplateGen(t *testing.T) {
	data := utils.NewEndpointTemplateData(2, "1.1.1.1")
	tests := []struct {
		tmpl     string
		expected string
		error    bool
	}{
		{
			tmpl:     "",
			expected: "2", // Index
		},
		{
			tmpl:     "{{.Index}}",
			expected: "2",
		},
		{
			tmpl:     "{{.Index}}-{{.HostIP | sha256sum | substr 0 8}}",
			expected: "2-f1412386",
		},
		{
			tmpl:  "abc-{{.Index}}-{{.HostIP}}-xyz",
			error: true, // HostIP contains dots
		},
		{
			tmpl:  `{{ "$USER" | expandenv }}`,
			error: true, // expandenv is not hermetic
		},
		{
			tmpl:  "aa{{.XX}}",
			error: true, // undefined variable
		},
		{
			tmpl:  "-{{.Index}}",
			error: true, // invalid start character
		},
		{
			tmpl:  "{{.Index}}-",
			error: true, // invalid end character
		},
	}

	for _, tc := range tests {
		t.Run(tc.tmpl, func(t *testing.T) {
			res, err := utils.ComputeEndpoint(tc.tmpl, data)
			assert.Equal(t, tc.expected, res)
			if tc.error {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
