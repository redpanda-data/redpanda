// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package iotune_test

import (
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/iotune"
	"github.com/stretchr/testify/require"
)

func TestDataFor(t *testing.T) {
	tests := []struct {
		name           string
		provider       string
		vm             string
		storage        string
		expectedErrMsg string
	}{
		{
			name:     "it shouldn't fail for supported setups",
			provider: "aws",
			vm:       "i3.large",
			storage:  "default",
		},
		{
			name:           "it should return an error for unsupported providers",
			provider:       "unsupported",
			vm:             "i3.large",
			storage:        "default",
			expectedErrMsg: "no iotune data found for provider 'unsupported'",
		},
		{
			name:           "it should return an error for unsupported vms",
			provider:       "aws",
			vm:             "unsupported",
			storage:        "default",
			expectedErrMsg: "no iotune data found for VM 'unsupported', of provider 'aws'",
		},
		{
			name:           "it should return an error for unsupported storage",
			provider:       "aws",
			vm:             "i3.large",
			storage:        "unsupported",
			expectedErrMsg: "no iotune data found for storage 'unsupported' in VM 'i3.large', of provider 'aws'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := iotune.DataFor("/mount/point", tt.provider, tt.vm, tt.storage)
			if tt.expectedErrMsg != "" {
				require.EqualError(t, err, tt.expectedErrMsg)
				return
			}
			require.NoErrorf(t, err,
				"got an error for a supported setup ('%s', '%s', '%s'): %v",
				tt.provider,
				tt.vm,
				tt.storage,
				err,
			)
		})
	}
}
