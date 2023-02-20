// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux
// +build linux

package factory_test

import (
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/factory"
	"github.com/stretchr/testify/require"
)

func getValidTunerParams() *factory.TunerParams {
	return &factory.TunerParams{
		Mode:          "",
		CPUMask:       "00000000000000000000000000000001",
		RebootAllowed: true,
		Disks:         []string{"dev1"},
		Directories:   []string{"/var/lib/redpanda"},
		Nics:          []string{"eth0"},
	}
}

func TestMergeTunerParamsConfig(t *testing.T) {
	tests := []struct {
		name        string
		tunerParams func() *factory.TunerParams
		expected    func() *factory.TunerParams
	}{
		{
			name:        "it should override the configuration",
			tunerParams: getValidTunerParams,
			expected:    getValidTunerParams,
		},
		{
			name: "it should take values from the configuration when they're not in the params",
			tunerParams: func() *factory.TunerParams {
				params := getValidTunerParams()
				params.Directories = []string{}
				return params
			},
			expected: func() *factory.TunerParams {
				params := getValidTunerParams()
				params.Directories = []string{
					config.DevDefault().Redpanda.Directory,
				}
				return params
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := config.DevDefault()
			res, err := factory.MergeTunerParamsConfig(tt.tunerParams(), conf)
			require.NoError(t, err)
			expected := tt.expected()
			require.Exactly(t, expected, res)
		})
	}
}
