// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package tuners_test

import (
	"fmt"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestChecker(t *testing.T) {
	tests := []struct {
		name      string
		before    func(fs afero.Fs) error
		expectOk  bool
		expectErr bool
	}{
		{
			name: "It should return true if the value is correct",
			before: func(fs afero.Fs) error {
				_, err := utils.WriteBytes(
					fs,
					[]byte(fmt.Sprint(tuners.ExpectedSwappiness)),
					tuners.File,
				)
				return err
			},
			expectOk: true,
		},
		{
			name: "It should return false if the file exists but the value iswrong",
			before: func(fs afero.Fs) error {
				_, err := utils.WriteBytes(
					fs,
					[]byte("120"),
					tuners.File,
				)
				return err
			},
			expectOk: false,
		},
		{
			name:      "It should fail if the file doesn't exist",
			expectOk:  false,
			expectErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			if tt.before != nil {
				err := tt.before(fs)
				require.NoError(t, err)
			}
			checker := tuners.NewSwappinessChecker(fs)
			res := checker.Check()
			if tt.expectErr {
				require.Error(t, res.Err)
				return
			}
			require.NoError(t, res.Err)
			require.Equal(t, tt.expectOk, res.IsOk)
		})
	}
}

func TestTuner(t *testing.T) {
	tests := []struct {
		name      string
		before    func(fs afero.Fs) error
		expectErr bool
	}{
		{
			name: "It should leave the same value if it was correct",
			before: func(fs afero.Fs) error {
				_, err := utils.WriteBytes(
					fs,
					[]byte(fmt.Sprint(tuners.ExpectedSwappiness)),
					tuners.File,
				)
				return err
			},
		},
		{
			name: "It should change the value if it was different",
			before: func(fs afero.Fs) error {
				_, err := utils.WriteBytes(
					fs,
					[]byte("120"),
					tuners.File,
				)
				return err
			},
		},
		{
			name:      "It should fail if the file doesn't exist",
			expectErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			if tt.before != nil {
				err := tt.before(fs)
				require.NoError(t, err)
			}
			tuner := tuners.NewSwappinessTuner(fs, executors.NewDirectExecutor())
			res := tuner.Tune()
			if tt.expectErr {
				require.Error(t, res.Error())
				return
			}
			require.NoError(t, res.Error())
			lines, err := utils.ReadFileLines(fs, tuners.File)
			require.NoError(t, err)
			require.Len(t, lines, 1)
			require.Equal(t, fmt.Sprint(tuners.ExpectedSwappiness), lines[0])
		})
	}
}
