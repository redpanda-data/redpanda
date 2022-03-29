// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package os_test

import (
	"fmt"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

type beforeFunc func(afero.Fs) error

func TestIsRunningPID(t *testing.T) {
	pid := 4321
	fileFmt := "4321 (rpk) %s 1 1 1 0 -1 4194560 115854 29631806 115 956 443 1316 612807 129163 20 0 1 0 45 175927296 3830 18446744073709551615 1 1 0 0 0 0 671173123 4096 1260 0 0 0 17 0 0 0 8 0 0 0 0 0 0 0 0 0 0"
	path := "/proc/4321/stat"
	defaultSetup := func(state string) beforeFunc {
		return func(fs afero.Fs) error {
			contents := fmt.Sprintf(fileFmt, state)
			_, err := utils.WriteBytes(fs, []byte(contents), path)
			return err
		}
	}
	tests := []struct {
		name        string
		before      beforeFunc
		expected    bool
		expectedErr string
	}{
		{
			name:     "it should return false if the file doesn't exist",
			expected: false,
		},
		{
			name:     "it should return false if the process' state is Z",
			before:   defaultSetup("Z"),
			expected: false,
		},
		{
			name:     "it should return false if the process' state is X",
			before:   defaultSetup("X"),
			expected: false,
		},
		{
			name:     "it should return false if the process' state is x",
			before:   defaultSetup("x"),
			expected: false,
		},
		{
			name:     "it should return true if the process' isn't Z, X or x",
			before:   defaultSetup("R"),
			expected: true,
		},
		{
			name: "it should fail if the file is corrupt",
			before: func(fs afero.Fs) error {
				_, err := utils.WriteBytes(fs, []byte("lolwut"), path)
				return err
			},
			expectedErr: "corrupt info for process 4321",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			if tt.before != nil {
				err := tt.before(fs)
				require.NoError(t, err)
			}
			running, err := os.IsRunningPID(fs, pid)
			if tt.expectedErr != "" {
				require.EqualError(t, err, tt.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.expected, running)
		})
	}
}
