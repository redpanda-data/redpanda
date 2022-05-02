// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package hwloc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTranslateToHwLocCpuSet(t *testing.T) {
	tests := []struct {
		name    string
		cpuset  string
		want    string
		wantErr bool
	}{
		{
			name:    "shall return all in not changed form",
			cpuset:  "all",
			want:    "all",
			wantErr: false,
		},
		{
			name:    "shall translate cpuset(7) list type to hwloc PU's",
			cpuset:  "0-1,4,10-12,3",
			want:    "PU:0-1 PU:4 PU:10-12 PU:3",
			wantErr: false,
		},
		{
			name:    "shall return error on invalid CPU set",
			cpuset:  "0 to 1",
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := TranslateToHwLocCPUSet(tt.cpuset)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
