// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package irq

import (
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func Test_cpuMasks_ReadMask(t *testing.T) {
	// given
	fs := afero.NewMemMapFs()
	cpuMasks := NewCPUMasks(fs, nil, executors.NewDirectExecutor())
	setMask := "0xff0,,0x13"
	afero.WriteFile(fs, "/test/cpu/0/smp_affinity", []byte{0}, 0o644)
	cpuMasks.SetMask("/test/cpu/0/smp_affinity", setMask)
	// when
	readMask, err := cpuMasks.ReadMask("/test/cpu/0/smp_affinity")
	// then
	require.Equal(t, setMask, readMask, "Set and Read masks must be equal")
	require.NoError(t, err)
}

func Test_masksEqual(t *testing.T) {
	tests := []struct {
		name string
		a    string
		b    string
		want bool
	}{
		{
			name: "masks with the same string should be equal",
			a:    "0x0000001",
			b:    "0x0000001",
			want: true,
		},
		{
			name: "mask with the same numbers are equal",
			a:    "0x0000001",
			b:    "01",
			want: true,
		},
		{
			name: "multi part masks are equal",
			a:    "01,,08",
			b:    "0000001,,00000008",
			want: true,
		},
		{
			name: "should return false if the masks' # of parts differs",
			a:    "0,1",
			b:    "0000001",
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MasksEqual(tt.a, tt.b)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
