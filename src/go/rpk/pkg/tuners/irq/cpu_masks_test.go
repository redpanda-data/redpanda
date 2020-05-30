package irq

import (
	"testing"
	"vectorized/pkg/tuners/executors"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func Test_cpuMasks_ReadMask(t *testing.T) {
	// given
	fs := afero.NewMemMapFs()
	cpuMasks := NewCpuMasks(fs, nil, executors.NewDirectExecutor())
	setMask := "0xff0,,0x13"
	afero.WriteFile(fs, "/test/cpu/0/smp_affinity", []byte{0}, 0644)
	cpuMasks.SetMask("/test/cpu/0/smp_affinity", setMask)
	//when
	readMask, err := cpuMasks.ReadMask("/test/cpu/0/smp_affinity")
	//then
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
			a:    "00000ff",
			b:    "00000ff",
			want: true,
		},
		{
			name: "should return false if the masks are different",
			a:    "afaf",
			b:    "17e",
			want: false,
		},
		{
			name: "multi part masks are equal",
			a:    "01,,07",
			b:    "01,,07",
			want: true,
		},
		{
			name: "should return false if the masks' # of parts differs",
			a:    "0,1",
			b:    "0000001",
			want: false,
		},
		{
			name: "shouldn't fail even if the numeric equivalent overflowed uint64",
			a:    "ffffffffffffffffffffffffffffffffffffffffffffffffff",
			b:    "ffffffffffffffffffffffffffffffffffffffffffffffffff",
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MasksEqual(tt.a, tt.b)
			require.Equal(t, tt.want, got)
		})
	}
}
