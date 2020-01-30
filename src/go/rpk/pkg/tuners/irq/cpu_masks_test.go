package irq

import (
	"testing"
	"vectorized/pkg/tuners/executors"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
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
	assert.Equal(t, setMask, readMask, "Set and Read masks must be equal")
	assert.NoError(t, err)
}

func Test_masksEqual(t *testing.T) {
	type args struct {
		a string
		b string
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "masks with the same string should be equal",
			args: args{
				a: "0x0000001",
				b: "0x0000001",
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "mask with the same numbers are equal",
			args: args{
				a: "0x0000001",
				b: "01",
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "multi part masks are equal",
			args: args{
				a: "01,,08",
				b: "0000001,,00000008",
			},
			want:    true,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MasksEqual(tt.args.a, tt.args.b)
			if (err != nil) != tt.wantErr {
				t.Errorf("masksEqual() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("masksEqual() = %v, want %v", got, tt.want)
			}
		})
	}
}
