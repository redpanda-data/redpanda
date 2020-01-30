package tuners

import (
	"reflect"
	"testing"
)

func TestNewTuneResult(t *testing.T) {
	type args struct {
		rebootRequired bool
	}
	tests := []struct {
		name string
		args args
		want TuneResult
	}{
		{
			name: "Shall indicate that reboot is required when passed true",
			args: args{rebootRequired: true},
			want: &tuneResult{rebootRequired: true},
		},
		{
			name: "Shall indicate that reboot is required when passed true",
			args: args{rebootRequired: false},
			want: &tuneResult{rebootRequired: false},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewTuneResult(tt.args.rebootRequired); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewTuneResult() = %v, want %v", got, tt.want)
			}
		})
	}
}
