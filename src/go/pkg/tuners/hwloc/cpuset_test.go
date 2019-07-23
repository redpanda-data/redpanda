package hwloc

import "testing"

func TestTranslateToHwLocCpuSet(t *testing.T) {
	type args struct {
		cpuset string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name:    "shall return all in not changed form",
			args:    args{cpuset: "all"},
			want:    "all",
			wantErr: false,
		},
		{
			name:    "shall translate cpuset(7) list type to hwloc PU's",
			args:    args{cpuset: "0-1,4,10-12,3"},
			want:    "PU:0-1 PU:4 PU:10-12 PU:3",
			wantErr: false,
		},
		{
			name:    "shall return error on invalid CPU set",
			args:    args{cpuset: "0 to 1"},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := TranslateToHwLocCpuSet(tt.args.cpuset)
			if (err != nil) != tt.wantErr {
				t.Errorf("TranslateToHwLocCpuSet() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("TranslateToHwLocCpuSet() = %v, want %v", got, tt.want)
			}
		})
	}
}
