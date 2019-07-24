package system

import (
	"reflect"
	"testing"

	"github.com/spf13/afero"
)

func TestReadRuntineOptions(t *testing.T) {
	type args struct {
		fs   afero.Fs
		path string
	}
	tests := []struct {
		name    string
		before  func(afero.Fs)
		args    args
		want    *RuntimeOptions
		wantErr bool
	}{
		{
			name: "shall return correct set of options",
			before: func(fs afero.Fs) {
				fs.MkdirAll("/proc", 0755)
				afero.WriteFile(
					fs, "/proc/test",
					[]byte("opt1 opt2 [opt3] opt4\n"),
					0644)
			},
			args: args{
				fs:   afero.NewMemMapFs(),
				path: "/proc/test",
			},
			want: &RuntimeOptions{
				optionsMap: map[string]bool{
					"opt1": false,
					"opt2": false,
					"opt3": true,
					"opt4": false,
				},
			},
			wantErr: false,
		},
		{
			name: "shall return correct set of options when last one is active",
			before: func(fs afero.Fs) {
				fs.MkdirAll("/proc", 0755)
				afero.WriteFile(
					fs, "/proc/test",
					[]byte("opt1 opt2 [opt3]\n"),
					0644)
			},
			args: args{
				fs:   afero.NewMemMapFs(),
				path: "/proc/test",
			},
			want: &RuntimeOptions{
				optionsMap: map[string]bool{
					"opt1": false,
					"opt2": false,
					"opt3": true,
				},
			},
			wantErr: false,
		},
		{
			name: "shall return error when there are more than one line in file",
			before: func(fs afero.Fs) {
				fs.MkdirAll("/proc", 0755)
				afero.WriteFile(
					fs, "/proc/test",
					[]byte("opt1 opt2 [opt3]\n second line\n"),
					0644)
			},
			args: args{
				fs:   afero.NewMemMapFs(),
				path: "/proc/test",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.before(tt.args.fs)
			got, err := ReadRuntineOptions(tt.args.fs, tt.args.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadRuntineOptions() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReadRuntineOptions() = %v, want %v", got, tt.want)
			}
		})
	}
}
