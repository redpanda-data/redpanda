package filesystem

import (
	"testing"

	"github.com/spf13/afero"
)

func TestDirectoryIsWriteable(t *testing.T) {
	type args struct {
		fs   afero.Fs
		path string
	}
	tests := []struct {
		name    string
		args    args
		before  func(fs afero.Fs)
		want    bool
		wantErr bool
	}{
		{
			name: "Shall not return an error when directory is writable",
			args: args{
				fs:   afero.NewMemMapFs(),
				path: "/redpanda/data",
			},
			before: func(fs afero.Fs) {

			},
			wantErr: false,
			want:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.before(tt.args.fs)
			got, err := DirectoryIsWriteable(tt.args.fs, tt.args.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("DirectoryIsWriteable() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("DirectoryIsWriteable() = %v, want %v", got, tt.want)
			}
		})
	}
}
