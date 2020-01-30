package redpanda

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/afero"
)

func TestFindConfig(t *testing.T) {
	type args struct {
		fs afero.Fs
	}
	tests := []struct {
		name    string
		args    args
		before  func(fs afero.Fs)
		want    string
		wantErr bool
	}{
		{
			name:   "should return an error when config is not found",
			before: func(afero.Fs) {},
			args: args{
				fs: afero.NewMemMapFs(),
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "should return config file from parent directory",
			before: func(fs afero.Fs) {
				currentDir := currentDir()
				fs.MkdirAll(currentDir, 0755)

				createConfigIn(fs, filepath.Dir(currentDir))
			},
			args: args{
				fs: afero.NewMemMapFs(),
			},
			want:    filepath.Join(filepath.Dir(currentDir()), "redpanda.yaml"),
			wantErr: false,
		},
		{
			name: "should return config file from 'etc' directory",
			before: func(fs afero.Fs) {
				createConfigIn(fs, "/etc/redpanda")
				currentDir := currentDir()
				fs.MkdirAll(currentDir, 0755)

				createConfigIn(fs, filepath.Dir(currentDir))
			},
			args: args{
				fs: afero.NewMemMapFs(),
			},
			want:    "/etc/redpanda/redpanda.yaml",
			wantErr: false,
		},
		{
			name: "should return config file from current directory",
			before: func(fs afero.Fs) {
				createConfigIn(fs, "/etc/redpanda")
				currentDir := currentDir()
				fs.MkdirAll(currentDir, 0755)
				createConfigIn(fs, filepath.Dir(currentDir))
				createConfigIn(fs, currentDir)
			},
			args: args{
				fs: afero.NewMemMapFs(),
			},
			want:    filepath.Join(currentDir(), "redpanda.yaml"),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.before(tt.args.fs)
			got, err := FindConfig(tt.args.fs)
			if (err != nil) != tt.wantErr {
				t.Errorf("FindConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("FindConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}

func createConfigIn(fs afero.Fs, path string) {
	fs.Create(filepath.Join(path, "redpanda.yaml"))
}

func currentDir() string {
	d, _ := os.Getwd()
	return d
}
