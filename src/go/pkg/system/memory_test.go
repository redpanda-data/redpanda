package system

import (
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/docker/go-units"
)

func Test_parseMemInfo(t *testing.T) {
	type args struct {
		reader io.Reader
	}
	tests := []struct {
		name    string
		args    args
		want    *MemInfo
		wantErr bool
	}{
		{
			name: "shall parse the correct meminfo file",
			args: args{
				reader: strings.NewReader(
					`MemTotal:       15862140 kB                                                                                                                                                                   
				 	 MemFree:         1938684 kB                                                                                                                                                                   
					 MemAvailable:    8933368 kB`),
			},
			want: &MemInfo{
				MemTotal:     15862140 * units.KiB,
				MemFree:      1938684 * units.KiB,
				MemAvailable: 8933368 * units.KiB,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseMemInfo(tt.args.reader)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseMemInfo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseMemInfo() = %v, want %v", got, tt.want)
			}
		})
	}
}
