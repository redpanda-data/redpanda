package builders

import (
	"testing"

	"github.com/spf13/afero"
)

func Test_sandboxBuilder_validateConfig(t *testing.T) {
	type fields struct {
		fs                 afero.Fs
		nodes              int
		dir                string
		tarball            string
		brokerType         BrokerType
		destroyOldIfExists bool
	}
	tests := []struct {
		name    string
		fields  fields
		before  func(afero.Fs)
		wantErr bool
	}{
		{
			name: "should not return error for valid kafka configuration",
			fields: fields{
				fs:                 afero.NewMemMapFs(),
				nodes:              1,
				dir:                "/tmp/sbox",
				brokerType:         KafkaBroker,
				destroyOldIfExists: true,
			},
			before:  func(afero.Fs) {},
			wantErr: false,
		},
		{
			name: "should return error when requesting kafka broker with more than one node",
			fields: fields{
				fs:                 afero.NewMemMapFs(),
				nodes:              2,
				dir:                "/tmp/sbox",
				brokerType:         KafkaBroker,
				destroyOldIfExists: true,
			},
			before:  func(afero.Fs) {},
			wantErr: true,
		},
		{
			name: "should return error when tarball is provided for kafka sandbox",
			fields: fields{
				fs:                 afero.NewMemMapFs(),
				nodes:              1,
				dir:                "/tmp/sbox",
				tarball:            "/home/rp/redpanda.tar.gz",
				brokerType:         KafkaBroker,
				destroyOldIfExists: true,
			},
			before:  func(afero.Fs) {},
			wantErr: true,
		},
		{
			name: "should not return error for valid redpanda configuration",
			fields: fields{
				fs:                 afero.NewMemMapFs(),
				nodes:              3,
				tarball:            "/home/rp/redpanda.tar.gz",
				dir:                "/tmp/sbox",
				brokerType:         RedpandaBroker,
				destroyOldIfExists: true,
			},
			before: func(fs afero.Fs) {
				fs.MkdirAll("/home/rp", 0755)
				f, _ := fs.Create("/home/rp/redpanda.tar.gz")
				f.Close()
			},
			wantErr: false,
		},
		{
			name: "should return an error when tarball is not provided",
			fields: fields{
				fs:                 afero.NewMemMapFs(),
				nodes:              3,
				dir:                "/tmp/sbox",
				brokerType:         RedpandaBroker,
				destroyOldIfExists: true,
			},
			before:  func(afero.Fs) {},
			wantErr: true,
		},
		{
			name: "should return an error when tarball does not exists",
			fields: fields{
				fs:                 afero.NewMemMapFs(),
				nodes:              3,
				dir:                "/tmp/sbox",
				tarball:            "/home/rp/redpanda.tar.gz",
				brokerType:         RedpandaBroker,
				destroyOldIfExists: true,
			},
			before:  func(afero.Fs) {},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.before(tt.fields.fs)
			b := &sandboxBuilder{
				fs:                 tt.fields.fs,
				nodes:              tt.fields.nodes,
				dir:                tt.fields.dir,
				tarball:            tt.fields.tarball,
				brokerType:         tt.fields.brokerType,
				destroyOldIfExists: tt.fields.destroyOldIfExists,
			}
			if err := b.validateConfig(); (err != nil) != tt.wantErr {
				t.Errorf("sandboxBuilder.validateConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
