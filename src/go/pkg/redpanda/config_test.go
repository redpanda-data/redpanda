package redpanda

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"vectorized/pkg/utils"

	"github.com/spf13/afero"
	"gopkg.in/yaml.v2"
)

func getValidConfig() *Config {
	return &Config{
		Redpanda: &RedpandaConfig{
			Directory: "/var/lib/redpanda/data",
			RPCServer: SocketAddress{
				Port:    33145,
				Address: "127.0.0.1",
			},
			Id: 1,
			KafkaApi: SocketAddress{
				Port:    9092,
				Address: "127.0.0.1",
			},
			SeedServers: []*SeedServer{
				&SeedServer{
					Host: SocketAddress{
						Port:    33145,
						Address: "127.0.0.1",
					},
					Id: 1,
				},
				&SeedServer{
					Host: SocketAddress{
						Port:    33146,
						Address: "127.0.0.1",
					},
					Id: 2,
				},
			},
		},
	}
}

func TestReadConfigFromPath(t *testing.T) {
	const baseDir string = "/etc/redpanda"
	type args struct {
		fs   afero.Fs
		path string
	}
	tests := []struct {
		name    string
		args    args
		before  func(afero.Fs, string) error
		want    func() *Config
		wantErr bool
	}{
		{
			name: "shall return config struct field with values from file",
			before: func(fs afero.Fs, path string) error {
				bs, err := yaml.Marshal(getValidConfig())
				if err != nil {
					return err
				}
				if err = fs.MkdirAll(baseDir, 0755); err != nil {
					return err
				}
				_, err = utils.WriteBytes(fs, bs, path)
				return err
			},
			args: args{
				fs:   afero.NewMemMapFs(),
				path: baseDir + "/redpanda.yaml",
			},
			want:    getValidConfig,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.before(tt.args.fs, tt.args.path); err != nil {
				t.Fatalf("got an error while setting up %v: %v", tt.name, err)
			}
			got, err := ReadConfigFromPath(tt.args.fs, tt.args.path)
			want := tt.want()
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadConfigFromPath() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("ReadConfigFromPath() = %v, want %v", *got, *want)
			}
		})
	}
}

func TestWriteConfig(t *testing.T) {
	type args struct {
		config *Config
		fs     afero.Fs
		path   string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
		after   func(afero.Fs) error
	}{
		{
			name: "shall write valid config file",
			args: args{
				fs:   afero.NewMemMapFs(),
				path: "/redpanda.yaml",
				config: &Config{
					Redpanda: &RedpandaConfig{
						Directory: "/var/lib/redpanda/data",
						RPCServer: SocketAddress{
							Port:    33145,
							Address: "127.0.0.1",
						},
						Id: 1,
						KafkaApi: SocketAddress{
							Port:    9092,
							Address: "127.0.0.1",
						},
						SeedServers: []*SeedServer{
							&SeedServer{
								Host: SocketAddress{
									Port:    33145,
									Address: "127.0.0.1",
								},
								Id: 1,
							},
							&SeedServer{
								Host: SocketAddress{
									Port:    33146,
									Address: "127.0.0.1",
								},
								Id: 2,
							},
						}},
				},
			},
			after: func(fs afero.Fs) error {
				if !utils.FileExists(fs, "/redpanda.yaml") {
					return errors.New("File redpanda.yaml must exists")
				}
				content, err := afero.ReadFile(fs, "/redpanda.yaml")
				if err != nil {
					return err
				}
				actualContent := string(content)
				expectedContent :=
					`redpanda:
  data_directory: /var/lib/redpanda/data
  rpc_server:
    address: 127.0.0.1
    port: 33145
  kafka_api:
    address: 127.0.0.1
    port: 9092
  node_id: 1
  seed_servers:
  - host:
      address: 127.0.0.1
      port: 33145
    node_id: 1
  - host:
      address: 127.0.0.1
      port: 33146
    node_id: 2
`
				if actualContent != expectedContent {
					return fmt.Errorf("Expected:\n'%s'\n and actual:\n'%s'\n content differs",
						strings.ReplaceAll(expectedContent, " ", "·"),
						strings.ReplaceAll(actualContent, " ", "·"))
				}
				return nil
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := WriteConfig(tt.args.config, tt.args.fs, tt.args.path); (err != nil) != tt.wantErr {
				t.Errorf("WriteConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err := tt.after(tt.args.fs); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestCheckConfig(t *testing.T) {
	type args struct {
		config *Config
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "shall return true when config is valid",
			args: args{
				config: &Config{
					Redpanda: &RedpandaConfig{
						Directory: "/var/lib/redpanda/data",
						RPCServer: SocketAddress{
							Port:    33145,
							Address: "127.0.0.1",
						},
						Id: 1,
						KafkaApi: SocketAddress{
							Port:    9092,
							Address: "127.0.0.1",
						},
						SeedServers: []*SeedServer{
							&SeedServer{
								Host: SocketAddress{
									Port:    33145,
									Address: "127.0.0.1",
								},
								Id: 1,
							},
							&SeedServer{
								Host: SocketAddress{
									Port:    33146,
									Address: "127.0.0.1",
								},
								Id: 1,
							},
						}},
				},
			},
			want: true,
		},
		{
			name: "shall return false when config file does not contain data directory setting",
			args: args{
				config: &Config{
					Redpanda: &RedpandaConfig{
						RPCServer: SocketAddress{
							Port:    33145,
							Address: "127.0.0.1",
						},
						Id: 1,
						KafkaApi: SocketAddress{
							Port:    9092,
							Address: "127.0.0.1",
						},
						SeedServers: []*SeedServer{
							&SeedServer{
								Host: SocketAddress{
									Port:    33145,
									Address: "127.0.0.1",
								},
								Id: 1,
							},
							&SeedServer{
								Host: SocketAddress{
									Port:    33146,
									Address: "127.0.0.1",
								},
								Id: 1,
							},
						},
					},
				},
			},
			want: false,
		},
		{
			name: "shall return false when id of server is negative",
			args: args{
				config: &Config{
					Redpanda: &RedpandaConfig{
						Directory: "/var/lib/redpanda/data",
						RPCServer: SocketAddress{
							Port:    33145,
							Address: "127.0.0.1",
						},
						Id: -1,
						KafkaApi: SocketAddress{
							Port:    9092,
							Address: "127.0.0.1",
						},
						SeedServers: []*SeedServer{
							&SeedServer{
								Host: SocketAddress{
									Port:    33145,
									Address: "127.0.0.1",
								},
								Id: 1,
							},
							&SeedServer{
								Host: SocketAddress{
									Port:    33146,
									Address: "127.0.0.1",
								},
								Id: 1,
							},
						}},
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CheckConfig(tt.args.config); got != tt.want {
				t.Errorf("CheckConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}
