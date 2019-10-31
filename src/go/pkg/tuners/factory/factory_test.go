package factory_test

import (
	"reflect"
	"testing"
	"vectorized/pkg/redpanda"
	"vectorized/pkg/tuners/factory"
)

func getValidConfig() *redpanda.Config {
	return &redpanda.Config{
		Redpanda: &redpanda.RedpandaConfig{
			Directory: "/var/lib/redpanda/data",
			RPCServer: redpanda.SocketAddress{
				Port:    33145,
				Address: "127.0.0.1",
			},
			Id: 1,
			KafkaApi: redpanda.SocketAddress{
				Port:    9092,
				Address: "127.0.0.1",
			},
			SeedServers: []*redpanda.SeedServer{
				&redpanda.SeedServer{
					Host: redpanda.SocketAddress{
						Port:    33145,
						Address: "127.0.0.1",
					},
					Id: 1,
				},
				&redpanda.SeedServer{
					Host: redpanda.SocketAddress{
						Port:    33146,
						Address: "127.0.0.1",
					},
					Id: 2,
				},
			},
		},
		Rpk: &redpanda.RpkConfig{},
	}
}

func getValidTunerParams() *factory.TunerParams {
	return &factory.TunerParams{
		Mode:          "",
		CpuMask:       "00000000000000000000000000000001",
		RebootAllowed: true,
		Disks:         []string{"dev1"},
		Directories:   []string{"/var/lib/redpanda"},
		Nics:          []string{"eth0"},
	}
}

func TestMergeTunerParamsConfig(t *testing.T) {
	type args struct {
		tunerParams func() *factory.TunerParams
		config      func() *redpanda.Config
	}
	tests := []struct {
		name     string
		args     args
		expected func() *factory.TunerParams
	}{
		{
			name: "it should override the configuration",
			args: args{
				tunerParams: getValidTunerParams,
				config:      getValidConfig,
			},
			expected: getValidTunerParams,
		},
		{
			name: "it should take values from the configuration when they're not in the params",
			args: args{
				tunerParams: func() *factory.TunerParams {
					params := getValidTunerParams()
					params.Directories = []string{}
					return params
				},
				config: getValidConfig,
			},
			expected: func() *factory.TunerParams {
				params := getValidTunerParams()
				params.Directories = []string{getValidConfig().Redpanda.Directory}
				return params
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := factory.MergeTunerParamsConfig(tt.args.tunerParams(), tt.args.config())
			if err != nil {
				t.Errorf("got an unexpected error: %v", err)
			}
			expected := tt.expected()
			if !reflect.DeepEqual(res, expected) {
				t.Errorf("the result was different than expected. Got:\n%vj\nWanted:\n%v", res, expected)
			}
		})
	}
}
