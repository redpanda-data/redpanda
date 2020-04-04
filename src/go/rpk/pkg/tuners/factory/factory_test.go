package factory_test

import (
	"testing"
	"vectorized/pkg/config"
	"vectorized/pkg/tuners/factory"

	"github.com/stretchr/testify/require"
)

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
	tests := []struct {
		name        string
		tunerParams func() *factory.TunerParams
		expected    func() *factory.TunerParams
	}{
		{
			name:        "it should override the configuration",
			tunerParams: getValidTunerParams,
			expected:    getValidTunerParams,
		},
		{
			name: "it should take values from the configuration when they're not in the params",
			tunerParams: func() *factory.TunerParams {
				params := getValidTunerParams()
				params.Directories = []string{}
				return params
			},
			expected: func() *factory.TunerParams {
				params := getValidTunerParams()
				params.Directories = []string{
					config.DefaultConfig().Redpanda.Directory,
				}
				return params
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := config.DefaultConfig()
			res, err := factory.MergeTunerParamsConfig(tt.tunerParams(), &conf)
			require.NoError(t, err)
			expected := tt.expected()
			require.Exactly(t, expected, res)
		})
	}
}
