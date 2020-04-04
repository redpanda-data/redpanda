package coredump

import (
	"os"
	"testing"
	"vectorized/pkg/config"
	"vectorized/pkg/tuners/executors"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func validConfig() config.Config {
	conf := config.DefaultConfig()
	conf.Rpk.TuneCoredump = true
	conf.Rpk.CoredumpDir = "/var/lib/redpanda/coredumps"
	return conf
}

func TestTune(t *testing.T) {
	tests := []struct {
		name string
		pre  func(afero.Fs, config.Config) error
		conf func() config.Config
	}{
		{
			name: "it should install the coredump config file",
			conf: validConfig,
		},
		{
			name: "it should not fail to install if the coredump config file already exists",
			pre: func(fs afero.Fs, config config.Config) error {
				_, err := fs.Create(corePatternFilePath)
				return err
			},
			conf: validConfig,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			conf := tt.conf()
			if tt.pre != nil {
				err := tt.pre(fs, conf)
				require.NoError(t, err)
			}
			tuner := NewCoredumpTuner(fs, tt.conf(), executors.NewDirectExecutor())
			res := tuner.Tune()
			require.NoError(t, res.Error())
			pattern, err := fs.Open(corePatternFilePath)
			require.NoError(t, err)
			script, err := fs.Open(scriptFilePath)
			require.NoError(t, err)
			info, err := script.Stat()
			require.NoError(t, err)
			// Check that the script is world-readable, writable and executable
			expectedMode := os.FileMode(int(0777))
			require.Equal(t, expectedMode, info.Mode())
			expectedScript, err := renderTemplate(coredumpScriptTmpl, *conf.Rpk)
			require.NoError(t, err)
			buf := make([]byte, len(expectedScript))
			_, err = script.Read(buf)
			require.NoError(t, err)
			actualScript := string(buf)
			require.Equal(t, expectedScript, actualScript)
			buf = make([]byte, len(coredumpPattern))
			_, err = pattern.Read(buf)
			require.NoError(t, err)
			actualPattern := string(buf)
			require.Equal(t, coredumpPattern, actualPattern)
		})
	}
}
