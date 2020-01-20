package tuners

import (
	"testing"
	"vectorized/pkg/tuners/executors"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

func TestDeviceNomergesTuner_Tune(t *testing.T) {
	// given
	schedulerInfo := &schedulerInfoMock{
		getNomergesFeatureFile: func(string) (string, error) {
			return "/sys/devices/pci0000:00/0000:00:1d.0/0000:71:00.0/nvme/fake/queue/nomerges", nil
		},
		getNomerges: func(string) (int, error) {
			return 0, nil
		},
	}
	fs := afero.NewMemMapFs()
	fs.MkdirAll("/sys/devices/pci0000:00/0000:00:1d.0/0000:71:00.0/nvme/fake/queue", 0644)
	tuner := NewDeviceNomergesTuner(fs, "fake", schedulerInfo, executors.NewDirectExecutor())
	// when
	tuner.Tune()
	// then
	setValue, _ := afero.ReadFile(fs, "/sys/devices/pci0000:00/0000:00:1d.0/0000:71:00.0/nvme/fake/queue/nomerges")
	assert.Equal(t, "2", string(setValue))
}
