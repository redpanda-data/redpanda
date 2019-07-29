package disk

import (
	"testing"
	"vectorized/pkg/tuners/irq"

	"github.com/stretchr/testify/assert"
)

type diskInfoProviderMock struct {
	getDirectoriesDevices    func([]string) (map[string][]string, error)
	getBlockDeviceFromPath   func(string) (BlockDevice, error)
	getBlockDeviceSystemPath func(string) (string, error)
}

func (infoProvider *diskInfoProviderMock) GetDirectoriesDevices(
	directories []string,
) (map[string][]string, error) {
	return infoProvider.getDirectoriesDevices(directories)
}

func (infoProvider *diskInfoProviderMock) GetBlockDeviceFromPath(
	path string,
) (BlockDevice, error) {
	return infoProvider.getBlockDeviceFromPath(path)
}

func (infoProvider *diskInfoProviderMock) GetBlockDeviceSystemPath(
	path string,
) (string, error) {
	return infoProvider.getBlockDeviceSystemPath(path)
}

func TestDisksIRQsTuner_getDeviceControllerPath(t *testing.T) {
	//given
	infoProvider := &diskInfoProviderMock{
		getBlockDeviceSystemPath: func(str string) (string, error) {
			return "/sys/devices/pci0000:00/0000:00:1f." +
				"2/ata1/host0/target0:0:0/0:0:0:0/block/sda/sda1", nil
		},
	}
	diskIRQsTuner := &disksIRQsTuner{
		devices:          []string{"sdb1"},
		diskInfoProvider: infoProvider,
	}
	//when
	controllerPath, err := diskIRQsTuner.getDeviceControllerPath("sdb1")
	//then
	assert.Nil(t, err)
	assert.Equal(t, "/sys/devices/pci0000:00/0000:00:1f.2", controllerPath)
}

type mockProcFile struct {
	irq.ProcFile
	getIRQProcFileLinesMap func() (map[string]string, error)
}

func (mockProcFile *mockProcFile) GetIRQProcFileLinesMap() (
	map[string]string,
	error,
) {
	return mockProcFile.getIRQProcFileLinesMap()
}

func Test_disksIRQsTuner_isIRQNvmeFastPathIrq(t *testing.T) {
	//given
	fields := []struct {
		name     string
		procFile irq.ProcFile
		expected bool
		numCpus  int
	}{
		{
			name: "Shall return true as device with IRQ 18 is a NVMe device",
			procFile: &mockProcFile{
				getIRQProcFileLinesMap: func() (map[string]string, error) {
					procFileLine := "18:          0          0          0         " +
						" 0          0          0          0         21 " +
						"IR-PCI-MSI 59244544-edge      nvme0q4"
					return map[string]string{"18": procFileLine}, nil
				},
			},
			expected: true,
			numCpus:  8,
		},
		{
			name: "Shall return false as device with IRQ 18 is a NVMe device" +
				"but queue number is larger than number of cpus",
			procFile: &mockProcFile{
				getIRQProcFileLinesMap: func() (map[string]string, error) {
					procFileLine := "18:          0          0          0         " +
						" 0          0          0          0         21 " +
						"IR-PCI-MSI 59244544-edge      nvme0q5"
					return map[string]string{"18": procFileLine}, nil
				},
			},
			expected: false,
			numCpus:  4,
		},
		{
			name: "Shall return false as device with IRQ 18 is not NVMe device",
			procFile: &mockProcFile{
				getIRQProcFileLinesMap: func() (map[string]string, error) {
					procFileLine := "18:       1178       1469       3467" +
						"         96         17       3453       5932" +
						"        331  IR-PCI-MSI 333825-edge      iwlwifi: queue 1"
					return map[string]string{"18": procFileLine}, nil
				},
			},
			expected: false,
			numCpus:  8,
		}}
	for _, test := range fields {
		t.Run(test.name, func(t *testing.T) {
			//when
			diskIRQsTuner := &disksIRQsTuner{
				irqProcFile:  test.procFile,
				numberOfCpus: test.numCpus,
			}

			isNVMEIrq, err := diskIRQsTuner.isIRQNvmeFastPathIrq("18")
			//then
			assert.Nil(t, err)
			assert.Equal(t, test.expected, isNVMEIrq)
		})
	}
}
