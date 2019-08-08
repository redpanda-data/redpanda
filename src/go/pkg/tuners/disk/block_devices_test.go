package disk

import (
	"testing"
	"vectorized/pkg/os"
	"vectorized/pkg/tuners/irq"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

type irqDeviceInfoMock struct {
	irq.DeviceInfo
}

type irqProcFileMock struct {
	irq.ProcFile
	getIRQProcFileLinesMap func() (map[int]string, error)
}

func (m *irqProcFileMock) GetIRQProcFileLinesMap() (map[int]string, error) {
	return m.getIRQProcFileLinesMap()
}

type procMock struct {
	os.Proc
}

func Test_blockDevices_getDeviceControllerPath(t *testing.T) {
	// given
	fs := afero.NewMemMapFs()
	irqDeviceInfo := &irqDeviceInfoMock{}
	irqProcFile := &irqProcFileMock{}
	proc := &procMock{}
	blockDevices := &blockDevices{
		fs:            fs,
		irqDeviceInfo: irqDeviceInfo,
		irqProcFile:   irqProcFile,
		proc:          proc}
	devSystemPath := "/sys/devices/pci0000:00/0000:00:1f.2/ata1/host0" +
		"/target0:0:0/0:0:0:0/block/sda/sda1"
	// when
	controllerPath, err := blockDevices.getDeviceControllerPath(devSystemPath)
	// then
	assert.Nil(t, err)
	assert.Equal(t, "/sys/devices/pci0000:00/0000:00:1f.2", controllerPath)
}

func Test_blockDevices_isIRQNvmeFastPathIRQ(t *testing.T) {
	//given
	fields := []struct {
		name     string
		procFile irq.ProcFile
		expected bool
		numCpus  int
	}{
		{
			name: "Shall return true as device with IRQ 18 is a NVMe device",
			procFile: &irqProcFileMock{
				getIRQProcFileLinesMap: func() (map[int]string, error) {
					procFileLine := "18:          0          0          0         " +
						" 0          0          0          0         21 " +
						"IR-PCI-MSI 59244544-edge      nvme0q4"
					return map[int]string{18: procFileLine}, nil
				},
			},
			expected: true,
			numCpus:  8,
		},
		{
			name: "Shall return false as device with IRQ 18 is a NVMe device" +
				"but queue number is larger than number of cpus",
			procFile: &irqProcFileMock{
				getIRQProcFileLinesMap: func() (map[int]string, error) {
					procFileLine := "18:          0          0          0         " +
						" 0          0          0          0         21 " +
						"IR-PCI-MSI 59244544-edge      nvme0q5"
					return map[int]string{18: procFileLine}, nil
				},
			},
			expected: false,
			numCpus:  4,
		},
		{
			name: "Shall return false as device with IRQ 18 is not NVMe device",
			procFile: &irqProcFileMock{
				getIRQProcFileLinesMap: func() (map[int]string, error) {
					procFileLine := "18:       1178       1469       3467" +
						"         96         17       3453       5932" +
						"        331  IR-PCI-MSI 333825-edge      iwlwifi: queue 1"
					return map[int]string{18: procFileLine}, nil
				},
			},
			expected: false,
			numCpus:  8,
		}}
	for _, test := range fields {
		t.Run(test.name, func(t *testing.T) {
			// given
			fs := afero.NewMemMapFs()
			irqDeviceInfo := &irqDeviceInfoMock{}
			proc := &procMock{}
			blockDevices := &blockDevices{
				fs:            fs,
				irqDeviceInfo: irqDeviceInfo,
				irqProcFile:   test.procFile,
				proc:          proc}
			// when
			isNvmeIRQ, err := blockDevices.isIRQNvmeFastPathIRQ(18, test.numCpus)
			// then
			assert.Nil(t, err)
			assert.Equal(t, test.expected, isNvmeIRQ)
		})
	}
}
