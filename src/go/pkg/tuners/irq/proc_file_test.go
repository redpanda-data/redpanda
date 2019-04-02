package irq

import (
	"testing"
	"vectorized/utils"

	"github.com/spf13/afero"
	"gotest.tools/assert"
)

var procFileLines = []string{
	"1:     184233          0          0       7985   IO-APIC   1-edge      i8042",
	"5:          0          0          0          0   IO-APIC   5-edge      parport0",
	"8:          1          0          0          0   IO-APIC   8-edge      rtc0"}

func TestProcFile_GetIRQProcFileLinesMap(t *testing.T) {
	// given
	var fs = afero.NewMemMapFs()
	_ = utils.WriteFileLines(fs, procFileLines, "/proc/interrupts")
	procFile := NewProcFile(fs)
	// when
	procFileLinesMap, err := procFile.GetIRQProcFileLinesMap()
	// then
	assert.NilError(t, err)
	assert.Equal(t, len(procFileLinesMap), 3)
	assert.Equal(t, procFileLinesMap["1"], procFileLines[0])
	assert.Equal(t, procFileLinesMap["5"], procFileLines[1])
	assert.Equal(t, procFileLinesMap["8"], procFileLines[2])
}
