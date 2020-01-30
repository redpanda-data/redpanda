package system

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

func Test_getCgroupMemLimitBytes(t *testing.T) {
	//shall return correct cgroups mem limit value
	// given
	fs := afero.NewMemMapFs()
	fs.MkdirAll("/sys/fs/cgroup/memory/", 0755)
	afero.WriteFile(fs, "/sys/fs/cgroup/memory/memory.limit_in_bytes",
		[]byte("12345687\n"), 0644)
	// when
	limit, err := getCgroupMemLimitBytes(fs)
	// then
	assert.NoError(t, err)
	assert.Equal(t, uint64(12345687), limit)
}
