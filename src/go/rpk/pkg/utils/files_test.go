package utils_test

import (
	"testing"
	"vectorized/pkg/utils"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestWriteBytes(t *testing.T) {
	fs := afero.NewMemMapFs()
	content := "redpanda:\nsome_field: somevalue"
	bs := []byte(content)
	filepath := "/tmp/testwritebytes.yaml"

	n, err := utils.WriteBytes(fs, bs, filepath)
	require.Equal(t, len(bs), n, "the number of bytes read doesn't match the number of bytes written")
	require.NoError(t, err)
	buf := make([]byte, len(bs))
	file, err := fs.Open(filepath)
	require.NoError(t, err)
	_, err = file.Read(buf)
	require.NoError(t, err)
	require.Exactly(t, bs, buf)
}
