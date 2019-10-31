package utils_test

import (
	"bytes"
	"testing"
	"vectorized/pkg/utils"

	"github.com/spf13/afero"
)

func TestWriteBytes(t *testing.T) {
	fs := afero.NewMemMapFs()
	content := "redpanda:\nsome_field: somevalue"
	bs := []byte(content)
	filepath := "/tmp/testwritebytes.yaml"

	n, err := utils.WriteBytes(fs, bs, filepath)

	if n != len(bs) {
		t.Errorf("the number of bytes read doesn't match the number of bytes written")
	}
	if err != nil {
		t.Errorf("utils.WriteBytes failed with error %v", err)
	}
	buf := make([]byte, len(bs))
	file, err := fs.Open(filepath)
	if err != nil {
		t.Errorf("got an error while trying to open %v: %v", filepath, err)
	}
	if _, err := file.Read(buf); err != nil {
		t.Errorf("got an error while reading %v: %v", filepath, err)
	}
	if res := bytes.Compare(bs, buf); res != 0 {
		t.Errorf(
			"the contents read don't match the contents written: Got %v, wanted %v",
			buf,
			bs,
		)
	}
}
