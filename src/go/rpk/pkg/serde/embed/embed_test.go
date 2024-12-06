package embed

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEmbeddedFiles(t *testing.T) {
	t.Run("Test Embedded files in rpk, equal to Redpanda", func(t *testing.T) {
		// /src/v/pandaproxy/schema_registry/protobuf
		redpandaProtoFS := os.DirFS("../../../../../v/pandaproxy/schema_registry/protobuf/")
		redpandaMap := make(map[string]string)
		err := fs.WalkDir(redpandaProtoFS, ".", func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || filepath.Ext(path) != ".proto" {
				return nil
			}
			data, err := fs.ReadFile(redpandaProtoFS, path)
			if err == nil {
				redpandaMap[path] = string(data)
			}
			return nil
		})

		embeddedMap, err := CommonProtoFileMap()
		require.NoError(t, err)

		for path, embedContent := range embeddedMap {
			if rpContent, ok := redpandaMap[path]; ok {
				require.Equalf(t, rpContent, embedContent, "Contents of %v have changed vs the embedded rpk files", path)
			} else {
				t.Fatalf("%s not found in Redpanda files", path)
			}
		}
	})
}
