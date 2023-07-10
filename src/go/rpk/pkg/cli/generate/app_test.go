package generate

import (
	"crypto/sha256"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func Test_extractTarGzAndReplace(t *testing.T) {
	type tt struct {
		name    string
		data    tmplData
		file    string
		out     string
		expHash string
		expDest string
	}

	var tests []tt
	for k, v := range fileTplMap {
		t := []tt{
			{
				name: fmt.Sprintf("parse and execute %v template correctly - full", k),
				file: filepath.Join(templateDir, v.Location),
				out:  "/tmp/full",
				data: tmplData{
					IsTLS:      true,
					Username:   "foo",
					Password:   "bar",
					ScramBits:  "256",
					SeedServer: []string{"127.0.0.1:9092", "redpanda.com"},
				},
				expDest: "/tmp/full/redpanda-starter-app",
				expHash: v.Hash,
			}, {
				name: fmt.Sprintf("parse and execute %v template correctly - no user:pw", k),
				out:  "/tmp/no-user",
				file: filepath.Join(templateDir, v.Location),
				data: tmplData{
					ScramBits:  "256",
					SeedServer: []string{"127.0.0.1:9092", "redpanda.com"},
				},
				expDest: "/tmp/no-user/redpanda-starter-app",
				expHash: v.Hash,
			}, {
				name: fmt.Sprintf("parse and execute %v template correctly - no TLS", k),
				out:  "/tmp/no-tls",
				file: filepath.Join(templateDir, v.Location),
				data: tmplData{
					Username:   "foo",
					Password:   "bar",
					ScramBits:  "256",
					SeedServer: []string{"127.0.0.1:9092", "redpanda.com"},
				},
				expDest: "/tmp/no-tls/redpanda-starter-app",
				expHash: v.Hash,
			}, {
				name: fmt.Sprintf("parse and execute %v template correctly - extract in pwd", k),
				file: filepath.Join(templateDir, v.Location),
				data: tmplData{
					Username:   "foo",
					Password:   "bar",
					ScramBits:  "256",
					SeedServer: []string{"127.0.0.1:9092", "redpanda.com"},
				},
				expDest: "redpanda-starter-app",
				expHash: v.Hash,
			},
		}
		tests = append(tests, t...)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			destFS := afero.NewMemMapFs()
			replace, err := extractTarGzAndReplace(tplFS, destFS, test.data, test.file, test.out)
			require.NoError(t, err)
			require.Equal(t, test.expDest, replace)

			// This is just a blind test, we only care atm that we can extract
			// and execute the template.
			exists, err := afero.Exists(destFS, test.expDest)
			require.NoError(t, err)
			require.Equal(t, true, exists)

			// And we compare the hash of the tar file, as a blind safeguard.
			f, err := tplFS.Open(test.file)
			require.NoError(t, err)
			defer f.Close()

			h := sha256.New()
			_, err = io.Copy(h, f)
			require.NoError(t, err)

			require.Equal(t, test.expHash, fmt.Sprintf("%x", h.Sum(nil)))
		})
	}
}
