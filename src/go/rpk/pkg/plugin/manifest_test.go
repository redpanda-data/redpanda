package plugin

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDownloadManifest(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name  string
		serve string

		exp    *Manifest
		expErr bool
	}{

		// This first test is our golden test; we ensure we can decode
		// every field properly, invalid fields are ignored, and things
		// are returned in alphabetical order.
		{
			name: "valid 2021-07-27",
			serve: `
api_version: 2021-07-27

plugins:
  - name: test
    description: Description
    path: path/to/plugin/test
    compression: gzip
    help_autocomplete: true
    os_arch_shas:
      darwin_amd64: foo
      darwin_arm64: bar
      linux_amd64: biz
      linux_arm64: baz

  - name: alphabeticallyfirst
`,

			exp: &Manifest{
				Version: "2021-07-27",
				Plugins: []ManifestPlugin{
					{
						Name: "alphabeticallyfirst",
					},

					{
						Name:             "test",
						Description:      "Description",
						Path:             "path/to/plugin/test",
						Compression:      "gzip",
						HelpAutoComplete: true,
						OSArchShas: map[string]string{
							"darwin_amd64": "foo",
							"darwin_arm64": "bar",
							"linux_amd64":  "biz",
							"linux_arm64":  "baz",
						},
					},
				},
			},
		},

		// The second test shows that a manifest with just a version is
		// valid, which in our next test allows us to how that changing
		// just the version breaks things.
		{
			name:  "valid 2021-07-27 empty",
			serve: `api_version: 2021-07-27`,

			exp: &Manifest{
				Version: "2021-07-27",
			},
		},

		// We now ensure that unknown versions are detected.
		{
			name:   "unknown version",
			serve:  `api_version: 2021-06-27`,
			expErr: true,
		},

		// The rest of the download code is just standard request
		// issuing and downloading.
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			const path = "/path"
			svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != path {
					t.Errorf("invalid request path %s != exp %s", r.URL.Path, path)
				}
				io.WriteString(w, test.serve)
			}))
			defer svr.Close()

			m, err := DownloadManifest(svr.URL + path)

			if gotErr := err != nil; gotErr != test.expErr {
				t.Errorf("got err? %v != exp err? %v", gotErr, test.expErr)
				return
			}
			if test.expErr {
				return
			}

			require.Equal(t, test.exp, m, "received manifest is not equal to expected manifest")
		})
	}
}
