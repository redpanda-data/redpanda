// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package plugin

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
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
		test := test
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

func TestManifestPluginDownload(t *testing.T) {
	t.Parallel()

	var (
		pluginName     = "name"
		pluginPath     = "path/to"
		pluginArch     = "os_arch"
		pluginContents = "foo"
		pluginSha256   = sha256.Sum256([]byte(pluginContents))
		pluginSha      = hex.EncodeToString(pluginSha256[:])
	)

	for _, test := range []struct {
		name   string
		plugin ManifestPlugin

		failSha         bool
		failCompression bool
		expErr          bool
	}{
		{
			name: "valid no compression",
			plugin: ManifestPlugin{
				Name: pluginName,
				Path: pluginPath,
				OSArchShas: map[string]string{
					pluginArch: pluginSha,
				},
			},
		},

		{
			name: "valid with compression",
			plugin: ManifestPlugin{
				Name:        pluginName,
				Path:        pluginPath,
				Compression: "gzip",
				OSArchShas:  map[string]string{pluginArch: pluginSha},
			},
		},

		{
			name: "invalid missing name",
			plugin: ManifestPlugin{
				Path:       pluginPath,
				OSArchShas: map[string]string{pluginArch: pluginSha},
			},
			expErr: true,
		},

		{
			name: "invalid missing path",
			plugin: ManifestPlugin{
				Name:       pluginName,
				OSArchShas: map[string]string{pluginArch: pluginSha},
			},
			expErr: true,
		},

		{
			name: "invalid missing os_arch",
			plugin: ManifestPlugin{
				Name: pluginName,
				Path: pluginPath,
			},
			expErr: true,
		},

		{
			name: "invalid os_arch",
			plugin: ManifestPlugin{
				Name:       pluginName,
				Path:       pluginPath,
				OSArchShas: map[string]string{"unknown": pluginSha},
			},
			expErr: true,
		},

		{
			name: "unknown compression",
			plugin: ManifestPlugin{
				Name:        pluginName,
				Path:        pluginPath,
				Compression: "unknown",
				OSArchShas:  map[string]string{pluginArch: pluginSha},
			},
			expErr: true,
		},

		{
			name: "failed sha",
			plugin: ManifestPlugin{
				Name:       pluginName,
				Path:       pluginPath,
				OSArchShas: map[string]string{pluginArch: pluginSha},
			},
			failSha: true,
			expErr:  true,
		},

		{
			name: "failed gzip",
			plugin: ManifestPlugin{
				Name:        pluginName,
				Path:        pluginPath,
				Compression: "gzip",
				OSArchShas:  map[string]string{pluginArch: pluginSha},
			},
			failCompression: true,
			expErr:          true,
		},

		//
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			// First determine what we will serve to the user. If
			// the test indicates to fail the sha, we modify what
			// we serve pre-compression. If the test indicates to
			// fail compression, we modify what we compress.
			serve := []byte(pluginContents)
			if test.failSha {
				serve[0]++
			}
			if test.plugin.Compression == "gzip" {
				b := new(bytes.Buffer)
				gz := gzip.NewWriter(b) // errors skipped; cannot fail
				gz.Write(serve)
				gz.Close()
				serve = b.Bytes()

				if test.failCompression {
					serve[0]++
				}
			}

			svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				path := fmt.Sprintf("/%s/%s/%s/%s", pluginPath, pluginArch, pluginSha, pluginName)
				if r.URL.Path != path {
					t.Errorf("invalid request path %s != exp %s", r.URL.Path, path)
				}
				w.Write(serve)
			}))
			defer svr.Close()

			got, err := test.plugin.Download(svr.URL, "os", "arch")

			if gotErr := err != nil; gotErr != test.expErr {
				t.Errorf("got err? %v (%v) != exp err? %v", gotErr, err, test.expErr)
				return
			}
			if test.expErr {
				return
			}

			if string(got) != pluginContents {
				t.Errorf("got %s != exp %s", got, pluginContents)
			}
		})
	}
}
