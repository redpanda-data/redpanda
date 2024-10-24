/*
* Copyright 2023 Redpanda Data, Inc.
*
* Use of this software is governed by the Business Source License
* included in the file licenses/BSL.md
*
* As of the Change Date specified in that file, in accordance with
* the Business Source License, use of this software will be governed
* by the Apache License, Version 2.0
 */

package buildpack

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/testfs"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func writeTarFile(tw *tar.Writer, f string, c string) error {
	mode := int64(0o644)
	if filepath.Ext(f) == "" {
		mode = 0o755
	}
	err := tw.WriteHeader(&tar.Header{
		Name: f,
		Size: int64(len(c)),
		Mode: mode,
	})
	if err != nil {
		return err
	}
	_, err = io.Copy(tw, strings.NewReader(c))
	return err
}

func makeShaMap(bytes []byte) map[string]map[string]string {
	shasum := sha256.Sum256(bytes)
	encodedShasum := strings.ToLower(hex.EncodeToString(shasum[:]))
	return map[string]map[string]string{
		runtime.GOOS: {
			runtime.GOARCH: encodedShasum,
		},
	}
}

type FakeDownloader struct {
	Data []byte
}

func (d *FakeDownloader) Download(_ context.Context, _ *Buildpack, w io.Writer) error {
	_, err := io.Copy(w, bytes.NewReader(d.Data))
	return err
}

func createTestArtifacts() (*Buildpack, *FakeDownloader, error) {
	b := &bytes.Buffer{}
	gw := gzip.NewWriter(b)
	tw := tar.NewWriter(gw)

	err := writeTarFile(tw, "foobar/readme.txt", "hello, buildpack!")
	if err != nil {
		return nil, nil, err
	}
	err = writeTarFile(tw, "foobar/bin/foobar", "#!/bin/bash\nsudo rm -rf /")
	if err != nil {
		return nil, nil, err
	}
	err = writeTarFile(tw, "data/resources.json", "[1,1,2,3,5,8,13]")
	if err != nil {
		return nil, nil, err
	}
	err = tw.Close()
	if err != nil {
		return nil, nil, err
	}
	err = gw.Close()
	if err != nil {
		return nil, nil, err
	}

	output := b.Bytes()
	testingBuildpack := &Buildpack{
		Name:    "foobar",
		baseURL: "http://example.com/not-used",
		shaSums: makeShaMap(output),
	}
	return testingBuildpack, &FakeDownloader{output}, nil
}

func TestUnpacksTarball(t *testing.T) {
	fs := afero.NewMemMapFs()
	bp, dl, err := createTestArtifacts()
	require.NoError(t, err)
	sha, err := bp.PlatformSha()
	if err != nil {
		t.Skip("Skipping test on unsupported platform")
	}
	err = bp.DownloadTo(context.Background(), dl, "/opt", fs)
	require.NoError(t, err)
	testfs.ExpectExact(t, fs, map[string]testfs.Fmode{
		"/opt/.foobar-sha.txt":            testfs.RFile(sha),
		"/opt/foobar/readme.txt":          testfs.RFile("hello, buildpack!"),
		"/opt/foobar/bin/foobar":          {Contents: "#!/bin/bash\nsudo rm -rf /", Mode: 0o755},
		"/opt/foobar/data/resources.json": testfs.RFile("[1,1,2,3,5,8,13]"),
	})
}

func TestBlowsAwayExisting(t *testing.T) {
	fs := testfs.FromMap(map[string]testfs.Fmode{
		"/opt/foobar/readme.md":  testfs.RFile("surprise!"),
		"/opt/foobar/bin/foobar": testfs.RFile("here I am with different perms"),
	})
	bp, dl, err := createTestArtifacts()
	require.NoError(t, err)
	err = bp.DownloadTo(context.Background(), dl, "/opt", fs)
	require.NoError(t, err)
	sha, err := bp.PlatformSha()
	require.NoError(t, err)
	testfs.ExpectExact(t, fs, map[string]testfs.Fmode{
		"/opt/.foobar-sha.txt":            testfs.RFile(sha),
		"/opt/foobar/readme.txt":          testfs.RFile("hello, buildpack!"),
		"/opt/foobar/bin/foobar":          {Contents: "#!/bin/bash\nsudo rm -rf /", Mode: 0o755},
		"/opt/foobar/data/resources.json": testfs.RFile("[1,1,2,3,5,8,13]"),
	})
}

func TestVerifiesSha(t *testing.T) {
	fs := afero.NewMemMapFs()
	bp, dl, err := createTestArtifacts()
	require.NoError(t, err)
	bp.shaSums = makeShaMap([]byte("sha sum mismatch!"))
	err = bp.DownloadTo(context.Background(), dl, "/opt", fs)
	require.Error(t, err)
}
