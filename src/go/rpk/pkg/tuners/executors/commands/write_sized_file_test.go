// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux
// +build linux

package commands_test

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors/commands"
	"github.com/stretchr/testify/require"
)

func TestWriteSizedFileCmdRender(t *testing.T) {
	cmd := commands.NewWriteSizedFileCmd(
		"/some/made/up/filepath.txt",
		int64(1),
	)

	expected := "truncate -s 1 /some/made/up/filepath.txt\n"
	var buf bytes.Buffer

	w := bufio.NewWriter(&buf)
	cmd.RenderScript(w)
	require.NoError(t, w.Flush())

	require.Equal(t, expected, buf.String())
}
