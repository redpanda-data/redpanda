// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cloud_test

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/cloud"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/vcloud/config"
)

func TestLogout(t *testing.T) {
	fs := afero.NewMemMapFs()
	configRW := config.NewVCloudConfigReaderWriter(fs)
	err := configRW.WriteToken("xxx")
	require.NoError(t, err)

	cmd := cloud.NewLogoutCommand(fs)
	err = cmd.Execute()
	require.NoError(t, err)

	token, err := configRW.ReadToken()
	require.NoError(t, err)
	if token != "" {
		t.Fatalf("Expecting empty token after logout called, got %s", token)
	}
}
