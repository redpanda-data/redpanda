// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package config_test

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/vcloud/config"
)

func TestReadWriteToken(t *testing.T) {
	fs := afero.NewMemMapFs()
	rw := config.NewVCloudConfigReaderWriter(fs)

	// read token before the config file exists
	_, err := rw.ReadToken()
	if err == nil || err != config.ErrConfigFileDoesNotExist {
		t.Errorf("expecting error with non-existent config, got %v", err)
	}

	// write token
	err = rw.WriteToken("token")
	if err != nil {
		t.Errorf("expecting no error writing token, got %v", err)
	}

	// read written token
	token, err := rw.ReadToken()
	if err != nil {
		t.Errorf("expecting no error reading token, got %v", err)
	}
	if token != "token" {
		t.Errorf("expecting token value 'token', got %s", token)
	}
}
