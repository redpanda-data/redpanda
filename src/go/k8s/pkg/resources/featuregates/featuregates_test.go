// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package featuregates_test

import (
	"testing"

	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/featuregates"
	"github.com/stretchr/testify/assert"
)

func TestFeatureGates(t *testing.T) { //nolint:funlen // table tests can be longer
	cases := []struct {
		version                  string
		shadowIndex              bool
		centralizedConfiguration bool
		maintenanceMode          bool
		perListenerAuthorization bool
		rackAwareness            bool
	}{
		{
			version:                  "v21.1.1",
			shadowIndex:              false,
			centralizedConfiguration: false,
			maintenanceMode:          false,
			perListenerAuthorization: false,
			rackAwareness:            false,
		},
		{
			version:                  "v21.11.1",
			shadowIndex:              true,
			centralizedConfiguration: false,
			maintenanceMode:          false,
			perListenerAuthorization: false,
			rackAwareness:            false,
		},
		{
			version:                  "v21.12.1",
			shadowIndex:              true,
			centralizedConfiguration: false,
			maintenanceMode:          false,
			perListenerAuthorization: false,
			rackAwareness:            false,
		},
		{
			version:                  "v22.1.1",
			shadowIndex:              true,
			centralizedConfiguration: true,
			maintenanceMode:          true,
			perListenerAuthorization: false,
			rackAwareness:            true,
		},
		{
			version:                  "v22.1.2",
			shadowIndex:              true,
			centralizedConfiguration: true,
			maintenanceMode:          true,
			perListenerAuthorization: false,
			rackAwareness:            true,
		},
		{
			version:                  "v22.2.1",
			shadowIndex:              true,
			centralizedConfiguration: true,
			maintenanceMode:          true,
			perListenerAuthorization: true,
			rackAwareness:            true,
		},
		{
			version:                  "dev",
			shadowIndex:              true,
			centralizedConfiguration: true,
			maintenanceMode:          true,
			perListenerAuthorization: true,
			rackAwareness:            true,
		},
		// Versions from: https://hub.docker.com/r/vectorized/redpanda/tags
		{
			version:                  "latest",
			shadowIndex:              true,
			centralizedConfiguration: true,
			maintenanceMode:          true,
			perListenerAuthorization: true,
			rackAwareness:            true,
		},
		{
			version:                  "v21.11.20-beta2",
			shadowIndex:              true,
			centralizedConfiguration: false,
			maintenanceMode:          false,
			perListenerAuthorization: false,
			rackAwareness:            false,
		},
		{
			version:                  "v21.11.20-beta2-amd64",
			shadowIndex:              true,
			centralizedConfiguration: false,
			maintenanceMode:          false,
			perListenerAuthorization: false,
			rackAwareness:            false,
		},
		{
			version:                  "v22.2.3-arm64",
			shadowIndex:              true,
			centralizedConfiguration: true,
			maintenanceMode:          true,
			perListenerAuthorization: true,
			rackAwareness:            true,
		},
		// Versions from: https://hub.docker.com/r/vectorized/redpanda-nightly/tags
		{
			version:                  "v0.0.0-20221006git23a658b",
			shadowIndex:              true,
			centralizedConfiguration: true,
			maintenanceMode:          true,
			perListenerAuthorization: true,
			rackAwareness:            true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.version, func(t *testing.T) {
			si := featuregates.ShadowIndex(tc.version)
			cc := featuregates.CentralizedConfiguration(tc.version)
			mm := featuregates.MaintenanceMode(tc.version)
			pl := featuregates.PerListenerAuthorization(tc.version)
			assert.Equal(t, tc.shadowIndex, si)
			assert.Equal(t, tc.centralizedConfiguration, cc)
			assert.Equal(t, tc.maintenanceMode, mm)
			assert.Equal(t, tc.perListenerAuthorization, pl)
		})
	}
}
