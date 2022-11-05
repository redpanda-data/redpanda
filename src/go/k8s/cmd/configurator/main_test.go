// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package main

import (
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/stretchr/testify/assert"
)

func TestPopulateRack(t *testing.T) {
	cfg := &config.Config{}
	tests := []struct {
		Zone         string
		ZoneID       string
		ExpectedRack string
	}{
		{Zone: "", ZoneID: "", ExpectedRack: ""},
		{Zone: "zone", ZoneID: "", ExpectedRack: "zone"},
		{Zone: "", ZoneID: "zoneid", ExpectedRack: "zoneid"},
		{Zone: "zone", ZoneID: "zoneid", ExpectedRack: "zoneid"},
	}
	for _, tt := range tests {
		populateRack(cfg, tt.Zone, tt.ZoneID)
		assert.Equal(t, tt.ExpectedRack, cfg.Redpanda.Rack)
	}
}
