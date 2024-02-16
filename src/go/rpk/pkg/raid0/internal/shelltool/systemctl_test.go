// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package shelltool

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSystemCTLDaemonReload(t *testing.T) {
	cmd, err := SystemCTL().
		DaemonReload().
		Build(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, "/usr/bin/systemctl daemon-reload", cmd.String())
}

func TestSystemCTLEnable(t *testing.T) {
	cmd, err := SystemCTL().
		Enable().
		Unit("var-lib-redpanda.mount").
		Build(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, "/usr/bin/systemctl enable var-lib-redpanda.mount", cmd.String())
}

func TestSystemCTLStart(t *testing.T) {
	cmd, err := SystemCTL().
		Start().
		Unit("var-lib-redpanda.mount").
		Build(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, "/usr/bin/systemctl start var-lib-redpanda.mount", cmd.String())
}

func TestSystemCTLOneSubcommandAllowed(t *testing.T) {
	cmd, err := SystemCTL().
		Start().
		DaemonReload().
		Enable().
		Unit("var-lib-redpanda.mount").
		Build(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, "/usr/bin/systemctl start var-lib-redpanda.mount", cmd.String())
}

func TestSystemCTLStartMultipleUnits(t *testing.T) {
	cmd, err := SystemCTL().
		Start().
		Unit("var-lib-redpanda.mount").
		Unit("redpanda.service").
		Build(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, "/usr/bin/systemctl start var-lib-redpanda.mount redpanda.service", cmd.String())
}
