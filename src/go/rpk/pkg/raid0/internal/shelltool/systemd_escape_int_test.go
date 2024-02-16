// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build integration

package shelltool

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSystemdEscapeOutput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	cmd, err := SystemdEscape("/var/lib/redpanda").
		Path().
		Suffix("mount").
		Build(context.Background())
	assert.NoError(t, err)

	unitName, err := cmd.Output()
	assert.NoError(t, err)
	escapedPath := strings.TrimSpace(string(unitName))
	assert.Equal(t, "var-lib-redpanda.mount", escapedPath)
}
