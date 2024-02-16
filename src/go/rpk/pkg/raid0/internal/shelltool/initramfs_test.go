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

func TestUpdateInitRAMFS(t *testing.T) {
	cmd, err := InitRAMFS().
		Update().
		Build(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, "/usr/sbin/update-initramfs -u", cmd.String())
}
