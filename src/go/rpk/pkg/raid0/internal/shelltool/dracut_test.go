// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build fedora

package shelltool

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDracutRegenerate(t *testing.T) {
	cmd, err := Dracut().
		RegenerateAll().
		Force().
		Build(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, "/usr/bin/dracut --regenerate-all --force", cmd.String())
}
