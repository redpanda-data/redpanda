// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package check

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateVersion(t *testing.T) {
	for _, ok := range []string{"latest", "0.1.0", "v0.1.0", "0.1.0-rc1", "0.100.0"} {
		require.NoError(t, validateVersion(ok), ok)
	}
	for _, bad := range []string{"", "abc", "0", "garbage", "0.1.0garbage"} {
		require.Error(t, validateVersion(bad), bad)
	}
}
