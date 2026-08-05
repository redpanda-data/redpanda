// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package connect

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateVersion(t *testing.T) {
	// Connect minor versions passed 99 in 4.100.0, so segments must not be
	// capped in width.
	for _, ok := range []string{"latest", "4.99.0", "4.102.0", "v4.102.0", "4.102.0-rc1", "4.102.100"} {
		require.NoError(t, validateVersion(ok), ok)
	}
	for _, bad := range []string{"", "abc", "4", "4.102", "garbage", "4.102.0garbage", "4.102.0 "} {
		require.Error(t, validateVersion(bad), bad)
	}
}
