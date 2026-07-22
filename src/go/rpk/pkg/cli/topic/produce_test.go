// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package topic

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFormatUsesPartition(t *testing.T) {
	for _, test := range []struct {
		name   string
		format string
		exp    bool
	}{
		{"default value format", "%v\n", false},
		{"key and value", "%k %v\n", false},
		{"explicit partition directive", "%p %v\n", true},
		{"partition with modifier", "%p{hex32} %v\n", true},
		{"partition not at the start", "%K{4}%p%v", true},
		{"escaped percent is not a directive", "%%p", false},
		{"escaped percent then real directive", "%%%p", true},
		{"brace escapes are skipped", "%{ %p %}", true},
		{"empty format", "", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.exp, formatUsesPartition(test.format))
		})
	}
}
