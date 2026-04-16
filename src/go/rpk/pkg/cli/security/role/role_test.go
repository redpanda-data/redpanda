// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package role

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParsePrincipal(t *testing.T) {
	tests := []struct {
		input    string
		wantType string
		wantName string
	}{
		{input: "alice", wantType: "User", wantName: "alice"},
		{input: "User:alice", wantType: "User", wantName: "alice"},
		{input: "Group:engineering", wantType: "Group", wantName: "engineering"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			gotType, gotName := parsePrincipal(tt.input)
			require.Equal(t, tt.wantType, gotType)
			require.Equal(t, tt.wantName, gotName)
		})
	}
}
