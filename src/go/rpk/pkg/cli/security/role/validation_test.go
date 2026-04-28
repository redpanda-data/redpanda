// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package role

import (
	"strings"
	"testing"
)

func TestValidateRoleNameForK8s(t *testing.T) {
	for _, tc := range []struct {
		name     string
		input    string
		wantOK   bool
		mustHave string
	}{
		{name: "lowercase simple", input: "my-role", wantOK: true},
		{name: "lowercase with dots", input: "team.read-only", wantOK: true},
		{name: "alphanumeric", input: "role01", wantOK: true},
		{name: "single char", input: "a", wantOK: true},

		{name: "uppercase rejected", input: "MyRole", wantOK: false, mustHave: "lower case"},
		{name: "leading hyphen rejected", input: "-foo", wantOK: false},
		{name: "trailing hyphen rejected", input: "foo-", wantOK: false},
		{name: "underscore rejected", input: "App_Role", wantOK: false},
		{name: "space rejected", input: "my role", wantOK: false},
		{name: "empty rejected", input: "", wantOK: false},
		{name: "too long rejected", input: strings.Repeat("a", 254), wantOK: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msgs := validateRoleNameForK8s(tc.input)
			gotOK := len(msgs) == 0
			if gotOK != tc.wantOK {
				t.Fatalf("validateRoleNameForK8s(%q) compliant=%v msgs=%v; want compliant=%v",
					tc.input, gotOK, msgs, tc.wantOK)
			}
			if tc.mustHave != "" {
				found := false
				for _, m := range msgs {
					if strings.Contains(m, tc.mustHave) {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("validateRoleNameForK8s(%q) msgs=%v; expected one to contain %q",
						tc.input, msgs, tc.mustHave)
				}
			}
		})
	}
}
