// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package config

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestRpkYamlVersion(t *testing.T) {
	s, err := formatType(RpkYaml{}, true)
	if err != nil {
		t.Fatal(err)
	}

	sha := sha256.Sum256([]byte(s))
	shastr := hex.EncodeToString(sha[:])

	const (
		v3sha = "cc2faa9790df9307d1fbf3e532128152d473e31a951152d2e2514a915269f880" // 24-04-14
	)

	if shastr != v3sha {
		t.Errorf("rpk.yaml type shape has changed (got sha %s != exp %s, if fields were reordered, update the valid v3 sha, otherwise bump the rpk.yaml version number", shastr, v3sha)
		t.Errorf("current shape:\n%s\n", s)
	}
}
