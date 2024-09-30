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
		expsha = "f3d394e88ba4a6a668d15b74208f32d71c82f8a0e9b6e6445defcdd64df1e37f" // 24-07-24
	)

	if shastr != expsha {
		t.Errorf("rpk.yaml type shape has changed (got sha %s != exp %s, if fields were reordered, update the valid v3 sha, otherwise bump the rpk.yaml version number", shastr, expsha)
		t.Errorf("current shape:\n%s\n", s)
	}
}
