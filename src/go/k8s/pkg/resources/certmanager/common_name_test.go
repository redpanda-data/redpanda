// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package certmanager_test

import (
	"fmt"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources/certmanager"
	"github.com/stretchr/testify/assert"
)

func TestCommonName(t *testing.T) {
	tests := []struct {
		testName           string
		clusterName        string
		suffix             string
		expectedCommonName string
	}{
		{"short name and suffix", "cluster", "suffix", "cluster-suffix"},
		{"long name and suffix", "thisisverylongnamethatishittingthemaximal64characterlimitofnames", "suffix", "thisisverylongnamethatishittingthemaximal64characterlimit-suffix"},
		{"long name and long suffix", "thisisverylongnamethatishittingthemaximal64characterlimitofnames", "thisisverylongsuffixthathas40chars123456", "thisisverylongnamethati-thisisverylongsuffixthathas40chars123456"},
	}

	for _, tt := range tests {
		cn := certmanager.NewCommonName(tt.clusterName, tt.suffix)
		assert.Equal(t, tt.expectedCommonName, string(cn), fmt.Sprintf("%s: expecting common name to be equal", tt.testName))
	}
}
