// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package resources_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vectorizedio/redpanda/src/go/k8s/pkg/resources"
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
	cluster := pandaCluster()
	for _, tt := range tests {
		cluster.Name = tt.clusterName
		cn := resources.CertNameWithSuffix(cluster, tt.suffix)
		assert.Equal(t, tt.expectedCommonName, cn.Name, fmt.Sprintf("%s: expecting name to be equal", tt.testName))
	}
}
