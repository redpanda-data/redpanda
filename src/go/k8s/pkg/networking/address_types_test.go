// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package networking_test

import (
	"testing"

	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/networking"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
)

func TestGetPreferredAddress(t *testing.T) {
	tests := []struct {
		name               string
		inputNode          *corev1.Node
		inputPreferredType corev1.NodeAddressType
		expectedOutput     string
	}{
		{
			"missing node - no preference", nil,
			"", "",
		},
		{
			"missing node - with preference", nil,
			"some-preference", "",
		},
		{
			"node with external ip - no preference", &corev1.Node{
				Status: corev1.NodeStatus{
					Addresses: []corev1.NodeAddress{
						{
							Type:    corev1.NodeExternalIP,
							Address: "ip-external",
						},
						{
							Type:    corev1.NodeInternalIP,
							Address: "ip-internal",
						},
					},
				},
			}, "", "ip-external",
		},
		{
			"node without external ip - no preference", &corev1.Node{
				Status: corev1.NodeStatus{
					Addresses: []corev1.NodeAddress{
						{
							Type:    corev1.NodeInternalIP,
							Address: "ip-internal",
						},
						{
							Type:    corev1.NodeInternalDNS,
							Address: "dns-internal",
						},
					},
				},
			}, "", "",
		},
		{
			"node with internal and external ip - prefer internal", &corev1.Node{
				Status: corev1.NodeStatus{
					Addresses: []corev1.NodeAddress{
						{
							Type:    corev1.NodeInternalDNS,
							Address: "dns-internal",
						},
						{
							Type:    corev1.NodeExternalIP,
							Address: "ip-external",
						},
						{
							Type:    corev1.NodeInternalIP,
							Address: "ip-internal",
						},
					},
				},
			}, corev1.NodeInternalIP, "ip-internal",
		},
	}
	for _, tt := range tests {
		actual := networking.GetPreferredAddress(tt.inputNode, tt.inputPreferredType)
		assert.Equal(t, tt.expectedOutput, actual)
	}
}
