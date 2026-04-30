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

	"github.com/stretchr/testify/require"
)

func TestRpkYamlVersion(t *testing.T) {
	s, err := formatType(RpkYaml{}, true)
	if err != nil {
		t.Fatal(err)
	}

	sha := sha256.Sum256([]byte(s))
	shastr := hex.EncodeToString(sha[:])

	const (
		expsha = "1032b2c2d3757baed2b319a05f7e942f475058155c8dea8904dc54a9a3a26286" // 26-04-28
	)

	if shastr != expsha {
		t.Errorf("rpk.yaml type shape has changed (got sha %s != exp %s, if fields were reordered, update the valid v3 sha, otherwise bump the rpk.yaml version number", shastr, expsha)
		t.Errorf("current shape:\n%s\n", s)
	}
}

func TestIsCloudBrokerURL(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		expected bool
	}{
		{
			name:     "cloud.redpanda.com URL",
			url:      "seed-123.abc.cloud.redpanda.com:9092",
			expected: true,
		},
		{
			name:     "byoc cluster URL",
			url:      "seed-7c375997.d4bpfk5875ie32jp5ut0.byoc.ign.cloud.redpanda.com:9644",
			expected: true,
		},
		{
			name:     "ign.cloud.redpanda.com URL",
			url:      "example.ign.cloud.redpanda.com:9092",
			expected: true,
		},
		{
			name:     "localhost",
			url:      "localhost:9092",
			expected: false,
		},
		{
			name:     "self-hosted IP",
			url:      "192.168.1.100:9092",
			expected: false,
		},
		{
			name:     "self-hosted domain",
			url:      "redpanda.example.com:9092",
			expected: false,
		},
		{
			name:     "case insensitive cloud URL",
			url:      "SEED-123.ABC.CLOUD.REDPANDA.COM:9092",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isLikelyCloudBrokerURL(tt.url)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestIsLikelyCloudCluster(t *testing.T) {
	tests := []struct {
		name     string
		profile  *RpkProfile
		expected bool
	}{
		{
			name: "cloud cluster with kafka broker",
			profile: &RpkProfile{
				KafkaAPI: RpkKafkaAPI{
					Brokers: []string{"seed-123.abc.cloud.redpanda.com:9092"},
				},
			},
			expected: true,
		},
		{
			name: "cloud cluster with admin API",
			profile: &RpkProfile{
				AdminAPI: RpkAdminAPI{
					Addresses: []string{"seed-123.abc.byoc.ign.cloud.redpanda.com:9644"},
				},
			},
			expected: true,
		},
		{
			name: "self-hosted cluster",
			profile: &RpkProfile{
				KafkaAPI: RpkKafkaAPI{
					Brokers: []string{"localhost:9092"},
				},
				AdminAPI: RpkAdminAPI{
					Addresses: []string{"localhost:9644"},
				},
			},
			expected: false,
		},
		{
			name: "mixed URLs (one cloud)",
			profile: &RpkProfile{
				KafkaAPI: RpkKafkaAPI{
					Brokers: []string{
						"localhost:9092",
						"seed-123.abc.cloud.redpanda.com:9092",
					},
				},
			},
			expected: true,
		},
		{
			name: "empty profile",
			profile: &RpkProfile{
				KafkaAPI: RpkKafkaAPI{
					Brokers: []string{},
				},
				AdminAPI: RpkAdminAPI{
					Addresses: []string{},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsLikelyCloudCluster(tt.profile)
			require.Equal(t, tt.expected, result)
		})
	}
}
