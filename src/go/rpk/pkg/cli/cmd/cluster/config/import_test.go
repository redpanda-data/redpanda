package config

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestClusterConfig_UnmarshalYAML(t *testing.T) {
	for _, test := range []struct {
		name string
		in   string
		exp  clusterConfig
	}{
		{
			name: "normal map",
			in: `aggregate_metrics: false
cloud_storage_access_key:
cloud_storage_api_endpoint_port: 443
cloud_storage_cache_size: 21474836480
cloud_storage_credentials_source: config_file
`,
			exp: map[string]any{
				"aggregate_metrics":                false,
				"cloud_storage_access_key":         nil,
				"cloud_storage_api_endpoint_port":  443,
				"cloud_storage_cache_size":         21474836480,
				"cloud_storage_credentials_source": "config_file",
			},
		}, {
			name: "normal map with timestamp",
			in: `cloud_storage_bucket: 2021-08-06
aggregate_metrics: true
cloud_storage_access_key:
cloud_storage_api_endpoint_port: 443
cloud_storage_cache_size: 21474836480
cloud_storage_credentials_source: config_file
`,
			exp: map[string]any{
				"cloud_storage_bucket":             "2021-08-06",
				"aggregate_metrics":                true,
				"cloud_storage_access_key":         nil,
				"cloud_storage_api_endpoint_port":  443,
				"cloud_storage_cache_size":         21474836480,
				"cloud_storage_credentials_source": "config_file",
			},
		}, {
			name: "map within a map",
			in: `cloud_storage_bucket: 2021-08-06
aggregate_metrics: true
nested_1:
  cloud_new_time: 2021-08-07
  int_test: 1234
  nested_2:
    another_time: 2023-01-25
    bool_test: true
`,
			exp: map[string]any{
				"cloud_storage_bucket": "2021-08-06",
				"aggregate_metrics":    true,
				"nested_1": map[string]any{
					"cloud_new_time": "2021-08-07",
					"int_test":       1234,
					"nested_2": map[string]any{
						"another_time": "2023-01-25",
						"bool_test":    true,
					},
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var cfg clusterConfig
			err := yaml.Unmarshal([]byte(test.in), &cfg)
			require.NoError(t, err)

			require.Equal(t, test.exp, cfg)
		})
	}
}
