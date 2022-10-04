package resources_test

import (
	"fmt"
	"testing"

	"github.com/go-logr/logr"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIngressWithTLS(t *testing.T) {
	table := []struct {
		defaultEndpoint string
		host            string
		tlsSecret       string
		tlsIssuer       string
		annotations     map[string]string
		tlsHosts        []string
	}{
		{
			host:        "test.example.local",
			tlsSecret:   "rp-abc123-redpanda",
			tlsIssuer:   resources.LEClusterIssuer,
			annotations: map[string]string{"foo.vectorized.io": "bar"},
			tlsHosts:    []string{"test.example.local", "*.test.example.local"},
		},
		{
			defaultEndpoint: "console",
			host:            "test.example.local",
			tlsSecret:       "rp-abc123-redpanda",
			tlsIssuer:       resources.LEClusterIssuer,
			tlsHosts:        []string{"test.example.local", "*.test.example.local"},
		},
	}
	for i, tt := range table {
		t.Run(fmt.Sprintf("%s-%d", tt.host, i), func(t *testing.T) {
			ingress := resources.NewIngress(nil, nil, nil, tt.host, "", "", logr.Discard()).
				WithDefaultEndpoint(tt.defaultEndpoint).
				WithTLS(tt.tlsIssuer, tt.tlsSecret).
				WithAnnotations(tt.annotations)

			annotations := ingress.GetAnnotations()

			issuer, ok := annotations["cert-manager.io/cluster-issuer"]
			require.True(t, ok)
			require.Equal(t, tt.tlsIssuer, issuer)

			sslRedirect, ok := annotations["nginx.ingress.kubernetes.io/force-ssl-redirect"]
			require.True(t, ok)
			require.Equal(t, "true", sslRedirect)

			for k, v := range tt.annotations {
				val, ok := annotations[k]
				require.True(t, ok)
				require.Equal(t, v, val)
			}

			require.Len(t, ingress.TLS, 1)
			assert.Equal(t, ingress.TLS[0].Hosts, tt.tlsHosts)
			assert.Equal(t, ingress.TLS[0].SecretName, tt.tlsSecret)
		})
	}
}
