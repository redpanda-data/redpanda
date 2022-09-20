package resources_test

import (
	"testing"

	"github.com/go-logr/logr"
	"github.com/redpanda-data/redpanda/src/go/k8s/pkg/resources"
	"github.com/stretchr/testify/require"
)

func TestIngressWithTLS(t *testing.T) {
	table := []struct {
		host        string
		tlsSecret   string
		tlsIssuer   string
		annotations map[string]string
	}{
		{
			host:        "test.example.local",
			tlsSecret:   "rp-abc123-redpanda",
			tlsIssuer:   resources.LEClusterIssuer,
			annotations: map[string]string{"foo.vectorized.io": "bar"},
		},
	}
	for _, tt := range table {
		ingress := resources.NewIngress(nil, nil, nil, tt.host, "", "", logr.Discard()).WithTLS(tt.tlsIssuer, tt.tlsSecret).WithAnnotations(tt.annotations)
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

		var found bool
		for _, tls := range ingress.TLS {
			// Host and SecretName should be in same TLS element
			var foundHost bool
			for _, host := range tls.Hosts {
				if host == tt.host {
					foundHost = true
				}
			}
			if foundHost && tls.SecretName == tt.tlsSecret {
				found = true
			}
		}
		require.True(t, found)
	}
}
