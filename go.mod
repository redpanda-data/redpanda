module github.com/redpanda-data/redpanda/src/go/k8s

go 1.16

require (
	github.com/Masterminds/semver/v3 v3.1.1
	github.com/banzaicloud/k8s-objectmatcher v1.7.0
	github.com/go-logr/logr v0.4.0
	github.com/hashicorp/go-multierror v1.1.0
	github.com/jetstack/cert-manager v1.2.0
	github.com/onsi/ginkgo v1.16.4
	github.com/onsi/gomega v1.15.0
	github.com/prometheus/client_golang v1.11.0
	github.com/spf13/afero v1.6.0
	github.com/stretchr/testify v1.7.0
	github.com/redpanda-data/redpanda/src/go/rpk v0.0.0-00010101000000-000000000000
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b
	k8s.io/api v0.21.4
	k8s.io/apimachinery v0.21.4
	k8s.io/client-go v0.21.4
	k8s.io/utils v0.0.0-20210802155522-efc7438f0176
	sigs.k8s.io/controller-runtime v0.9.7
)

replace github.com/redpanda-data/redpanda/src/go/rpk => ../rpk
