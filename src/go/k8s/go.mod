module github.com/vectorizedio/redpanda/src/go/k8s

go 1.16

require (
	github.com/Masterminds/semver/v3 v3.1.1
	github.com/banzaicloud/k8s-objectmatcher v1.5.1
	github.com/go-logr/logr v0.3.0
	github.com/hashicorp/go-multierror v1.1.0
	github.com/jetstack/cert-manager v1.2.0
	github.com/onsi/ginkgo v1.14.1
	github.com/onsi/gomega v1.10.2
	github.com/prometheus/client_golang v1.10.0
	github.com/spf13/afero v1.6.0
	github.com/stretchr/testify v1.7.0
	github.com/vectorizedio/redpanda/src/go/rpk v0.0.0-00010101000000-000000000000
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b
	k8s.io/api v0.19.2
	k8s.io/apimachinery v0.19.2
	k8s.io/client-go v0.19.2
	k8s.io/utils v0.0.0-20200912215256-4140de9c8800
	sigs.k8s.io/controller-runtime v0.7.2
)

replace github.com/vectorizedio/redpanda/src/go/rpk => ../rpk
