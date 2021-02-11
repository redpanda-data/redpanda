module github.com/vectorizedio/redpanda/src/go/k8s

go 1.15

require (
	github.com/Shopify/sarama v1.26.1
	github.com/go-logr/logr v0.3.0
	github.com/onsi/ginkgo v1.14.1
	github.com/onsi/gomega v1.10.2
	github.com/stretchr/testify v1.6.1
	github.com/vectorizedio/redpanda/src/go/rpk v0.0.0-00010101000000-000000000000
	golang.org/x/tools v0.0.0-20210107193943-4ed967dd8eff // indirect
	google.golang.org/protobuf v1.25.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20200615113413-eeeca48fe776
	k8s.io/api v0.19.2
	k8s.io/apimachinery v0.19.2
	k8s.io/client-go v0.19.2
	k8s.io/utils v0.0.0-20200912215256-4140de9c8800
	sigs.k8s.io/controller-runtime v0.7.2
)

replace github.com/vectorizedio/redpanda/src/go/rpk => ../rpk
