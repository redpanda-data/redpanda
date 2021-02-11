module operator-e2e-tests

go 1.15

replace github.com/vectorizedio/redpanda/src/go/rpk => ../../../../rpk

replace github.com/vectorizedio/redpanda/src/go/k8s => ../../../

require (
	github.com/kudobuilder/kuttl v0.8.1-0.20210211045410-9567718388be
	github.com/stretchr/testify v1.7.0
	github.com/vectorizedio/redpanda/src/go/k8s v0.0.0-00010101000000-000000000000
	k8s.io/api v0.19.2
	k8s.io/apiextensions-apiserver v0.19.2
	k8s.io/apimachinery v0.19.2
	k8s.io/client-go v0.19.2
	k8s.io/utils v0.0.0-20200912215256-4140de9c8800
	sigs.k8s.io/controller-runtime v0.7.2
)
