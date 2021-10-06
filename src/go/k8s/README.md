# Redpanda Operator

[Official Redpanda Kubernetes operator](https://github.com/vectorizedio/redpanda/src/go/k8s)
automates tasks related to managing Redpanda clusters in Kubernetes. It is built using the
[kubebuilder project](https://github.com/kubernetes-sigs/kubebuilder).

[Redpanda](https://github.com/vectorizedio/redpanda) is a streaming platform for mission critical
workloads. Kafka® compatible, No Zookeeper®, no JVM, and no code changes required.
Use all your favorite open source tooling - 10x faster.

## Getting started

Official Kubernetes quick start documentation can be found at
[https://vectorized.io/docs/](https://vectorized.io/docs/quick-start-kubernetes)

### Requirements

* Kubernetes 1.16 or newer
* kubectl 1.16 or newer
* kustomize v3.8.7 or newer
* cert-manager v1.0.0 or newer

Optionaly to run operator locally:

* kind v0.9.0 or newer

### Installation

#### Local installation

Create local Kubernetes cluster using KIND

```
export KUBECONFIG=your/path/to/kubeconfig.yaml
kind create cluster --config kind.yaml
```

In order to have validating webhook the cert manager needs to be
installed. Please follow 
[the installation guide](https://cert-manager.io/docs/installation/)

The cert manager needs around 1 minute to be ready. The Redpanda
operator will create Issuer and Certificate custom resource. The
webhook of cert-manager will prevent from creating mentioned
resources. To verify that cert manager is ready please follow
[the verifying the installation](https://cert-manager.io/docs/installation/kubernetes/#verifying-the-installation)

You can simply deploy the Redpanda operator with webhook (recommended) by running the following command

```
kubectl apply -k https://github.com/vectorizedio/redpanda/src/go/k8s/config/default
```

You can deploy the Redpanda operator without webhook by running the following command:

```
kubectl apply -k https://github.com/vectorizedio/redpanda/src/go/k8s/config/without-webhook
```

Install sample RedpandaCluster custom resource

```
kubectl apply -f https://raw.githubusercontent.com/vectorizedio/redpanda/dev/src/go/k8s/config/samples/one_node_cluster.yaml
```

#### Developing


Create kind cluster

```
make kind-create
```

Install cert manager

```
make certmanager-install
```

Build docker images for manager and configurator

```
make docker-build
make docker-build-configurator
```

Deploy operator to kind

```
make deploy-to-kind
```

#### Clean up

To remove all resources even the running Redpanda cluster
please run the following command:

```
kubectl delete -k https://github.com/vectorizedio/redpanda/src/go/k8s/config/default
```

#### Running e2e tests

Before you execute any kuttl tests, that are defined in e2e test Makefile target,
please create kind cluster.