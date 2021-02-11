# Redpanda Operator

[Official Redpanda Kubernetes operator](https://github.com/vectorizedio/redpanda/src/go/k8s)
automates tasks related to managing Redpanda clusters in Kubernetes. It is built using the
[kubebuilder project](https://github.com/kubernetes-sigs/kubebuilder).

[Redpanda](https://github.com/vectorizedio/redpanda) is a streaming platform for mission critical
workloads. Kafka® compatible, No Zookeeper®, no JVM, and no code changes required.
Use all your favorite open source tooling - 10x faster.

## Getting started

### Requirements

* Kubernetes 1.16 or newer
* kubectl 1.16 or newer
* kustomize v3.8.7 or newer

Optionaly to run operator locally:

* kind v0.9.0 or newer

### Installation

#### Local installation

First clone the repo

```
git clone https://github.com/vectorizedio/redpanda.git
```

Create local Kubernetes cluster using KIND

```
export KUBECONFIG=your/path/to/kubeconfig.yaml
kind create cluster --config kind.yaml
```

You can simply deploy the latest version stored in GCR

```
make deploy
```

Or you can build and load the container to Kubernetes cluster

```
# Only if you want to build container from source!
make docker-build deploy-to-kind
```

Install sample RedpandaCluster custom resource

```
kubectl apply -f config/samples/redpanda_v1alpha1_cluster.yaml
```
