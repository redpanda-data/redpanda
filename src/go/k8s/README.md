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

Optionaly to run operator locally:

* kind v0.9.0 or newer

### Installation

#### Local installation

Create local Kubernetes cluster using KIND

```
export KUBECONFIG=your/path/to/kubeconfig.yaml
kind create cluster --config kind.yaml
```

You can simply deploy the Redpanda operator by running the following command

```
kubectl apply -k https://github.com/vectorizedio/redpanda/src/go/k8s/config/default
```

Install sample RedpandaCluster custom resource

```
kubectl apply -f https://raw.githubusercontent.com/vectorizedio/redpanda/dev/src/go/k8s/config/samples/redpanda_v1alpha1_cluster.yaml
```

#### Clean up

To remove all resources even the running Redpanda cluster
please run the following command:

```
kubectl delete -k https://github.com/vectorizedio/redpanda/src/go/k8s/config/default
```
