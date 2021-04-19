---
title: Kubernetes Quick Start Guide
order: 0
---
# Kubernetes Quick Start Guide

Redpanda is a modern [streaming platform](/blog/intelligent-data-api/) for mission critical workloads.
With Redpanda you can get up and running with streaming quickly
and be fully compatible with the [Kafka ecosystem](https://cwiki.apache.org/confluence/display/KAFKA/Ecosystem).

This quick start guide can help you get started with Redpanda for development and testing purposes.
For production or benchmarking, setup a [production deployment](/docs/production-deployment).

Using [Helm](https://helm.sh/) is the fastest way to get started with Redpanda on Kubernetes.
To get up and running you need to create a cluster and deploy the Redpanda operator on the cluster.

> **_Note_** - Run a container inside the Kubernetes cluster to communicate with the Redpanda cluster.
> Currently, a load balancer is not automatically created during deployment by default.

## Prerequisites

Before you start installing Redpanda you need to setup your Kubernetes environment.

### Install Kubernetes, Helm, and cert-manager 

You'll need to install:

- Kubernetes v1.16 or above
- [kubectl](https://kubernetes.io/docs/tasks/tools/) v1.16 or above
- [helm](https://github.com/helm/helm/releases) v3.0.0 or above
- [cert-manager](https://cert-manager.io/docs/installation/kubernetes/) v1.3.0 or above

    Follow the instructions to verify that cert-manager is ready to create certificates.

### Create a Kubernetes cluster

You can either create a Kubernetes cluster on your local machine or on a cloud provider.

<tabs>

  <tab id="Kind">

  [Kind](https://kind.sigs.k8s.io) is a tool that lets you create local Kubernetes clusters using Docker.
    After you install Kind, set up a cluster with:

  ```
  kind create cluster
  ```

  </tab>

  <tab id="AWS EKS">

  Use the [EKS Getting Started](https://docs.aws.amazon.com/eks/latest/userguide/getting-started-eksctl.html) guide to set up EKS.
  When you finish, you'll have `eksctl` installed so that you can create and delete clusters in EKS.
  Then, create an EKS cluster with:

  ```
  eksctl create cluster \
  --name redpanda \
  --nodegroup-name standard-workers \
  --node-type m5.xlarge \
  --nodes 3 \
  --nodes-min 1 \
  --nodes-max 4 \
  --node-ami auto
  ```

  It will take about 10-15 minutes for the process to finish.

  </tab>

  <tab id="Google GKE">

  First complete the "Before You Begin" steps describe in [Google Kubernetes Engine Quickstart](https://cloud.google.com/kubernetes-engine/docs/quickstart).
  Then, create a cluster with:

  ```
  gcloud container clusters create redpanda --machine-type n1-standard-4
  ```

  </tab>
</tabs>

## Using Helm to Install Redpanda

1. Using Helm, add the Redpanda chart repository and update it:

    ```
    helm repo add redpanda https://charts.vectorized.io/ && helm repo update
    ```

2. Install the Redpanda operator CRDs:

    ```
    kubectl apply -k 'https://github.com/vectorizedio/redpanda/src/go/k8s/config/crd?ref=<latest version>'
    ```

    You can find the latest version number of the operator on the [list of operator releases](https://github.com/vectorizedio/redpanda/releases).

3. Install Redpanda operator on your Kubernetes cluster with:

    ```
    helm install --namespace redpanda-system --create-namespace redpanda-operator redpanda/redpanda-operator
    ```

## Next steps

After you set up Redpanda in your Kubernetes cluster, you can use our samples to install resources and see Redpanda in action:

- Create a namespace for your resources:

    ```
    kubectl create ns redpanda-test
    ```

- Install resources from [our sample files](https://github.com/vectorizedio/redpanda/tree/dev/src/go/k8s/config/samples), for example the single-node cluster:
                
    ```
    kubectl apply -n redpanda-test -f https://raw.githubusercontent.com/vectorizedio/redpanda/dev/src/go/k8s/config/samples/one_node_cluster.yaml
    ```

- Review the [cluster_types file](https://github.com/vectorizedio/redpanda/blob/dev/src/go/k8s/apis/redpanda/v1alpha1/cluster_types.go) to see the resource configuration options.

- Use `rpk` to work with your Redpanda nodes, for example:

    - Create a topic:

        ```
        rpk topic create test-topic-name --brokers cluster-sample-tls-0.cluster-sample-tls.redpanda-test.svc.cluster.local:9092
        ```

    - Show the list of topics:

        ```
        rpk topic list --brokers cluster-sample-tls-0.cluster-sample-tls.redpanda-test.svc.cluster.local:9092
        ```

    - Produce a message to the topic:

        ```
        echo {"test":"message"} | rpk topic produce test-topic-name --brokers cluster-sample-tls-0.cluster-sample-tls.redpanda-test.svc.cluster.local:9092
        ```

    - Consume the message from the topic:

        ```
        rpk topic consume test-topic-name --brokers cluster-sample-tls-0.cluster-sample-tls.redpanda-test.svc.cluster.local:9092
        ```
    
