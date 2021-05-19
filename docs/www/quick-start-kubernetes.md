---
title: Kubernetes Quick Start Guide
order: 0
---
# Kubernetes Quick Start Guide

Redpanda is a modern [streaming platform](/blog/intelligent-data-api/) for mission critical workloads.
With Redpanda you can get up and running with streaming quickly
and be fully compatible with the [Kafka ecosystem](https://cwiki.apache.org/confluence/display/KAFKA/Ecosystem).

This quick start guide can help you get started with Redpanda for development and testing purposes.
To get up and running you need to create a cluster and deploy the Redpanda operator on the cluster.

- For production or benchmarking, set up a [production deployment](/docs/production-deployment).
- You can also set up a [Kubernetes cluster with external access](/docs/kubernetes-external-connect).

> **_Note_** - Run a container inside the Kubernetes cluster to communicate with the Redpanda cluster.
> Currently, a load balancer is not automatically created during deployment by default.

## Prerequisites

Before you start installing Redpanda you need to setup your Kubernetes environment.

### Install Kubernetes, Helm, and cert-manager 

You'll need to install:

- Kubernetes v1.16 or above
- [kubectl](https://kubernetes.io/docs/tasks/tools/) v1.16 or above
- [helm](https://github.com/helm/helm/releases) v3.0.0 or above
- [cert-manager](https://cert-manager.io/docs/installation/kubernetes/) v1.2.0 or above

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
  --nodes 1 \
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
  gcloud container clusters create redpanda --machine-type n1-standard-4 --num-nodes=1
  ```

  **_Note_** - You may need to add a `--region` or `--zone` to this command.

  </tab>
</tabs>

## Install cert-manager

The Redpanda operator requires cert-manager to create certificates for TLS communication.
You can [install cert-manager with a CRD](https://cert-manager.io/docs/installation/kubernetes/#installing-with-helm),
but here's the command to install using helm:

```
helm repo add jetstack https://charts.jetstack.io && \
helm repo update && \
helm install \
  cert-manager jetstack/cert-manager \
  --namespace cert-manager \
  --create-namespace \
  --version v1.2.0 \
  --set installCRDs=true
```

We recommend that you use [the verification procedure](https://cert-manager.io/docs/installation/kubernetes/#verifying-the-installation) in the cert-manager docs
to verify that cert-manager is working correcly.

## Use Helm to install Redpanda operator

1. Using Helm, add the Redpanda chart repository and update it:

    ```
    helm repo add redpanda https://charts.vectorized.io/ && \
    helm repo update
    ```

2. Just to simplify the commands, create a variable for the version number:

    ```
    export VERSION=v21.4.15
    ```

    **_Note_** - You can find the latest version number of the operator in the [list of operator releases](https://github.com/vectorizedio/redpanda/releases).

3. Install the Redpanda operator CRD:

    ```
    kubectl apply \
    -k https://github.com/vectorizedio/redpanda/src/go/k8s/config/crd?ref=$VERSION
    ```

4. Install the Redpanda operator on your Kubernetes cluster with:

    ```
    helm install \
    --namespace redpanda-system \
    --create-namespace redpanda-operator \
    --version $VERSION \
    redpanda/redpanda-operator
    ```

## Install and connect to a Redpanda cluster

After you set up Redpanda in your Kubernetes cluster, you can use our samples to install a cluster and see Redpanda in action.

Let's try setting up a Redpanda topic to handle a stream of events from a chat application with 5 chat rooms:

1. Create a namespace for your cluster:

    ```
    kubectl create ns chat-with-me
    ```

2. Install a cluster from [our sample files](https://github.com/vectorizedio/redpanda/tree/dev/src/go/k8s/config/samples), for example the single-node cluster:
                
    ```
    kubectl apply \
    -n chat-with-me \
    -f https://raw.githubusercontent.com/vectorizedio/redpanda/dev/src/go/k8s/config/samples/one_node_cluster.yaml
    ```

    You can see the resource configuration options in the [cluster_types file](https://github.com/vectorizedio/redpanda/blob/dev/src/go/k8s/apis/redpanda/v1alpha1/cluster_types.go).

3. Use `rpk` to work with your Redpanda nodes, for example:

    a. Check the status of the cluster:

        kubectl -n chat-with-me run -ti --rm \
        --restart=Never \
        --image vectorized/redpanda:$VERSION \
        -- rpk --brokers one-node-cluster-0.one-node-cluster.chat-with-me.svc.cluster.local:9092 \
        cluster info
    
    b. Create a topic:

        kubectl -n chat-with-me run -ti --rm \
        --restart=Never \
        --image vectorized/redpanda:$VERSION \
        -- rpk --brokers one-node-cluster-0.one-node-cluster.chat-with-me.svc.cluster.local:9092 \
        topic create chat-rooms -p 5

    c. Show the list of topics:

        kubectl -n chat-with-me run -ti --rm \
        --restart=Never \
        --image vectorized/redpanda:$VERSION \
        -- rpk --brokers one-node-cluster-0.one-node-cluster.chat-with-me.svc.cluster.local:9092 \
        topic list

As you can see, the commands from the "rpk" pod created a 5 partition topic in for the chat rooms.

## Next steps

- Contact us in our [Slack](https://vectorized.io/slack) community so we can work together to implement your Kubernetes use cases.
- Check out how to set up a Kubernetes cluster with [access from an external machine](/docs/kubernetes-external-connect).
