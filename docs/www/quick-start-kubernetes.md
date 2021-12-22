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

- [Kubernetes](https://kubernetes.io/docs/setup/) v1.19 or above
- [kubectl](https://kubernetes.io/docs/tasks/tools/) v1.19 or above
- [helm](https://github.com/helm/helm/releases) v3.0.0 or above
- [cert-manager](https://cert-manager.io/docs/installation/kubernetes/) v1.2.0 or above

    Follow the instructions to verify that cert-manager is ready to create certificates.

Make sure you also have these common tools installed:
- [Go](https://golang.org/doc/install) v1.17 or above
- [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git) 

To run locally
- [Kind](https://kind.sigs.k8s.io/docs/user/quick-start/) v0.12 or above

**_Note_** Make sure that you have kind configured in your path. [This reference in the GO documentation](https://golang.org/doc/code#GOPATH) can help you configure the path.




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
  --nodes-max 4
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

  <tab id="Digital Ocean">

  First, set up your [Digital Ocean account](https://docs.digitalocean.com/products/getting-started/) and install [`doctl`](https://docs.digitalocean.com/reference/doctl/how-to/install/).

  Remember to setup your [personal access token](https://docs.digitalocean.com/reference/api/create-personal-access-token/).

  
  For additional information, check out the [Digital Ocean setup docs](https://github.com/digitalocean/Kubernetes-Starter-Kit-Developers/blob/main/01-setup-DOKS/README.md).

  Then you can create a cluster for your Redpanda deployment:

  ```
  doctl kubernetes cluster create redpanda --wait --size s-4vcpu-8gb
  ```

  </tab>
</tabs>

## Kubectl context
Most cloud utility tools will automatically change your `kubectl` config file.   
To check if you're in the correct context, run the command:

```bash
kubectl config current-context
```

For Digital Ocean for example, the output will look similar to this:

```bash
do-nyc1-redpanda
```
   
If you're running multiple clusters or if the config file wasn't set up automatically, look for more information in the [Kubernetes documentation](https://kubernetes.io/docs/tasks/access-application-cluster/configure-access-multiple-clusters/).

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

We recommend that you use [the verification procedure](https://cert-manager.io/docs/installation/verify/#manual-verification) in the cert-manager docs
to verify that cert-manager is working correctly.

## Use Helm to install Redpanda operator

1. Using Helm, add the Redpanda chart repository and update it:

    ```
    helm repo add redpanda https://charts.vectorized.io/ && \
    helm repo update
    ```

2. Just to simplify the commands, create a variable to hold the latest version number:

    ```
    export VERSION=$(curl -s https://api.github.com/repos/vectorizedio/redpanda/releases/latest | jq -r .tag_name)
    ```

    **_Note_** - You can find information about the versions of the operator in the [list of operator releases](https://github.com/vectorizedio/redpanda/releases).   
    We're using `jq` to help us. If you don't have it installed run this command:

    <tabs>
      <tab id="apt">

    ```bash
    sudo apt-get update && \
    sudo apt-get install jq
    ```
      </tab>
      <tab id="brew">

    ```bash
    brew install jq
    ```
      </tab>
    </tabs>

3. Install the Redpanda operator CRD:

<tabs group="shell">

  <tab id="bash">

```
kubectl apply \
-k https://github.com/vectorizedio/redpanda/src/go/k8s/config/crd?ref=$VERSION
```
  </tab>

  <tab id="zsh">

```
noglob kubectl apply \
-k https://github.com/vectorizedio/redpanda/src/go/k8s/config/crd?ref=$VERSION
```

  </tab>

</tabs>

4. Install the Redpanda operator on your Kubernetes cluster with:

    ```
    helm install \
    redpanda-operator \
    redpanda/redpanda-operator \
    --namespace redpanda-system \
    --create-namespace \
    --version $VERSION
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
        --image docker.vectorized.io/vectorized/redpanda:$VERSION \
        -- rpk --brokers one-node-cluster-0.one-node-cluster.chat-with-me.svc.cluster.local:9092 \
        cluster info
    
    b. Create a topic:

        kubectl -n chat-with-me run -ti --rm \
        --restart=Never \
        --image docker.vectorized.io/vectorized/redpanda:$VERSION \
        -- rpk --brokers one-node-cluster-0.one-node-cluster.chat-with-me.svc.cluster.local:9092 \
        topic create chat-rooms -p 5

    c. Show the list of topics:

        kubectl -n chat-with-me run -ti --rm \
        --restart=Never \
        --image docker.vectorized.io/vectorized/redpanda:$VERSION \
        -- rpk --brokers one-node-cluster-0.one-node-cluster.chat-with-me.svc.cluster.local:9092 \
        topic list

As you can see, the commands from the "rpk" pod created a 5-partition topic in for the chat rooms.

## Next steps

- Check out our in-depth explanation of how to [connect external clients](/docs/kubernetes-connectivity) to a Redpanda Kubernetes deployment.
- Contact us in our [Slack](https://vectorized.io/slack) community so we can work together to implement your Kubernetes use cases.
