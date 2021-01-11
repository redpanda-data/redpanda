---
title: Kubernetes Quick Start Guide
order: 0
---
# Kubernetes Quick Start Guide

Redpanda is a modern streaming platform for mission critical workloads.
Redpanda is also fully API compatible Kafka allowing you to make full
use of the Kafka ecosystem.

This quick start guide to intended to help you get started with Redpanda for
development and testing purposes. For production deployments or performance
testing please see our [Production Deployment](production-deployment.md) for more information.

Using [Helm](https://helm.sh/) is the fastest way to get started with Redpanda
on Kubernetes. First, we need to create a cluster. There are a number of
different ways to create a cluster, either local or with a hosting provider.

- [Kind](#Kind)
- [AWS EKS](#AWS-EKS)
- [Google GKE](#Google-GKE)

## Using Helm to Install Redpanda

[Helm](https://helm.sh/) provides a quick and easy way to deploy Redpanda on
Kubernetes. To get started please use the
[Helm Quickstart Guide](https://helm.sh/docs/intro/quickstart/)
to install Helm. Once installed you will want to add the Redpanda chart repo.

```
helm repo add redpanda https://charts.vectorized.io/
```

After the repo has been added, run the Helm repo update command to retrieve the
latest version of the Helm chart.

```
helm repo update
```

Now you can install Redpanda on your Kubernetes cluster using Helm.

```
helm install --namespace redpanda --create-namespace redpanda redpanda/redpanda
```

> **_Note:_** In order to communicate with the Redpanda cluster you will have to
> run a container inside the Kubernetes cluster. Currently a Load Balancer is
> not automatically created during deployment by default.

## Kind

[Kind](https://kind.sigs.k8s.io) is an easy to use tool for creating local Kubernetes clusters using Docker. Once you have Kind installed, setting up a cluster is a simple as:

```
kind create cluster
```

Once the cluster is created please follow the [Helm install instructions](#Using-Helm-to-Install-Redpanda).

## AWS EKS

First complete all the steps describe in [EKS Getting Started](https://docs.aws.amazon.com/eks/latest/userguide/getting-started-eksctl.html)
guide. This includes installing `eksctl` which is used to create and delete
clusters in EKS.

Once `eksctl` is installed, you can then use it to create an EKS cluster:

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

This command will take 10-15 minutes to complete. Once it is completed then
follow the [Helm install instructions](#Using-Helm-to-Install-Redpanda).

## Google GKE

First complete the "Before You Begin" steps describe in
[Google Kubernetes Engine Quickstart](https://cloud.google.com/kubernetes-engine/docs/quickstart).
Once complete you can create a cluster using the following command:

```
gcloud container clusters create redpanda --machine-type n1-standard-4
```

Once the command completes then follow the
[Helm install instructions](#Using-Helm-to-Install-Redpanda)