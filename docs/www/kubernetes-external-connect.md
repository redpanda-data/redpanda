---
title: Kubernetes Quick Start Guide with external connectivity
order: 0
---

# Kubernetes Quick Start Guide with external connectivity

The [Kubernetes Quick Start guide](/docs/quick-start-kubernetes) describes how to quickly get up and running with a Kubernetes cluster.
Those instructions only provide access to the cluster from within the Kuberenetes network.

Here we'll show you an example of how to set up Kubernetes in
[Google GKE](https://cloud.google.com/kubernetes-engine), [Amazon EKS](https://aws.amazon.com/eks), or [Digital Ocean](https://cloud.digitalocean.com/)
so you can work with Redpanda from outside of the Kubernetes network.

Let's get started...

## Create a Kubernetes cluster

Create a 3-node cluster on the platform of your choice:

<tabs>

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
  --nodes-max 4 
  ```

  It will take about 10-15 minutes for the process to finish.

  </tab>

  <tab id="Google GKE">

  First complete the "Before You Begin" steps described in [Google Kubernetes Engine Quickstart](https://cloud.google.com/kubernetes-engine/docs/quickstart).
  Then, create a cluster with:

  ```
  gcloud container clusters create redpanda --machine-type e2-standard-4 --cluster-version 1.19 && \
  gcloud container clusters get-credentials redpanda
  ```

  **_Note_** - You may need to add a `--region`, `--zone`, or `--project` to this command.

  </tab>
  <tab id="Digital Ocean">

  First, set up your [Digital Ocean account](https://docs.digitalocean.com/products/getting-started/) and install [`doctl`](https://docs.digitalocean.com/reference/doctl/how-to/install/).

  Then you can create a cluster for your Redpanda deployment:

  ```
  doctl kubernetes cluster create redpanda --wait --size s-2vcpu-4gb
  ```

  </tab>
</tabs>

## Prepare TLS certificate infrastructure

The Redpanda cluster uses cert-manager to create TLS certificates for communication between the cluster nodes.

We'll use Helm to install cert-manager:

  ```bash
  helm repo add jetstack https://charts.jetstack.io && \
  helm repo update && \
  helm install \
    cert-manager jetstack/cert-manager \
    --namespace cert-manager \
    --create-namespace \
    --version v1.2.0 \
    --set installCRDs=true
  ```

## Install the Redpanda operator and cluster

1. Just to simplify the commands, create a variable to hold the latest version number:

    ```
    export VERSION=$(curl -s https://api.github.com/repos/vectorizedio/redpanda/releases/latest | jq -r .tag_name)
    ```

    **_Note_** - You can find information about the versions of the operator in the [list of operator releases](https://github.com/vectorizedio/redpanda/releases).

2. Install the latest redpanda operator:

<tabs group="shell">

  <tab id="bash">

  ```
  kubectl apply -k https://github.com/vectorizedio/redpanda/src/go/k8s/config/crd?ref=$VERSION && \
  helm repo add redpanda https://charts.vectorized.io/ && \
  helm repo update && \
  helm install \
    --namespace redpanda-system \
    --create-namespace redpanda-operator \
    --version $VERSION \
    redpanda/redpanda-operator
  ```

  </tab>

  <tab id="zsh">


  ```
  noglob kubectl apply -k https://github.com/vectorizedio/redpanda/src/go/k8s/config/crd?ref=$VERSION && \
  helm repo add redpanda https://charts.vectorized.io/ && \
  helm repo update && \
  helm install \
    --namespace redpanda-system \
    --create-namespace redpanda-operator \
    --version $VERSION \
    redpanda/redpanda-operator
  ```

  </tab>

</tabs>

2. Install a cluster with external connectivity:

  ```
  kubectl apply -f https://raw.githubusercontent.com/vectorizedio/redpanda/$VERSION/src/go/k8s/config/samples/external_connectivity.yaml
  ```

3. For GKE only, open the firewall for access to the cluster:
  
  a. Get the port number that Redpanda is listening on:

    ```
    kubectl get service external-connectivity-external -o=jsonpath='{.spec.ports[0].nodePort}'
    ```

    The port is shown in the command output.

  b. Create a firewall rule that allows traffic to Redpanda on that port:

    ```
    gcloud compute firewall-rules create redpanda-nodeport --allow tcp:<port_number>
    ```

    The port that Redpanda is listening on is shown in the command output, for example:

    `30249`

4. Get the addresses of the brokers:

  ```
  kubectl get clusters external-connectivity -o=jsonpath='{.status.nodes.external}'
  ```

  The broker addresses are shown in the command output, for example:

  `["34.121.167.159:30249","34.71.125.54:30249","35.184.221.5:30249"]`

## Verify the connection

1. From a remote machine that has `rpk` installed, get information about the cluster:

  ```
  rpk --brokers 34.121.167.159:30249,34.71.125.54:30249,35.184.221.5:30249 \
  cluster info
  ```

2. Create a topic in your Redpanda cluster:

  ```
  rpk --brokers 34.121.167.159:30249,34.71.125.54:30249,35.184.221.5:30249 \
  topic create chat-rooms -p 5
  ```

Now you know how to set up a Kubernetes cluster in a cloud and access it from a remote machine.

## Next steps

- Check out our in-depth explanation of [Kubernetes connectivity](/docs/kubernetes-connectivity).
- Contact us in our [Slack](https://vectorized.io/slack) community so we can work together to implement your Kubernetes use cases.
