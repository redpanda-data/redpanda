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
    --create-namespace redpanda-system \
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

## Internal connectivity

The Redpanda operator supports configuration for internal and external connectivity. In the folllowing sections we'll go over the corresponding configuration settings, the main actions the operator takes, the resouces created, and the expected outputs. We first look into internal connectivity.

In this section we are using a modified version of the [one node cluster](https://raw.githubusercontent.com/vectorizedio/redpanda/dev/src/go/k8s/config/samples/one_node_cluster.yaml) example with *3 nodes*.

### Cluster Custom Resource

The Cluster CR allows us to configure a listener per API with each listener containing the desired port number. For example,

```
  configuration:
    kafkaApi:
    - port: 9092
    pandaproxyApi:
    - port: 8082
    adminApi:
    - port: 9644
```

### Created Service

The above example results in the creation of a single, headless Kubernetes Service. It is important for a Kafka client to have direct access to individual brokers, hence the choice of a headless service (clusterIP: None). That's instead of load balancing requests across the brokers. The service has a port per listener:

```
$ kubectl describe svc three-node-cluster 
Name:              three-node-cluster
..
Type:              ClusterIP
IP Families:       <none>
IP:                None
IPs:               <none>
Port:              admin  9644/TCP
TargetPort:        9644/TCP
Endpoints:         10.0.1.7:9644, ..
Port:              kafka  9092/TCP
TargetPort:        9092/TCP
Endpoints:         10.0.1.7:9092, ..
Port:              proxy  8082/TCP
TargetPort:        8082/TCP
Endpoints:         10.0.1.7:8082, ..
```

Each listener is given three fields. For example, the Kafka listener has a named port “kafka” with port 9092, as specified in the CR. It’s also given an identical target port that clients in the internal network can use to communicate with, and finally 3 endpoints with the internal IP addresses of the service. For more information on Services, please check the [Kubernetes documentation](https://kubernetes.io/docs/concepts/services-networking/service/).

### Redpanda configuration

In addition to creating the headless service, the Redpanda operator prepares a configuration file `redpanda.yaml` for Redpanda. The configuration read during startup and contains the advertised addresses, which in this case are set to the FQDN of each broker:

```
$ kubectl logs three-node-cluster-0
..
advertised_kafka_api:
    - address: three-node-cluster-0.three-node-cluster.default.svc.cluster.local.
      name: kafka
      port: 9092
  ...
  kafka_api:
    - address: 0.0.0.0
      name: kafka
      port: 9092
```

### Cluster status

Similarly, the CR status contains the advertised internal addresses:

```
$ kubectl describe cluster three-node-cluster
..
Status:
  Nodes:
    Internal:
      three-node-cluster-0.three-node-cluster.default.svc.cluster.local.
      three-node-cluster-1.three-node-cluster.default.svc.cluster.local.
      three-node-cluster-2.three-node-cluster.default.svc.cluster.local.
```

### Trying it out

The configuration we went over provides internal connectivity. A client in the same (internal) network can use the following addresses to communicate with the brokers using the Kafka API:

```
BROKERS=`kubectl get clusters three-node-cluster -o=jsonpath='{.status.nodes.internal}'  | jq -r 'join(",")'`
```

The result should hold the FQDN list:
```
$ echo $BROKERS
three-node-cluster-0.three-node-cluster.default.svc.cluster.local.,\
three-node-cluster-1.three-node-cluster.default.svc.cluster.local.,\
three-node-cluster-2.three-node-cluster.default.svc.cluster.local.
```

Using rpk we can check the cluster info
```
rpk --brokers $BROKERS cluster info
```

In the above example we focused on the Kafka API. Similar information is available for the Admin API and Pandaproxy API. To conclude, regardless of the API, the internal addresses can only be used by clients within the internal network.

## External connectivity

In this section we use the [external connectivity](https://raw.githubusercontent.com/vectorizedio/redpanda/dev/src/go/k8s/config/samples/external_connectivity.yaml) specification as a basis.

### Custom Resource
Let's create a cluster that is externally accessible. The CR allows us to enable external connectivity by adding a second listener for each API through the “external” field. A practical reason for having two listeners is to enable TLS/mTLS on the external while keeping the internal one open. Let’s enable external connectivity for all supported APIs:

```
  configuration:
    kafkaApi:
     - port: 9092
     - external:
         enabled: true
    pandaproxyApi:
     - port: 8082
     - external:
         enabled: true
    adminApi:
    - port: 9644
     - external:
         enabled: true
```

### Created Services

We now have two listeners, one internal and one external. The operator will create the headless service as earlier and additionally, two services to support external connectivity: 1) a load-balanced ClusterIP service that is used as an entrypoint for the Pandaproxy; 2) a Nodeport service used to expose each API to the node’s external network - note that if the node is not reachable externally the Redpanda listeners will not be reachable.

|   | \<cluster-name\> <br> headless  | \<cluster-name\>-cluster <br> load-balanced | \<cluster-name\>-external nodeports |
|---|---|---|---|
| Admin API  | y  | n  | y |
| Kafka API  | y  | n  | y |
| Pandaproxy API  | y  | y  | y|

Each external listener is provided with a nodeport that is automatically selected by Kubernetes. A benefit of not specifying one explicitly is the prevention of port collisions. Each nodeport has a corresponding internal port, which is set by convention to `internal-port +1`, e.g., 9092 becomes 9093, whereas the port name has “-external” appended to it:

```
$ kubectl describe svc external-connectivity-external
...
Port:                     kafka-external  9093/TCP
TargetPort:               9093/TCP
NodePort:                 kafka-external  31848/TCP
Endpoints:                <none>
```

Please ensure that your the generated nodeports are open and reachable.

Let’s go over the two new services.

The `-cluster` Service is currently used by Pandaproxy as a bootstrap point. It is a ClusterIP service, which means a request is load-balanced by Kubernetes to the Redpanda nodes with the help of a selector. As you can see from the service description below, in a 3-node Redpanda cluster we have three endpoints.

```
$ kubectl describe svc external-connectivity-cluster

Name:              external-connectivity-cluster
..
Selector:          ..., app.kubernetes.io/name=redpanda
Type:              ClusterIP
IP Families:       <none>
IP:                10.3.246.143
IPs:               <none>
Port:              proxy-external  8083/TCP
TargetPort:        8083/TCP
Endpoints:         10.0.0.8:8083,10.0.1.8:8083,10.0.2.4:8083
```

The `-external` service is responsible for setting up nodeports for each API. Since we have enabled external connectivity for all three APIs, in the description below we have three nodeports, each pointing to a target port (set to the original port + 1).

```
$ kubectl describe svc external-connectivity-external
Name:                     external-connectivity-external
..
Selector:                 <none>
Type:                     NodePort
IP Families:              <none>
IP:                       10.3.247.127
IPs:                      <none>

Port:                     kafka-external  9093/TCP
TargetPort:               9093/TCP
NodePort:                 kafka-external  31848/TCP
Endpoints:                <none>

Port:                     admin-external  9645/TCP
TargetPort:               9645/TCP
NodePort:                 admin-external  31490/TCP
Endpoints:                <none>

Port:                     proxy-external  8083/TCP
TargetPort:               8083/TCP
NodePort:                 proxy-external  30638/TCP
Endpoints:                <none>
```

Assuming a client has network access to the node and the nodeports are open, a client can use the following addresses to communicate with the brokers using the configured APIs. For accessing the Kafka API:

```
BROKERS=`kubectl get clusters external-connectivity -o=jsonpath='{.status.nodes.external}'  | jq -r 'join(",")'`
```

The result contains a list of external IPs with the broker port:
```
$ echo $BROKERS
<node-0-external-ip>:31848,<node-1-external-ip>:31848,<node-2-external-ip>:31848
```

Given the list of addresses, we can use rpk (or a Kafka client) to test the connection:
```
rpk --brokers $BROKERS cluster info
```

The above walk-through focuses on the Kafka API, however, similar steps can be followed for the Admin API and the Pandaproxy API. Remember that the internal addresses can only be used by clients within the internal network, whereas the node must have a reachable external IP and the nodeports must be open to the client.

### Redpanda configuration

Similarly to the “internal” listener case, the Redpanda operator prepares a configuration file `redpanda.yaml`. In this case, for each API it contains an additional, external listener and its corresponding advertised address. The 2nd advertised address points to the external IP of each node and the nodeport of that API, for example,

```
$ kubectl exec external-connectivity -- cat /etc/redpanda/redpanda.yaml 
  advertised_kafka_api:
    - address: external-connectivity-0.external-connectivity.default.svc.cluster.local.
      name: kafka
      port: 9092
    - address: <external-node-ip>
      name: kafka-external
      port: 31848
```

### Container configuration

The Redpanda operator ensures that the Redpanda container exposes the requested ports, as described in the CR. The Redpanda containers are configured to expose two ports per API - one internal and one external. The operator does not create an external port for RPC.

By running `kubectl describe pod external-connectivity-0` we can see the ports and the mapping to the node ports created through the nodeport service:

```
$ kubectl describe pod external-connectivity-0 
...
Containers:
  redpanda:
    ..
    Ports:         33145/TCP, 9644/TCP, 9092/TCP, 8082/TCP, 9093/TCP, 9645/TCP, 8083/TCP
    Host Ports:    0/TCP, 0/TCP, 0/TCP, 0/TCP, 31848/TCP, 31490/TCP, 30638/TCP
```

### Port Summary - Configuration based on example specification

|   | Internal listener ports  | External listener ports (port+1:nodeport)  |
|---|---|---|
| Admin API  | 9644  | 9645:31490  |
| Kafka API  | 9092  | 9093:31848  |
| Pandaproxy API  | 8082  | 8083:30638  |


### Using names instead of external IPs

The CRD includes a field “subdomain” that allows to specify the advertised address of external listeners. Here’s an example for the Kafka API:
```
  configuration:
    kafkaApi:
    - port: 9092
    - external:
        enabled: true
        subdomain: "test.subdomain.com"
```

The generated `redpanda.yaml` configuration uses the subdomain field to generate the advertised addresses for the external listeners following this format: <broker_id>.<subdomain>:<node_port>. Note that the DNS configuration is *not* handled by the Redpanda operator.

The Redpanda configuration will reflect this in the advertised addresses: 

```
$ kubectl exec external-connectivity -- cat /etc/redpanda/redpanda.yaml 
...
redpanda:
  advertised_kafka_api:
    - address: external-connectivity-0.external-connectivity.default.svc.cluster.local.
      name: kafka
      port: 9092
    - address: 0.test.subdomain.com
      name: kafka-external
      port: 31631
```

Finally, the CR contains the addresses in its status:

```
$ kubectl describe cluster external-connectivity
...
Status:
  Nodes:
    External:
      0.test.subdomain.com:31631
      1.test.subdomain.com:31631
      2.test.subdomain.com:31631
```

## Next steps

- Contact us in our [Slack](https://vectorized.io/slack) community so we can work together to implement your Kubernetes use cases.
- Check out how to set up a Kubernetes cluster with [access from an external machine](/docs/kubernetes-external-connect).
