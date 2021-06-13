---
title: Configuring Redpanda operator for connectivity
order: 0
---

The Redpanda operator supports configuration for internal and external connectivity.
In the folllowing sections we'll go over the corresponding configuration settings, the main actions the operator takes, the resouces created, and the expected outputs.
First let's look at configuring for internal connectivity.

## Internal connectivity

In this section we'll use a modified version of the [one node cluster](https://raw.githubusercontent.com/vectorizedio/redpanda/dev/src/go/k8s/config/samples/one_node_cluster.yaml) example with *3 nodes*.

### Custom Resource

The Cluster Custom Resource (CR) allows us to configure a listener per API with each listener containing the desired port number.
For example:

```
  configuration:
    kafkaApi:
    - port: 9092
    pandaproxyApi:
    - port: 8082
    adminApi:
    - port: 9644
```

### Created service

The above example results in the creation of a single, headless Kubernetes Service.
It is important for a Kafka client to have direct access to individual brokers, hence the choice of a headless service (clusterIP: None).
That's instead of load balancing requests across the brokers. The service has a port per listener:

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

Each listener is given three fields.
In this example, the Kafka listener has a named port “kafka” with port 9092, as specified in the CR.
It’s also given an identical target port that clients in the internal network can use to communicate with, and the listener gets 3 endpoints with the internal IP addresses of the service.
For more information on Services, look at the [Kubernetes documentation](https://kubernetes.io/docs/concepts/services-networking/service/).

### Redpanda configuration

In addition to creating the headless service, the Redpanda operator prepares a `redpanda.yaml` configuration file for Redpanda.
The configuration is read during startup and contains the advertised addresses, which in this case are set to the FQDN of each broker:

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

The configuration we went over provides internal connectivity.
A client in the internal network can use the following addresses to communicate with the brokers using the Kafka API:

```
BROKERS=`kubectl get clusters three-node-cluster -o=jsonpath='{.status.nodes.internal}'  | jq -r 'join(",")'`
```

The result should show this FQDN list:

```
$ echo $BROKERS
three-node-cluster-0.three-node-cluster.default.svc.cluster.local.,\
three-node-cluster-1.three-node-cluster.default.svc.cluster.local.,\
three-node-cluster-2.three-node-cluster.default.svc.cluster.local.
```

Using rpk we can check the cluster info:

```
rpk --brokers $BROKERS cluster info
```

In the above example we focused on the Kafka API.
Similar information is available for the Admin API and Pandaproxy API.
Regardless of the API, the internal addresses can only be used by clients within the internal network.

## External connectivity

In this section, we'll use the [external connectivity](https://raw.githubusercontent.com/vectorizedio/redpanda/dev/src/go/k8s/config/samples/external_connectivity.yaml) specification as a base.

### Custom Resource
Let's create a cluster that is externally accessible.
The CR allows us to enable external connectivity by adding a second listener for each API through the “external” field.
In particular, you can use TLS/mTLS on the external port and keep the internal port open.
Let’s enable external connectivity for all supported APIs:

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

### Created services

We now have two listeners, one internal and one external.
The operator will create the headless service as in the internal connectivity case,
and will also create two services to support external connectivity:

1. A load-balanced ClusterIP service that is used as an entrypoint for the Pandaproxy.
2. A Nodeport service used to expose each API to the node’s external network.
    Make sure that the node is externally accesible.

|   | \<cluster-name\> <br/> headless  | \<cluster-name\>-cluster <br /> load-balanced | \<cluster-name\>-external <br /> nodeports |
|---|---|---|---|
| Admin API  | y  | n  | y |
| Kafka API  | y  | n  | y |
| Pandaproxy API  | y  | y  | y|

Each external listener is provided with a nodeport that is automatically selected by Kubernetes.
A benefit of not specifying one explicitly is the prevention of port collisions.
Each nodeport has a corresponding internal port, which is set by convention to the internal-port +1. As a result, port 9092 becomes 9093 and the port name has “-external” appended to it.

```
$ kubectl describe svc external-connectivity-external
...
Port:                     kafka-external  9093/TCP
TargetPort:               9093/TCP
NodePort:                 kafka-external  31848/TCP
Endpoints:                <none>
```

Make sure that your the generated nodeports are open and reachable.

Let’s go over the two new services.

#### ClusterIP service

The ClusterIP service load-balanced load-balances requests by Kubernetes to the Redpanda nodes with the help of a selector.
As you can see from the service description below, in a 3-node Redpanda cluster we have three endpoints.
The `-cluster` service is currently used by Pandaproxy as a bootstrap point.

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

#### External service

The `-external` service is responsible for setting up nodeports for each API.
Because we enable external connectivity for all three APIs in the description below, we have three nodeports each one point to a target port with the port number of the original port + 1.

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

Assuming a client has network access to the node and the nodeports are open, a client can use the following addresses to communicate with the brokers using the configured APIs:

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

You can use the same steps to configure external access for the Admin API and the Pandaproxy API.
Remember that the internal addresses can only be used by clients within the internal network,
so to allow external connections the node must have a reachable external IP and the nodeports must be open to the client.

### Redpanda configuration

Similar to the “internal” listener case, the Redpanda operator prepares a configuration file `redpanda.yaml`.
For each API the configuration file contains an additional external listener and its corresponding advertised address.
The second advertised address points to the external IP of each node and the nodeport of that API, for example:

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

The Redpanda operator ensures that the Redpanda container exposes the requested ports, as described in the CR.
The Redpanda containers are configured to expose two ports per API - one internal and one external.
The operator does not create an external port for RPC.

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

The CRD includes a subdomain field that allows to specify the advertised address of external listeners.
Here’s an example for the Kafka API:

```
  configuration:
    kafkaApi:
    - port: 9092
    - external:
        enabled: true
        subdomain: "test.subdomain.com"
```

The generated `redpanda.yaml` configuration uses the subdomain field to generate the advertised addresses for the external listeners following this format: \<broker_id\>.\<subdomain\>:\<node_port\>.
Note that the DNS configuration is *not* handled by the Redpanda operator.

The Redpanda configuration reflects this in the advertised addresses: 

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

## 
