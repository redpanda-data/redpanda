- Feature Name: External connectivity in Kubernetes environment
- Status: in-progress
- Start Date: 2021-02-23
- Authors: RafalKorepta
- Issue: none

# Executive Summary

To give users out of the box experience Redpanda
Kubernetes operator should pave the way for the networking stack to
expose each Redpanda node in k8s environment.

## What is being proposed

The most performant and cost-efficient solution is to expose each
process to the outside, by specifying
[host port in the container ports specification](https://Kubernetes.io/docs/reference/generated/Kubernetes-api/v1.20/#containerport-v1-core)
for each Redpanda node.

## Why (short reason)

The Redpanda is focused on performance and its low tail latency. To
not add more network hops this proposal tries to merit dynamic
Kubernetes environment with the direct access to Redpanda process.

## How (short plan)

Service of type NodePort will have assigned port that is free
on each node and will be used in the advertised kafka API
configuration. The node port will be set in the host port
configuration of Redpanda container along with internal, RPC and
Admin ports. The external Kafka API port will be not configurable
and arbitrarily chosen to internal Kafka API + 1. Init container will
discover public IP of the host on which it was scheduled. The port
will be set in the advertised Kafka API address.

## Impact

The Kubernetes worker nodes must open port per Redpanda cluster
which might be an additional attack vector. The additional
firewalls should be managed to allow node access.

# Motivation
## Why are we doing this?

Redpanda can be easily managed in multiple environments, but vast
range of users plan to manage their workload in Kubernetes
environment. Saying that, teams would like to not introduce
clients application into their Kubernetes clusters. The Kafka API
requires that each broker will be reachable at any given time.
Canonical way of exposing webservers in Kubernetes environment is
to create Ingress resources that can be reconciled into cloud
provider load balancer. Another way of exposing different kind of
workloads is to expose their TCP port using Service type LoadBalancer
and again cloud provider load balancer will be provisioned. On
the other hand the TCP port can be exposed on each Kubernetes worker
node using Service type Node Port. All the mentioned methods
doesn't guarantee that each Redpanda node will be accessible
individually. One could create Service type Node Port for each
broker, but that would be a waste of ports in the whole cluster.
The optimal solution would be to pick one port and take public
IP of a node on which Redpanda is scheduled and use it in
advertised Kafka API.

During testing in AWS environment the service type Node Port doesn't
have assigned external IP. That's why init container needs to
retrieve node information from the Kubernetes API and use it in
advertised Kafka API address.

The operator should accommodate dynamic nature of any cloud
where nodes can disappear or be rescheduled.

## What use cases does it support?

Exposing each Redpanda node to networks outside the Kubernetes
cluster.

## What is the expected outcome?

The Redpanda operator will make sure that from the network standpoint
brokers will be reachable. Users need to manage security of the
exposed Kubernetes worker nodes e.g. `aws security groups`.

# Guide-level explanation
## How do we teach this?

The Kafka API requires that each broker will be reachable from
the client side. To expose Redpanda nodes, but not add additional
latency to the communication each Redpanda process will have assigned
unique host port. In case of cloud providers the host VM need to be
publicly reachable and have opened some range ports. The operator
would be responsible to orchestrate Kubernetes to achieve that.

## Dictionary

- Kubernetes Service type NodePort with empty selector that gives
  unused port that is available across all k8s worker nodes
- ServiceAccount with RBAC rule assigned to init container will
  allow retrieving the node information from Kubernetes API server

# Reference-level explanation

( This is the technical portion of the RFC )

## Interaction with other features

- The kafka multi listener needs to be set up in each Redpanda
  node, so that internal connectivity can stay as is. For external
  connectivity the dedicated port would be opened.
- The statefulset must bootstrap Redpanda cluster with or without
  external connectivity.
- The user intention defined in cluster.redpanda.vectorized.io custom
  resource should be reconciled into set of ready to use address with
  ports that can be reach from the internet. The status field will
  be the place to look for the addresses.

## Telemetry & Observability

- metrics that can be anchor in Redpanda or in VM network stack
    - Number of active connection
    - Number of dropped connection
    - Number of bytes processed
- Tail latency observed by the clients
- Additional probe that will live close to operator, but will try
  reach the Redpanda Nodes by their external address

## Corner cases dissected by example.

None.

## Detailed design - What needs to change to get there

The host port need to be added in kafka port in the Statefulset
definition. Service type node port needs to be added. The init
container needs to know how to get the public IP of the node.
New ServiceAccount need to be added with the RBAC rule for getting
node information.

## Detailed design - How it works

Each Kubernetes node needs to be exposed to the internet in order
to expose Redpanda node. Operator's responsibility is creation of
Redpanda configuration that has correctly registered advertised
Kafka API and assign correct unique and unused node port. With the
help of Kubernetes Service type node port the unused port can be
injected into container host port. This service doesn't have selector,
so that no rule should be created in `iptable` or any other CNI
implementation. Init container will retrieve from the Kubernetes API
server information about node and its public IP. That container will
then change the Redpanda configuration, so that internal and external
ports and addresses will be in Kafka API and Advertised Kafka API
section.

## Drawbacks

The nodes must be exposed to some other networks. This scope goes
beyond the Kubernetes. There is no easy way for the operator to
manage firewalls that can be blocking the network traffic.

## Rationale and Alternatives

The more natural way of integrating with k8s resource is to use service
type `LoadBalancer` per pod. This would add latency and cost will scale
as the bytes comes through.

Each instance could be registered in DNS provider of one's choice.
The benefits with this approach is nice looking brokers names. On the
other hand the latency of propagating change in DNS systems could
not be acceptable.

Following `external-dns`
[tutorial for headless services](https://github.com/Kubernetes-sigs/external-dns/blob/eaf933328f4f7ed1db1546145fa81ed5a61cae3b/docs/tutorials/hostport.md)
there should be an easy way of exposing each pod. During the tests
in AWS EKS the DNS A records shows internal IP address of a Pod in
the AWS route 53 service.

Exposing all brokers via LoadBalancer and Envoy proxy. The Envoy can
reroute connection to proper broker. The Envoy can be replaced with
any other proxy Nginx etc. There will be bottleneck in hot path as
every connection will go through LB + proxy. It doesn't scale well
as one client can starve the network hardware.

## Unresolved questions

- What do to do with the migration of the brokers to different machine?
- How this feature integrates with TLS?
  - How the subject alternative names should look like?
  - Does the advertised Kafka API need to be change from
    HOST_PUBLIC_IP:PORT to DNS_NAME:PORT?
