- Feature Name: External connectivity in Kubernetes environment
- Status: in-progress
- Start Date: 2021-02-23
- Authors: RafalKorepta
- Issue: [PR #694](https://github.com/vectorizedio/redpanda/pull/694)

# Executive Summary

To give users out of the box experience Redpanda
Kubernetes operator should pave the way for the networking stack to
expose each Redpanda node in k8s environment.

## What is being proposed

The most performant solution is to expose each process to the outside,
by specifying
[host port in the container ports specification](https://Kubernetes.io/docs/reference/generated/Kubernetes-api/v1.20/#containerport-v1-core)
for each Redpanda node.

## Why (short reason)

The Redpanda is focused on performance and its low tail latency. To
not add more network hops this proposal tries to merit dynamic
Kubernetes environment with the direct access to Redpanda process.

## How (short plan)

The [PR #694](https://github.com/vectorizedio/redpanda/pull/694)
introduces Service type NodePort and external advertised kafka API.
The Service typed NodePort is focused on exposing unique ports in
each Kubernetes node. Operator's responsibility is to use that
assigned port in initial script that mangles Redpanda configuration.

## Impact

The Kubernetes worker nodes must open port per Redpanda cluster
which might be an additional attack vector.

# Motivation
## Why are we doing this?

Redpanda can be easily managed in multiple environments, but vast
range of users plan to manage their workload in Kubernetes
environment. Saying that, teams would like to not introduce
clients application into their Kubernetes clusters. The Kafka API
requires that each broker will be reachable at any given time.
Canonical way of exposing webservers is to create Ingress resources
that can be reconciled into cloud provider load balancer. Another
way of exposing different kind of workloads is to expose their TCP
port using again cloud provider load balancer. Ont the other hand
the TCP port can be exposed on each Kubernetes worker node using
Service type Node Port. All the mentioned methods doesn't guarantee
that each Redpanda node will be individually accessible. For that
reasons the Kubernetes model of exposing workloads outside of the
k8s cluster doesn't provide predictable way of reaching individual
broker in Redpanda cluster.

The operator should accommodate dynamic nature of any cloud
where nodes can disappear or be rescheduled. The reporting of the
brokers addresses should be as quick as possible.

## What use cases does it support?

Exposing each Redpanda node to networks outside the Kubernetes
cluster.

## What is the expected outcome?

The Redpanda operator will make sure that from the network standpoint
brokers will be reachable. Users need to managed security of the
exposed Kubernetes worker nodes e.g. `aws security groups`.

# Guide-level explanation
## How do we teach this?

The Kafka API requires that each broker will be reachable from
the client side. To expose Redpanda nodes, but not add additional
latency to the communication each Redpanda process will have assigned
unique host port. In case of cloud providers the host VM need to be
publicly reachable and have opened port. The operator would be
responsible to orchestrate Kubernetes to achieve that.

## Dictionary

- Kubernetes Service type NodePort with empty selector that gives
  unused port that is available across all k8s worker nodes

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

- List all corner cases:
  And a detailed explanation of the corner cases

## Detailed design - What needs to change to get there

The host port need to be added in kafka port in the Statefulset
definition. Service type node port needs to be added.

## Detailed design - How it works

Each Kubernetes node needs to be exposed to the internet in order
to expose Redpanda node. Operator's responsibility is creation of
Redpanda configuration that has correctly registered advertised
Kafka API and assign correct unique and unused node port. With the
help of Kubernetes Service type node port the unused port can be
injected into container host port. This service doesn't have selector,
so that no rule should be created in `iptable` or any other CNI
implementation.

## Drawbacks

The configuration of each node is done in init container. This
container will have mounted script that need to provide node public
IP to the advertised kafka API configuration. The coupling between
identity of the Redpanda node and node on which it is running creates
problem of whom should have access rights to configure `redpanda`.

Current implementation mounts file that has list of private and
public IPs of all available nodes. The init script mangle the list
of IPs using `grep` and `awk`. The RBAC rules are assigned to the
operator, but the init container and ConfigMap has access to the
mentioned list of IPs. This can be considered as private information.

With the proposed implementation there is problem when the Kubernetes
cluster have significant amount of nodes. The ConfigMap resource
will spend a lot of time in constructing the list of IPs. The RBAC
rules need to be added for ServiceAccount that is assigned to the POD.

The retrieval of such information could be done in `rpk` to tight
the security and give better performance.

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
- How should discover advertised kafka api?
  - init container script that reach the Kubernetes API server
  - operator by reconfiguring `redpanda` in runtime
  - rpk by having Kubernetes golang client

- What parts of the design do you expect to resolve through the RFC
  process before this gets merged?
- What parts of the design do you expect to resolve through the
  implementation of this feature before stabilization?
- What related issues do you consider out of scope for this RFC that
  could be addressed in the future independently of the solution that
  comes out of this RFC?
