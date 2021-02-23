- Feature Name: Rafał Korepta
- Status: in-progress
- Start Date: 2021-02-23
- Authors: RafalKorepta
- Issue: (one or more # from the issue tracker)

# Executive Summary

Users would like to managed Redpanda clusters in kubernetes environment,
but the clients could live outside of the kubernetes network boundary.
To give users out of the box experience Redpanda kubernetes operator
should pave the way for the networking stack to expose each Redpanda
node in k8s environment.

## What is being proposed

The most performant solution is to expose each process to the outside,
by specifying
[host port in the container ports specification](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.20/#containerport-v1-core)
for each Redpanda node. Each instance will
need to be registered in DNS provider of one's choice.

The `external-dns` project is chosen as a integration point to ease
the integration with the DNS provider.

## Why (short reason)

The Redpanda is focused on performance and its low tail latency. To
not add more network hops this proposal tries to merit dynamic
kubernetes environment with the direct access to Redpanda process.

## How (short plan)

The [PR #658](https://github.com/vectorizedio/redpanda/pull/658)
introduces Service type NodePort and DNSEndpoint resource. The
Service resources focus on exposing unique ports in each kubernetes
node. Operator responsibility is to create statefulset that managed
Redpanda instances in a form of kubernetes Pod. After statefulset
creates pods, the operator will map assigned node public IP to
the unique broker DNS. To achieve that the DNSEndpoint custom
resource is used which is part of `external-dns` project. It can
create DNS A records into multiple different DNS providers e.g. `AWS Route53`.

## Impact

Users of the operator should manage `external-dns` by their own.
The integration point is the DNSEndpoint custom resource which
`external-dns` needs to start watching it. The kubernetes worker
nodes need open port per Redpanda cluster which might be an
additional attack vector.

# Motivation
## Why are we doing this?

Redpanda can be easily managed in multiple environment, but vast
range of users plan to manage their workload in kubernetes
environment. Saying that, teams would like to not introduce
clients application into their kubernetes clusters. The Kafka API
requires that each broker will be reachable at any given time.
Canonical way of exposing webservers is to create Ingress resources
that can be reconciled into cloud provider load balancer. Another
way of exposing different kind of workloads is to expose their TCP
port using again cloud provider load balancer. Ont the other hand
the TCP port can be exposed on each kubernetes worker node using
Service type Node Port. All the mentioned methods doesn't guarantee
that each Redpanda node will be individually accessible. For that
reasons the kubernetes model of exposing workloads outside of the
k8s cluster doesn't provide predictable way of reaching individual
broker in Redpanda cluster.

There must be easily identifiable Redpanda node address (DNS).
Those addresses should accommodate dynamic nature of any cloud
where nodes can disappear or be rescheduled.

## What use cases does it support?

Exposing each Redpanda node to the internet.

## What is the expected outcome?

Users of the Redpanda operator can easily integrate with the cloud
providers DNS service. The nodes should be reachable from the
internet after the reconciliation ends. With the caveat that DNS
records can propagate up to 24-48 hours.

# Guide-level explanation
## How do we teach this?

The Kafka API requires that each broker will be reachable from
the client side. To expose Redpanda nodes, but not add additional
latency to the communication each Redpanda process will have assigned
unique host port. In case of cloud providers the host VM would open
the port and start listening to it. DNS will be used to provide the
way of changing underling host. The operator would be responsible
to orchestrate kubernetes to achieve that.

## Dictionary

- DNSEndpoints is the custom resource created by Redpanda operator
  and reconciled by `external-dns`
- Kubernetes Service type NodePort with empty selector that gives
  unused port that is available across all k8s worker nodes
- ExternalDNSSubdomain can be set in cluster.redpanda.vectorized.io
  custom resource for hosted zone name. It needs to be in sync with
  `--domain-filter` parameter in `external-dns`.

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
    - Number of drpped connection
    - Number of bytes processed
- Tail latency observed by the clients
- Rate of changes in cloud provider DNS service
- Number of DNS registry errors in `external-dns`
- Additional probe that will live close to operator, but will try
  reach the Redpanda Nodes by their external DNS address

## Corner cases dissected by example.

- List all corner cases:
  And a detailed explanation of the corner cases

## Detailed design - What needs to change to get there

Nothing significant needs to change.

## Detailed design - How it works

Each kubernetes node needs to be exposed to the internet in order
to expose Redpanda node. Operator responsibility is to create
unique DNS name per Redpanda node with the public IP of the k8s node.
Another responsibility is to find unique port that can be assigned
to each kubernetes node. With the help of kubernetes Service type
node port the unused port can be injected into container host port.
This service doesn't have selector, so that no rule should be created
in `iptable` or any other CNI implementation. Operator then needs to
wait for pods to be assigned to node, so that it can wire the DNS
name with the node public IP. To help user with syncing mentioned DNS
records with the cloud provider DNS service the `external-dns`
project was used as it has multiple DNS provider to which one can
integrate too. Users probably already use this project in their
environment. There is only one additional step for end users in order
to enable DNSEndpoints reconciliation. The `external-dns` has program
flag named `--crd-source-apiversion` and `--crd-source-kind` which
are necessary for kubernetes api machinery to register DNSEndpoint
object.

## Drawbacks

The design introduces dependency on `external-dns`.

The DNSEndpoints could be ported to the code base. It would allow
kubebuilder to build the custom resource definition that is required
by Redpanda operator.

There could be situation where pod is moved to another kubernetes
node, so that DNS records need to be updated. The availability of
the broker will drop significantly. The DNS change isn't instant
as it could propagate for almost 1 days.

Why should we *not* do this?

If applicable, list mitigating factors that may make each drawback acceptable.

Investigate the consequences of the proposed change onto other areas of.
If other features are impacted, especially UX, list this impact as a reason
not to do the change. If possible, also investigate and suggest mitigating
actions that would reduce the impact. You can for example consider additional
validation testing, additional documentation or doc changes,
new user research, etc.

Also investigate the consequences of the proposed change on performance. Pay
especially attention to the risk that introducing a possible performance
improvement in one area can slow down another area in an unexpected way.
Examine all the current "consumers" of the code path you are proposing to
change and consider whether the performance of any of them may be negatively
impacted by the proposed change. List all these consequences as possible
drawbacks.

## Rationale and Alternatives

The more natural way of integrating with k8s resource is to use service type
`LoadBalancer` per pod. This would add latency and cost will scale as
the bytes comes through.

Following `external-dns`
[tutorial for headless services](https://github.com/kubernetes-sigs/external-dns/blob/eaf933328f4f7ed1db1546145fa81ed5a61cae3b/docs/tutorials/hostport.md)
there should be an easy way of exposing each pod. During the tests
in AWS EKS the DNS A records shows internal IP address of a Pod in
the AWS route 53 service.

Exposing all brokers via LoadBalancer and Envoy proxy. The Envoy can
reroute connection to proper broker. The Envoy can be replaced with
any other proxy Nginx etc. There will be bottleneck in hot path as
every connection will go through LB + proxy. It doesn't scale well
as one client can starve the network hardware.

This section is extremely important. See the
[README](README.md#rfc-process) file for details.

- Why is this design the best in the space of possible designs?
- What other designs have been considered and what is the rationale for not
  choosing them?

## Unresolved questions

- What parts of the design do you expect to resolve through the RFC
  process before this gets merged?
- What parts of the design do you expect to resolve through the
  implementation of this feature before stabilization?
- What related issues do you consider out of scope for this RFC that
  could be addressed in the future independently of the solution that
  comes out of this RFC?
