- Feature Name: Controller Refactoring
- Status: in-progress
- Start Date: 2020-05-29
- Authors: Michal
- Issue: ch545

# Executive Summary

As redpanda feature set started to grow it became clear that our current
controller design became hard to maintain. In its current shape the controller has
lots of responsibilities and many dependencies. In order to move forward and
make it easier and faster to develop new feature we need controller refactoring
that will leverage recently available Raft state machine abstraction and
state snapshots. New controller design should be decoupled from other system
components and have clear set of dependencies. Additionally the current
implementation lacks sound concurrency control.

## What is being proposed

The proposition is to refactor/split the controller to meet the following
requirements:

- Sound concurrency control
- Ease of adding new features
- Decoupling and reduced number of dependencies
- Clear and reduced set of responsibilities
- Ability to handle long running tasks (f.e. topic resharding)

## Why (short reason)

Currently controller has the following responsibilities:

- joining cluster
- creating raft-0 group
- managing topics (create/delete operations)
- controlling partition assignment
- updating the partition allocator
- updating metadata cache with topic state changes
- updating metadata cache with cluster member changes
- forwarding leadership notifications from raft groups to metadata cache
- managing nodes connection cache

Additionally the controller current concurrency control has following drawbacks

- it is pessimistic i.e only one operation can happen at the time which may
  become a bottleneck in the future
- in order to be correct it has to block indefinitely (no timeouts are allowed)
  while waiting for entries to be replicated

Large set of responsibilities and complicated concurrency control make it hard 
to maintain and extend.

## How (short plan)

Proposed solution includes splitting controller into separate and mostly
decoupled modules that will have clear responsibility boundaries.
Looking at the current list of controller functions we can enumerate
following mostly separate modules:

- **cluster members management** - it includes adding/removing nodes from the
  cluster, providing information about members to Kafka APIs,managing internode
  connections, handling cluster join sequence

- **partitions leadership handling** - this includes handling leadership
  notifications, caching current partition leaders, triggering metadata
  dissemination

- **topics management** - the topics management includes keeping topics state,
  validating topic/partition requests, state reconciliation (for optimistic locking),
  providing topic related information for Kafka API

- **partitions placement constraint solving** - this includes the algorithm
  providing information about how to assign partitions to resources existing in
  redpanda cluster in optimal way

In proposed solution each module is responsible for its own state. Metadata
cache used currently by Kafka APIs is designed as a facade over all
above-mentioned modules.

## What use cases does it support?

Proposed solution is going to support all the use cases that current controller
implementation does. Moreover the new design will be easily extendible and able
to support long running jobs.

## What is the expected outcome?

After the refactoring we should end up with controller code that is clear and
easy to maintain.


# Detailed design - How it works

## `cluster::members`

Cluster members is a map containing all cluster brokers.
It provides information to Kafka Metadata API and other components about broker
existing in the cluster and all the details about the brokers
(ip addresses, number of cores, etc.).

## `cluster::members_manager`

Members manager class is responsible for updating information about cluster
members, joining the cluster and creating connections between node.
This class receives updates from controller STM. It reacts only on raft
configuration batch types. All the updates are propagated to core local
cluster::members instances. There is only one instance of members manager
running on core-0.

## `cluster::metadata_dissemination_service`

The metadata dissemination service is responsible for disseminating and
listening to information about partition leaders. It will also propagate those
information to the nodes as not all groups are instantiated on all of the nodes.
The metadata dissemination service using disjoint sets based dissemination
algorithm. The service sends updates to all core local instances of
`partition_leader_table`.

## `cluster::partition_leader_table`

Class holding mapping between NTP and partition leaders. This information can is
independent from the one contained in topic_table as leadership notification can
happen before the node knows about given topic. The `partition_leader_table`
expose an interface allowing users to wait for leaders to be elected.

## `cluster::topics_frontend`

The topics frontend is exposing an APU to manipulate topics and partitions.
The frontend in responsible for request validation and requesting partition
allocator to get the partition assignments. Topics frontend creates the command
and replicates them using `controller_stm`.

## `cluster::topic_table`

The topics state is a core local mirror copy of all topics and partition
properties. (sharding can be easily implemented in future). The topic state
accepts applies from `controller_stm` and on apply it calculates operation result.
As `topic_state` instance exists on every core and it contains the same data the
`controller_stm` have to apply updates to instance existing at every core.
In order to do that we use `core_mirrored_state` wrapper class. The wrapper
replicates commands application on every core.

### Note about ordering

`raft::state_machine` and `raft::mux_state_machine` executes only one apply at
the time (they wait for apply to finish). Thanks to that updates are applied in
the same order on every core as no other updates can interleave with then one
currently being applied to state.

## `cluster::controller_backend`

The `controller_backend` is a component responsible for actually applying changes
being reflected in the state to the cluster. Currently this involves topic
creation and deletion. Backend queries the state components for changes and when
they are available execute actions. The backend component has the housekeeping
timer that retries failed and not finished operations.

## `cluster::controller_stm`

The controller STM is going to be component handling all controller related
state replication and updates. Currently the only state that redpanda stores in
controller are topic configurations and partition assignments. This is going to
change in the future as we are probably going to extend redpanda with features
like ACLs authentication, etc. Those feature will require keeping cluster wide
state. Additionally controller_stm applies raft configuration batches to
`cluster::members_manager`.

## `cluster::partition_allocator`

Currently named `cluster::partition_allocator` it will be update with the
information coming from the cluster members manager. The partitions scheduler
will be responsible for solving the partitions placements constraints. i.e. it
will assign partitions to hardware resources in an optimal way taking given
constraints (f.e. number of cpus, disk space, free mem, etc.).

### Topics operations component diagram

```plain

+----------+                  +----------+                +----------------+
| External |      Request     |          |     Replicate  |                |
|   APIs   +----------------->+ Frontend +--------------->+ Controller STM |
|          |                  |          |                |                |
+----------+                  +----------+                +----------------+
                                                          |     Raft 0     |
                              +----------+                +-------+--------+
                              |          |         Apply          |
                              |  State   +<-----------------------+
                              |          |
                              +-----+----+
                                    ^
                                    |  Wait for changes
                                    |
            +------------+    +-----+----+    +------------+
            |  Partition |    |          |    |    Group   |
            |   Manager  +<---+ Backend  +--->+   manager  |
            |            |    |          |    |            |
            +------------+    +----------+    +------------+
```

### Concurrency control

When executing operations at frontend level we do not require locking. We relay 
on state updates to be correctly 

## Interaction with other features

### Metadata cache

In its current shape the metadata cache holds all the state that is required to
fill Kafka metadata response. The state is updated by the Controller and
metadata dissemination service. In order to simplify the design and to be in
align with the SRP (Single Responsibility Principle) in the proposed design the
MetadataCache will be the facade over other members of `cluster` namespace.
The metadata cache core-affinity will be independent from the actual state
location as the Metadata cache facade, for simplicity, is going to be
instantiated on every core. It will root requests to different core or use core
local whole state copy depending on the use case.

```plain

   Kafka API                  Kafka Proxy
       +                           +
       |                           |
       |                           |
       |                           |
       |                           |
 +-----v---------------------------v--------+
 |                                          |
 |         Metadata Cache (facade)          |
 |                                          |
 +----+-------------+----------------+------+
      |             |                |
 +----v----+   +----v-----+    +-----v------+
 | Members |   |  Topics  |    |  Leaders   |
 +---------+   +----------+    +------------+

```

### Kafka API

All components that are used by Kafka API requests are instantiated on each core.
Request dispatching to target core and cross core communication is transparent.
There are currently only two components that are going to be used by Kafka API
those are the `cluster::metadata_cache` and `cluster::topics_frontend`.

## Drawbacks

The main drawback of current solution is the number of component that have to
interact together in order to provide the controller features.

## Rationale and Alternatives

The main alternative to current design that was considered was using a
pessimistic locking approach. The pessimistic locking may at the first sight
seems easier to implement but considering its performance penalty and what is
more important the need to implement state reconciliation (similar to the one
that is required for optimistic locking)

## Unresolved questions

Should we distribute entities over caches or keep a copy per core as it was
proposed in the design.

## Telemetry & Observability

Each of the cluster state management component will expose set of domain specific
metrics. The metric set should allow to check rate of successful operations and
rate of errors.


## Additional resources

- [Cluster components diagram](https://docs.google.com/drawings/d/14UMFwXATAwSzDo9pZQX2VefMh3gsgX7r2BdguFDDH2U/edit?usp=sharing)
- [Topic creation sequence diagram](https://www.websequencediagrams.com/cgi-bin/cdraw?lz=CnBhcnRpY2lwYW50IGthZmthX2FwaQAJDXRvcGljc19mcm9udGVuZAAlDQA5BXRpb25fbGVhZGVycwANEV9hbGxvY2F0b3IAQRRzdGF0ZQB3DWJhY2sAVhBjb250cm9sbGVyLXN0bQCBJg1yYWZ0LTAKCm5vdGUgb3ZlcgAeDywAGwY6IFNUTQoAgVcJLT4AgUYPOiBjcmVhdGVfAIFoBSgpCgCBZg8tPgCBPg46IGFzc2lnbl8AgXgJcyhwbGFjZW1lbnRfY2ZnKQCCVAUAgXMKAF8TAIIyCgBGBm1lbnQAZhIAgW4OOiByZXBsaWNhdGVfYW5kX3dhaXQoAIEvDF9jbWQpCgCCIw4tPgCBfQgANAkAJg5vcCkKAIJDBgBUHHJlc3VsdDxvZmZzZXQ-ACkIAIEQEmFwcGx5KGJhdGNoAHgSAINsDAAjCAB7EQCEEQwAgmgSY29uZmlybQCEUQhpb24AIg8AgiMQZXJyYzo6c3VjY2VzcwB6GACEAAoAIQ4Ag3QVAIVmDTogd2FpdF9mb3IAhXcOAIYLDACEXhNuZXcAhiwJCgoKbG9vcCByZWNvbmNsaWxpAIFTBSBsb29wCiAgIACGDggAgjAQAHAJZGVsdGFzAB0OAIZDBzogY2hlY2sgaWYgAIZpBSBpcwCDHgVpZWQKZW5kAIVYEgCHfQkAhg0IAId8BiAAg3oGCg&s=default)