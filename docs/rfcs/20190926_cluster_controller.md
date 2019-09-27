- Feature Name: cluster::controller 2-phase local node bootstrap
- Status: draf
- Start Date: 2019-09-26
- Authors: alex
- Issue: (one or more # from the issue tracker)

# Summary

two-phase local node bootstrap.

controller is our global partition. It is a model::ntp("redpanda", "controller", 0) alias.
controller is effectively a global metadata cache. It is a compacted topic.

Upon local node start, restart, crash, etc,
we bootstrap by read the controller contents entirely. We expect the
ammount of data to be actually small, i.e.: less than 1GB of content.

While we are reading the contents, we delegate to
`sharded<cluster::partition_manager> pm` the recovery of particular `ntp` via
`cluster::partition_manager>::manage(model::ntp, raft::group_id)`.
This partition manager holds one `storage::log_manager` instance. The
`storage::log_manager::manage(ntp)` will perform the filesystem recovery of
the log segments for a given `ntp`. Note that a `ntp` may have thousands
of individual log segments.

This proposal removes the knowledge from the storage about which core `ntp`
belong to. It pushes this to the place of knowledge `cluster::controller`



```
              +               Kafka +-----------+
              |                 +               |
              |                 |               |
              |          (a)    |               |
              |                 |               |
              |                 |               |  (h)
              |                 |               |
              |                 v               |
              |               cluster           |
              |                 +               |
              |                 |               |
              |          (b)    |               |
              |                 |               |
              |                 v               | read-path [h]
              |           consensus::raft       |
write-path    |            +          +         |
 [ a-g ]      |       (d)  |       (e)|         |
              |            v          v         |
              |           RPC       storage<----+
              |            +
              |            |        (leader / replica 1)
              |            |
              |            |
              |            |
              |     (f)    |    (g)
              |     +------v-------+
              |     |              |
              |     v              v
              +  replica-2      replica-3

```

# Motivation

We need to have a sound bootstraping and log recovery mechanism
upon a machine restart (from shutdown, reboot, crash, etc)

# Guide-level explanation

`cluster::controller` reads the full `cluster::partition`. It will scan for
all the assignments for `model::node_id` belongs to the machine.

It will build 2 indexes. `model::ntp -> seastar::shard_id` and a
`raft::group_id -> seastar::shard_id`. This abstraction is captured by
the `cluster::shard_table`.

`cluster::controller` _copies_ the full assignment to all cores.
Once `cluster::shard_table` is built (and copied to all cores).

We will do a concurrent recovery of all the `model::ntp`. First,
we delegate the recovery to the `sharded<cluster::partition_manager>`.
Every time we ask the
`cluster::partition_manager>::manage(model::ntp, raft::group_id)` to
manage an ntp and raft group, it will create an entry on the
`storage::log_manager` via the `storage::log_manager::manage(ntp)`.

The `storage::log_manager::manage(ntp)` will perform the particular `ntp`
recovery of all log segments on disk.

Note that this operation is happening concurrently. Currently the
`storage::log_manager::manage(ntp)` uses the `seastar::default_priority`
so it means that recovery is happening fairly across all cores and within
each core doing all the IO for recovery.

# Reference-level explanation

This RFC is attached to an interface prototype.

## Detailed design


1. cluster::controller must perform the bootstrap
2. storage must remove the core-assignment lookup or defer to interface
3. We must wire the protocol through redpanda/main.cc
4. We must include a e2e test for it.

## Drawbacks

Why should we *not* do this?

Recovery now has a constant cost before actual machine bootstrap can occur.
However, as long as controller is small (1GB), two-phase recovery will be a
negligible cost since the _majority_ of the cost will be on recovering the
actual log segments.

In addition, when we hit this bottleneck, we can create a _secondary_ log
with _only_ the assignments for this machine which maybe quite small (thousands)
A cache of assignments.

## Rationale and Alternatives

- Why is this design the best in the space of possible designs?

* First, we have no node bootstrapping yet.
* This design is iterative. It is a simple & correct design for node bootstrap.
* The process can be expanded with time, it just sets up the correct
  expectations for subsystems, namely:
  **  storage does not need to know the cluster assignment at the
      `log_manager` level
  **  Wiring of components happens at `main.cc`


- What other designs have been considered and what is the rationale for not choosing them?

* Static partitioning:
  `model::parition_id::type % smp::core_count` is fragile and offers no real
  load balacing system. For example, core0 will always do a little more work
  in most systems because it is the only core guaranteed to exist.
  Second, having a cluster level knowledge gives us the opportunity to optimize
  physical core placement.
  Third, having a global component allows us to do global optimization
  strategies rather than node-local-maxima strategies

## Unresolved questions


* Wiring through the Kafka API is out of scope.
  Both in the current form and for the Management API
