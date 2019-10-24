- Feature Name: add cluster::parition_allocator
- Status: in-progress
- Start Date: 2019-10-20
- Authors: Alex
- Issue: #223

# Summary

We introduce a new type `cluster::partition_allocator`.
It is a round-robin selector of machines followed by a greedy 
assignment of lowest loaded core.

The proposed algorithm handles machine diversity and rollbacks of
partial assignments (see tests). That is, in the case
that we cannot allocate all replicas on different machines,
we rollback the partial assignments and return an allocation failure.

## What

Adding a centralized place for constraint solving, partition
assignment.

## Why (short reason)

We need a centralized allocator capable of taking  advantage 
of non-uniform hardware. Hardware will
become non-uniform even if it starts out uniform as components fail
throughout the life of the program. Machine and data center hardware
evolution ensure that most of the time, in the long term, we'll
be facing with heterogeneous hardware.

## How (short plan)

We'll be extending the `cluster::controller` to keep track of
machine registrations

## Impact

This is required for v1. Fundamental to product.

# Motivation

We need a centralized place for constraint solving, taking into account
disks sizes, networking, core count, etc.

We expect an even of load given a set of constraints. This
proposal only covers even distribution among cores.

# Guide-level explanation

We introduce a new type `cluster::partition_allocator`. The type is responsible
for even distribution of load across _all cores_ in the cluster.

In addition, it sheds a _little bit_ of load from core 0, by increasing the weight
for the core by _exactly_ 2 partitions, see `cluster::allocation_node::core0_extra_weight`.

The idea is that core-0 is already busy with
default-allocated work for all the `cluster::controller*` logic

# Reference-level explanation

`cluster::controller` needs to delegate the actual _physical_ placement
of partitions to a scheduler. The oracle should solve 2 problems:

1. even distribution of load across _all_ cluster cores
2. do partition allocation respecting constraints (taints & tolerations)
   Note: We do not discuss taints and tolerations in this design
   as we are postponing the discussion and implementation of
   advanced soft/hard constraints, commonly known as taints & tolerations[1]

The flow of information is

Kafka Admin -> _local_ controller -> _global_ controller -> partition_allocator


## Telemetry and Observability

* The telemetry is observed locally at each node with the number
  of partitions assigned to each node

* Partition alloation telemetry is deferred

## Detailed design - how


Assume this POD

```
struct allocation_node {
    /// physical machine
    model::broker _node;
   
    /// number of partition allocations left
    /// initialized to (#cores * 7000) - 2
    uint32_t _partition_capacity;
   
    /// each index is a CPU. A weight is roughly the number of assigments
    /// _weights.size() == # of cores
    std::vector<uint32_t> _weights;
};

```


The general algorithm for balancing _# of cores_ only:

* Compose a list of all nodes with capacity to allocate partitions
** This is kept in an intrusive linked list, when nodes become full
   the list removes the node from the available nodes with capacity
   
* iterate over all partitions
* for all replicas, choose next computer with our round robin pointer

** if no match, rollback _all_ previous allocations and return std::nullopt 



At the node level
`allocation_node::allocate()` gets us the lowest loaded core via
`min_element()` (by weight), which ensures perfect balance within the machine.

```
    uint32_t allocate() {
        auto it = std::min_element(_weights.begin(), _weights.end());
        *it++; // increase weight
        return std::distance(_weights.begin(), it);
    }

```

This algorithm gives us round-robin assignment for diversity of machines 
and good balance per _individual machine_ at the core level.


## Drawbacks

## Rationale and Alternatives


### Why is this design the best in the space of possible designs?

This design is very simple. We keep a single pointer to the
the next element to do round robin, and when that element is 
saturated, we remove it from the list. We attempt to add it on
deallocation if we later remove the assignment.

The solution however ignores rack diversity, soft constraints
also known as affinity, anti-affinity for negative
constraints as well as _hard constraints_ whic make
the affinities a failure on allocation.

These techniques require a much more sophisticated scheduler,
similar to the `max-min`[1] proposed in the Apache Mesos paper
which when taken multiple resources into account is known as
Dominant Resource Fairness (DRF)[2]


### What other designs have been considered and what is the rationale for not choosing them?

- DRF - It is correct, decays correctly into max-min
  and used at Google for Borg, Kubernetes and Apache Mesos to run
  very large workloads.

  DRF is complicated and requires more unification of models & constraints.
  It is unreasonable to expect a good implementation in 1 week of DRF.
  
- std::shuffle() + random node selection. The shuffle as the list of nodes increases
  is very expensive. It calls the move ctor on all elements effectively.

- jump consistent hash + linear probing proved required a lot of state to keep track
  of which CPU's have been assigned to make the correct jump. The implementation
  was much more complex with very little gain over std::shuffle
  
- rendezvous hashing should also provide a decent balance

```
Destination = namedtuple('Destination', ['host', 'hash', 'weight'])


def score(hash_value, weight):
    return -weight / math.log(hash_value / HASH_MAX)


def pick_destinations(key, destinations, k=1):
    key_hash = hash_key(key)
    annotated = [(score(merge_hashes(key_hash, dest.hash), dest.weight), dest.host)
                 for dest in destinations]
    ordered = sorted(annotated)
    return [host for _, host in ordered[:k]]

```

- Ceph CRUSH algorithm is also an option for load balancing

### What is the impact of not doing this?

N/A. We need _some_ partition allocator for the product.

## Unresolved questions


* Internal RPC discussion in case of controller forwarding
* usage of `machine_lables` is deferred discussion for RPC RFC
* Taints & Tolerations scheduling, also known as soft and hard
  constraints.
* Interface exposed to the Kafka Admin API
* Prometheus Telemetry & Observability is deferred,
  covered in Issue #224
* We don't yet take into account system saturation.
  We throw an exception when we've assigned 7K partitions to
  a particular core which should be about 7TB on the fastest collection of segments (1GB)
  and about 112TB on a 16core machine.


## References

[1] https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.16/#pod-v1-core
[2] max-min: https://en.wikipedia.org/wiki/Max-min_fairness~
[3] DRF: https://cs.stanford.edu/~matei/papers/2011/nsdi_drf.pdf
