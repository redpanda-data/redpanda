- Feature Name: Leader epoch handling in Redpanda
- Status: in-progress
- Start Date: 2022-02-09
- Authors: Michal Maslanka
- Issue: https://github.com/redpanda-data/redpanda/issues/3689

# Executive summary

In Kafka, some of the client requests are fenced with the leader epoch. 
The leader epoch is a 32-bit integer that is increased every time the partition 
leadership changes. The leader epoch is used to detect stale metadata, 
fence produce and fetch requests, and recognize truncations.

## What is being proposed

The proposal is to use the Raft protocol `term` as a Kafka leader epoch. The `term` is semantically 
identical to Kafka's leader epoch as it is increased every time the Raft group 
leadership changes and it stays constant when leader is active.

## Why (short reason)

The leader epoch is used by Kafka clients to detect truncations and stale metadata. 
In Redpanda we are missing that mechanism. This may lead to some issues that 
can occur when one of the nodes is isolated and does not receive metadata updates. 
Returning the leader epoch will allow the client to identify this situation 
and update metadata against a different broker. 

## How (short plan)

We will expose Raft group terms that are already gathered in the Redpanda metadata 
infrastructure as leader epochs. Additionally, we will implement the 
`OffsetForLeaderEpoch` Kafka API to provide full compatibility. When exposed to 
the Kafka layer `raft::term` type will be wrapped in `kafka::leader_epoch` helper.


# Motivation
## Why are we doing this?

Adding leader epoch support will allow us to better handle stale metadata 
and unclean truncations.

### Note on unclean truncations in redpanda

Unclean leader election may happen in Kafka when stale replica is allowed to 
become a leader. This may lead to situation in which partition 
high watermark would move backward. 

Since Redpanda replication is based on Raft, we do not allow for unclean leader 
election that may lead to the high watermark being decreased. The only situation in 
which the high watermark may be decreased is simultaneous failure of the majority of 
nodes handling relaxed consistency writes such as `ACKS=1` and `ACKS=0`. It is 
more likely to happen for topics with replication factor of `1`. Then it would 
be enough for only one node to fail to move high watermark backward.

## What use cases does it support?

We will add support for a leader epoch in each request that requires it and is 
currently handled by Redpanda.

## What is the expected outcome?

The validation of a leader epoch that is consistent with Kafka.

# Guide-level explanation
## How do we teach this?

This is a standard feature of the Kafka protocol and we may use existing, related KIPS.

- KIP-101 Alter Replication Protocol to use Leader Epoch rather than High Watermark for Truncation
- KIP-232 Detect outdated metadata using leaderEpoch and partitionEpoch
- KIP-320 Allow fetchers to detect and handle log truncation
- KIP-359 Verify leader epoch in produce requests (not yet implemented in Kafka)

# Reference-level explanation
## Detailed design - What needs to change to get there

### Leader epoch metadata

We need to store the last stable leader term in `cluster::partition_leaders_table`.
The term will be directly mapped to the leader epoch returned in `MetadataResponse`. 

We will introduce a type representing the current leader and the term. 

```c++
    struct leader_term {
        std::optional<model::node_id> leader;
        model::term_id term;
    };

```

### Offset for leader epoch API handler

The `OffsetForLeaderEpoch` API returns the last offset in the request leader epoch plus 
one. That is the first offset of the next leader epoch, i.e. an offset 
on which the requested leader epoch ends.

The `OffsetForLeaderEpoch` is handled only by the requested partition leader. 
In order to implement that feature, we need to add an API that will allow us to 
query for last offset in the given term. Then the offset will have to be translated 
with an offset translator to obtain a valid Kafka offset.

For local partitions this is an easy task since segments are stored in a sorted 
collection. Each segment can contain only single term. 

For remote partitions (cloud storage), we will use 
information stored in partition manifests. The manifest contains the segment 
term and the offsets stored in a segment. Given that we do not allow two 
terms to be stored in the same segment, we may use that information to determine 
last term offset without downloading actual data.
## Drawbacks

We add more complexity to the implementation of few Kafka APIs. The complexity 
however will allow us to better recover from failures and network partitions and
will provide better compatibility with Kafka APIs. 

## Rationale and Alternatives

One of the considered alternative was to use a separate integer tracking 
leadership changes. That would give us a separation of concerns between term 
and leader epoch. This approach was rejected since the introduced integer would 
be semantically identical to already existing `term` and implementing all the 
necessary pieces would require a lot of work and coordination between all 
redpanda submodules.
