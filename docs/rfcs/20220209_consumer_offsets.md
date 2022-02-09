- Feature Name: Support for Kafka `__consumer_offsets` topic
- Status: in-progress
- Start Date: 2022-02-09
- Authors: Michal Maslanka
- Issue: https://github.com/Redpanda-data/Redpanda/issues/3701

# Executive summary

In Kafka, consumer offsets and group metadata are stored in a `__consumer_offsets` 
topic. The data stored in the `__consumer_offsets` topic can be read and updated
by external Kafka clients, such as MirrorMaker2. To be fully compatible with the 
ecosystem, Redpanda must expose the `__consumer_offsets` topic in the same 
data format as in Kafka.

## Proposal

The addition of a `kafka/__consumer_offsets` alias for the current `kafka_internal/groups` 
topic along with a translation layer that will allow clients to treat 
`kafka/__consumer_offsets` as a regular internal topic. 

## Why (short reason)

Creating the `kafka/__consumer_offsets` alias will decouple the Redpanda 
data format from the format that is used in Kafka. We may still continue 
evolving the `kafka_internal/group` topic content and add Redpanda-specific 
extensions or record types. Additionally, we will not have to adapt the 
internal Redpanda logic to meet Kafka requirements. The only part that would need 
an adaption would be the data translation layer.

## How (short plan)

We will introduce a mechanism that will allow adding an alias to 
any topic in the `kafka_internal` namespace. Additionally, we will add the ability 
to assign a simple data transformation policy related to an alias. 
Each record that is produced or fetched from a topic will need to be transformed with the 
alias-related transformation layer. 
Since we tolerate gaps in offsets, the data transformation layer may also filter out 
unnecessary batches when processing them and perform validation of 
the batches that are written to the topic.


# Motivation
## What use cases does it support?

The following APIs must be supported for internal topic aliases:

### `MetadataAPI`

Internal topic aliases will be included in the list that is returned in 
`MetadataResponse`. Internal topic filtering is done on the client side.

### `ListOffsets`, `DescribeConfigs`, `DescribeLogDirs`

Each of the requests will access the aliased `kafka_internal` topic and return 
the corresponding properties.

### `AlterConfig` `IncrementalAlterConfig`

Since Kafka allows users to change internal topic properties,
we need to do the same. The `AlterConfig` and `IncrementalAlterConfig` properties 
will alter the properties of an aliased topic.

### `CreateTopic`

When `CreateTopic` is called for an aliased topic, we will create an aliased internal
topic with the properties that are requested during topic creation. 

### `Fetch`

`Fetch` will create a dedicated translating reader that will perform data 
translation on the fly.

### `Produce`

Producing to a topic alias will produce to the aliased `kafka_internal` 
topic. Data will then be persisted to the internal topic alias where the related data 
transformation will be applied.


### Compatibility with Kafka

We currently support the following versions of `__consumer_offsets` types:

- `OffsetCommitValue`: version 1 
- - We do not support `expiryTimestamp`, but we always return `-1` (i.e. no expiration)

- `GroupMetadataValue`: version 3

- `MemberMetadata`: version 3
- 
# Guide-level explanation
## How do we teach this?

- Explain to users that we added another layer of compatibility with Kafka.
- Explain what kind of data is accessible by consuming the `__consumer_offsets` topic.

## Introducing new named concepts

We will introduce an `internal_topic_alias` abstraction that will allow mapping topics
from the `kafka_internal` namespace to topics marked as internal in the clients' 
accessible `kafka` namespace.

# Reference-level explanation

## Interaction with other features

### RPK

We may want to add an `--internal-topics` flag to `rpk` to allow the user 
to interact with the `__consumer_offsets` topic. Additionally, we may want to add 
decoding capabilities so that `rpk` will be able to display human/machine 
readable content from the `__consumer_offsets` topic.

### Admin API

There is no need to expose the `__consumer_offsets` alias via the Admin API. 
We may assume that Admin API users are aware of Redpanda internals and 
we may operate using the `kafka_internal/group` Redpanda-specific topic name.


## Corner cases dissected by example

### Topic `kafka/__consumer_offsets` already exists in the cluster

In this case we will just use whatever is there and ignore the alias.

### Topic `kafka/__consumer_offsets` is created by the user

In this case, Redpanda will create an underlying `kafka_internal/groups` topic with 
a specified set of options provided by a caller. We may emit a WARN log message 
that the topic being created is an internal topic. 

## Detailed design - What needs to change to get there

### Internal topics

We will introduce a `bool is_internal_topic(const model::topic&)` function
that will return true whenever a topic is internal. This will provide 
compatibility with Kafka internal topic abstraction. Topics are marked as 
internal in `MetadataResponse`. 

### Topic aliases

Add an `std::vector<topic_alias>` sequence container to the topic table where topic aliases will be 
registered. We can tolerate the linear lookup const since the number of aliases 
is very limited (currently 2).

```c++
struct  topic_alias {
    model::topic_namespace alias;
    model::topic_namespace source;
};
```

`cluster::metadata_cache` will transparently handle all APIs for aliased topics.

NOTE: `topic_alias` data structure is generic. However, for simplicity in the
Kafka API handlers, we will handle aliases for topics marked as `is_internal`.

### Translation layer

We must add a data structure that will hold the data transformation factor related to the given 
topic alias. Reference to that data structure will be provided by request via 
`kafka::request_context`. Data transformation will be a simple class, 
creating `model::record_batch_reader` instances that will apply a simple 
transformation.


## Drawbacks

The main drawback of the proposed solution is that it must be special cased in 
multiple places in the code. However, creating a generic internal topic alias 
mechanism will allow us to more easily handle similar cases in the future,  
such as support of a transactions topic.

## Rationale and alternatives

The main advantage of the approach presented here is the decoupling of the Redpanda implementation 
from Kafka internals. An alternative approach would be to migrate all the data 
from the `kafka_internal/group` topic to the new `kafka/__consumer_offsets`. 
This, however, would bound the Redpanda implementation to the format of the data that 
Kafka stores internally. Additionally, we would need to add special handling 
every time the Redpanda implementation changes. 

## Unresolved questions

- How do we handle the `CreatePartitionsAPI` for the `__consumer_offsets` topic?
  Changing the number of partitions in the group topic would change the 
  `group_id` to `partition_id` mapping.

