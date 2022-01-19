- Feature Name: Raft replication simplification - phase one
- Status: in-progress
- Start Date: 2022-01-19
- Authors: Michal Maslanka
- Issue: #1829 (Quality initiative)

## Current solution

Redpanda's Raft protocol implementation currently supports two replication 
modes, or consistency levels - the Quorum replication mode and the 
leader ack mode.

The Quorum replication mode works the same as the 'original' 
version of the Raft protocol, i.e. the replication call finishes when the requested entry 
is safely replicated to the majority of the replicas and is flushed. 

In the leader ack mode (Kafka's ACKS=1) the `replicate` call finishes as soon as the write 
to the leader disk is finished (before it is flushed). 

The entries' visibility for  readers is also influenced by 
the replicate consistency levels. 
For entries replicated with Quorum consistency level, Redpanda makes them visible as soon as 
they are fsynced on the majority of nodes. On the other hand, entries with the 
`leader_ack` consistency level are made visible as soon as they 
are written to the follower's disk, but they are not required to be fsynced.

In the current implementation, the two consistency levels
use totally different approaches and code paths. Quorum level replication 
requests go through the batching on the leader `raft::replicate_batcher`
and then they are dispatched to the followers via `raft::replicate_entries_stm`. 
Raft `replicate_stm` writes data to the leader disk and then dispatches 
`append_entries_requests` to the follower in parallel with the leader disk flush.
In this case, Raft implementation may have multiple requests in flight per follower. 
(currently the number of requests pending is limited by per follower semaphore). 

Leader ack consistency replication is implemented differently. 
Control is returned to the caller immediately after the data is written 
to the underlying `storage::log`. On the next heartbeat, the follower 
is recognized to be behind the leader and is recovered by `raft::recovery_stm`. 
The `recovery_stm` reads a chunk of data and sends it to the follower 
in a loop (a single request at a time).

## Purpose of the simplification

There are multiple reasons that makes the simplification necessary and demanded, 
the most important are:

- code simplification
- unification of approaches
- better handling of requests with mixed consistency levels (currently they cause latency spikes)

Also some future features that will benefit from simplification are

- in-memory acks
- leaders that are remote to data 


## Proposed solution

Proposed solution assumes that both currently supported consistency levels will 
be handled in the same way with use of `replicate_batcher` and `replicate_entries` stm. 
This approach has several advantages over the one currently 
used for `leader_ack` consistency level. 

The current implementation of `quorum_acks` replication is robust and pretty 
well tested. No large code changes are required to use the 
`replicate_batcher` and `replicate_stm` to handle `leader_acks` replication. 
The way that the `replicate_stm` handles `append_entries` requests dispatching 
and leader log writes will allow us to make them parallel in future. 

Additionally, using `replicate_stm` prevents us from creating the 
append entries request twice since batches coming from `replicate_batcher` 
are not first handed off to the disk and then read again, but rather shared to 
generate a leader disk write and follower `append_entries` requests. 
This approach significantly reduces the `storage::batch_cache` pressure and may 
also reduce the disk usage (in some clusters with limited memory cache, the hit 
ratio is not always 1.0 even if there are no reads other than the reads issued by 
`replicate_stm`).

### Backpressure

In the current `quorum_ack` replication implementation, the backpressure is 
propagated from the followers to the `raft::replicate` caller 
(the Kafka Produce request handler effectively). Currently, the backpressure 
propagation is based on the per-follower semaphore since the RPC layer 
lacks a backpressure propagation mechanism. 
When `replicate_entries` stm is unable to acquire follower dispatch units, 
it will wait, holding the `replicate_batcher` memory reservation lock so that 
it can not accept writes indefinitely. When the `replicate_batcher` memory 
reservation semaphore is exhausted, it prevents the first stage of `replicate_stages` 
from finishing, not allowing more requests to be processed. 

The current `leader_ack` mechanism lacks backpressure propagation. 
The leader can be an arbitrary large number of entries ahead of followers. 
This solution may be a problem since the user has an impression that data is 
replicated while the data is actually stored only on the leader.

In the proposed solution, we are going to handle backpressure in the same way 
for both consistency levels. This way there will be a guarantee that followers  
can always keep up with the leader.


### Memory usage

Memory usage control will be achieved in the same way it is done right now, 
i.e. we will not finish first stage of replication when the `replicate_batcher` 
memory semaphore is exhausted. This way we will prevent the Kafka layer from 
buffering an indefinite number of batches in pending replicate requests. Replicate 
batcher memory units are released after the request is handed off to the 
socket so that all the user space buffer can be freed. 
In future, we may consider a per-shard semaphore that controls the total amount 
of memory used by all the replicate batchers.

### Recovery STM coordination - finishing recovery

`recovery_stm` is started every time the leader identifies the follower as 
being behind. Since recovery stm is currently used for both recovering the 
follower (when it is actually behind) and delivering follower data replicated 
with the `leader_ack` consistency, the `recovery_stm` implementation contains some 
optimization to minimize the `leader_ack` latency. 

There are two main optimizations:
- not stopping the recovery when the last replicate request consistency level is `leader_ack`
- triggering the next dispatch loop immediately after the leader disk append

These optimizations make the handling of the recovering follower more complex in 
other scenarios, such as `leadership_transfer`.

Since there is no coordination between the two paths that the follower append 
entry requests are sent from (i.e. `replicate_stm` and `recovery_stm`), those 
requests may be reordered and cause log truncations and redeliveries, which lead 
to increased latency.

In the proposed solution, since we are not longer going to rely on the 
recovery STM to deliver messages to the follower in normal conditions, the complicated
handling of recovery finishing logic will no longer be needed. 
When the follower is up to date with the leader, `recovery_stm` is simply not running.

The only situation that will require coordination is when the follower 
is no longer behind the leader, but `replicate_stm` skips dispatching requests 
to the follower as it is still recovering. This may lead to a situation in 
which the next leader append will happen before the `recovery_stm` received the follower 
response for the last request, i.e. it cannot finish recovery because the log end already 
advanced. This case may be handled by coordinating dispatching follower 
requests from `recovery_stm` and `replicate_stm`. 
We can introduce the last sent offset to 
`raft::follower_index_metadata` to check whether recovery should finish 
instead of the check being based only on the follower response.

This way, the `replicate_stm` will not skip sending a request to the follower if 
it is the next request the follower is supposed to receive. On the other hand, the 
`recovery_stm` will not try to send the same request again, but will finish, 
assuming the response will be successful.

### Implementation details

To implement the simplified solution, we will pass the `consistency_level` 
parameter to the `raft::replicate_batcher::replicate` method. 
The `raft::replicate_entries_stm::apply_on_leader` method will return immediately after 
the successful `leader_append`. The full quorum replication round will be finished 
after the `raft::replicate_entries_stm::wait_for_majority()` returns. 
The replicate batcher, based on the cached request `consistency_level`, will decide when 
to release the request, either after `apply_on_leader` finishes or 
`wait_for_majority` finishes.

As in current implementation, the `raft::replicate_entries_stm` will
release batcher memory units after dispatching the follower request. 
This will allow propagating backpressure for both consistency levels.
