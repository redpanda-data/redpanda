- Feature Name: Partition Hibernation
- Status: draft
- Start Date: 2022-04-08
- Authors: John Spray
- Issue: (one or more # from the issue tracker)

# Executive Summary

Avoid idle partitions using system resources.

## What is being proposed

A new state for partitions, where they exist in the topics table
but do not have active raft groups.

## Why (short reason)

To enable higher scale when a significant proportion of topics
are idle.

## How (short plan)

Topics will be automatically placed into the hibernate state when
they have been idle for more than a configurable threshold.

## Impact

Reduced cost of delivering free/cheap public-facing services, where
users are likely to create topics for occasional or one-off use.

# Motivation

## Why are we doing this?

Partitions have some runtime resource overhead, even if they aren't being
used, and haven't been used for some time.

To support public-facing Kafka services on which many users might create
and then abandon idle topics, we could implement a "hibernate" mode
for partitions that haven't been used for a long time.

## What use cases does it support?

Implementing a low-cost/no-cost public service, in which developers are likely
to create unused topics with which to experiment.

## What is the expected outcome?

Systems will run smoothly with a higher number of partitions than
they could before, as long as some of those partitions are idle.

# Guide-level explanation

## Introducing new named concepts.

`Hibernated topic`: a topic whose partitions do not have active raft
groups running in the cluster.  It cannot serve user I/O (aside from
metadata requests) until it is woken up.

`Waking a topic`: a control plane operation to change the status of
the topic, followed by raft elections for all partitions within the
topic.

These concepts are not visible to applications.  They are optionally visible
to system administrators optionally:
- system monitoring telemetry, where we would surface indicators for how many partitions
  and topics are currently hibernated.
- tunable configuration properties, where they could control the delay til hibernation
  if needed.


# Reference-level explanation

## High level design

The topic table, which stores the definition of the topics & partitions
on the system, is driven by controller log commands.  New commands
are added for hibernate and wake.  Nodes in a partition's raft group
notice the hibernation of the topic, and shut down the raft group.

There is a "hibernating" intermediate state where members of the
partitions' raft groups are responsible for refusing writes, and
reporting the highest offset back to the controller so that it can
reliably store the frozen offset state of the topic when storing
the controller message that finalizes the hibernation.

Once the topic is hibernated, all its partitions will report a
fictional leader, so that clients know where to send metadata requests,
and so that monitoring systems do not incorrectly flag the partition
as being in a problematic state.

On startup, a hibernated partition's storage resources are not
recovered or replayed.  Hibernated partitions are not subject to
continuous retention policies, BUT the controller can notice if
a partition has been hibernated for longer than its retention.ms
limit, and wake it up for long enough to do housekeeping.

We should still be able to serve metadata requests for idle topics,
because otherwise any monitoring system will end up re-awakening
the topics whenever querying them.

## How do we decide when to hibernate?

We don't persist an `atime` type stat for partitions, and rightly so,
because otherwise we'd be writing metadata every time someone did
a read.

Detecting an idle partition at runtime therefore relies on a partition
staying up with stable leadership long enough to reach the idle timeout.
That's fine if the timeout is a few hours long (if we're changing leaders
more often than that then we've got bigger problems) but more problematic
if we have a long idle timeout (perhaps a day or more) with a reasonable
chance of leadership changes or node restarts before we reach it.

If we wanted to support very long idle timeouts better, we could
do so by adding something like a kvstore record of the access time
that is updated at most every N minutes, and when a leader is elected
it primes its access time from that if it is the same node that was
also the previous leader.

## What resources are we saving by hibernating?

- Avoid creating new files on log roll during startup
- Avoid replaying segments during startup
- No memory overhead from storing segment indices, list of segments, etc.
- No raft heartbeats for hibernated topics
- No data for hibernated topics in node health telemetry messages
- No prometheus output for topic-tagged metrics

And the resources we still consume for hibernated topics:
- Disk space for any segments that had not expired before we hibernated
- Memory for the topic's frozen metadata
- Bandwidth to awake and move the data on rebalance or node decommission

## Interaction with other features

Any 'scrub' feature will probably end up waking partitions up to
scrub them, but that is manageable as long as the scrub mechanism
properly limits the number of partitions to scrub at once, and groups
the partitions together by partition when scrubbing them.

Partition movement (especially during node decommission) will require
waking up the topic.  This should not create a thundering herd during
node evacuation, because partition movement is already scheduled to 
avoid moving all at once.

Any continuous data rebalancing feature that we implement should
prefer to avoid migrating hibernated topics, although it can wake
them up if they're the only viable candidates to resolve resource
imbalances.

All internal partitions will be exempt from hibernation.

Partition health monitor data will include an extra field for
last access time of a partition, or perhaps just the most-recent
access per topic on each node, so that the controller can use this
data to figure out the global per-topic access time to enforce
hibernation timeout.

## Telemetry & Observability

- Gauge for number of topics hibernated
- Gauge for number of partitions hibernated
- Counter for number of hibernate events
- Counter for number of wake events
- rpk commands for listing topics by status

## Detailed design

### List of controller commands
### List of RPCs

## Corner cases dissected by example.

## Drawbacks

### Resource determinism & what happens if all partitions go active?

Redpanda's resource footprint
will grow and shrink depending on the number of partitions that are
idle.  We can mitigate that somewhat if we set a ceiling on the number
of partitions that may be active, and force-sleep the least recently used
partitions when we hit the limit, but that doesn't help if all the partitions
in the system really are active (we'd just be flapping them in and out of
hibernation).

That nondeterministic footprint means this only makes sense at some
statistical scale where the ratio of idle partitions can be proactively
monitored and the system grown/shrunk in response.  For example, we might
have a system that is in principle able to support 1M partitions across
ten nodes, but when many are idle it might shrink right down to a 3 node
cluster.  We would re-expand the cluster (either by hand or via some
external infrastructure automation) if that idle ratio changed.

### Latency to wake

There will be a short delay on the first I/O to a partition that wakes
it from hibernation.  On a healthy system this should be in the range
of a few hundred milliseconds.

## Alternatives

### Do nothing

The practical capacity of a system will be how many _active_ partitions
we can handle: this has the advantage of determinism, as there is no
risk of a system 

### Radically improved resource management elsewhere

Separately to this RFC, we know that various subsystems' resource management
can be improved, such as the way we keep unbounded in-memory indices in
the storage layer, and hold open file descriptors for segments.

In the raft layer, hibernation would be less important if we moved to a
model where several partitions were grouped together in one raft group,
thereby avoiding the "heartbeat per partition" scaling issue.  This would
be a large architectural change.

### Only hibernate on explicit command from user

This is viable where an external control plane has its own tracking
of the most recent I/O on topics.  However, it requires a more
chatty external protocol for the external control plane to learn
the access times of partitions to figure out which ones should be
shut down.

As long as the policy for hibernating is as simple as a timeout, it
makes sense to build it in.  We could optionally also add an API for
hibernation so that 

### Hibernate per-partition rather than per-topic

There is a reasonable expectation that applications will produce
and consume to topics rather than partitions, and per-topic hibernation
is much simpler to expose in user interfaces, as well as writing less
to the controller log to control it.

A situation where some partitions in a topic are idle but others
are active is a fringe case, and this feature is aimed at providing
statistically meaningful efficiency improvements at scale: it's fine
if we miss the opportunity to hibernate any "weird" partitions like that.

### Allow reads from hibernated partitions

If a partition is hibernated, its data is still present on disk, and it
is safe to serve reads from any replica, because no writes are going
on.

If reads are going on concurrently with a partition waking up,
this takes careful handling.  The node serving reads is not part of a raft
group, and may not see the wakeup or the new raft group form, and thereby
could end up serving stale reads.  To make it safe, the raft group would
have to refuse to form until the node that had been serving reads acknowledged
that it has noticed the partition wake up.

The main reason not to do this is that reads consume system resources, so
it contradicts the purpose of the feature.

*However* we can service the special case of a fetch from an offset higher
than the frozen offset of a the partition at time of hibernation.  That way,
if the user leaves consumers running, they won't wake up the topic.  The topic
can stay hibernated until a write happens.


## Unresolved questions

- Should hibernate also have an explicit API, as well as the autonomous
  timeout-driven mode described in this RFC?
- Should hibernate be on by default, or not?  Being off by default simplifies
  the experience for single-tenant clusters.  Maybe this feature should only
  be switched on by users who know they have a meaningful number of
  idle topics.
- Should we add a persistent record of a partition's last access time, so
  that we can more rigorously enforce the hibernation timeout even if the
  system isn't running continuously for long enough to reach the idle timeout
  based on in-memory tracking of access?
