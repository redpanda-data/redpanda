- Feature Name: Client conection limits
- Status: implemented
- Start Date: 2022-02-07
- Authors: John Spray
- Issue: 

# Executive Summary

Add per-client state to track and limit the number of connections open.

Similar to KIP-308, adapted to Redpanda's thread per core model.

## What is being proposed

New configuration properties:
- kafka_connections_max (optional integer, unset by default)
- kafka_connections_max_per_ip (optional integer, unset by default)
- kafka_connections_max_overrides (list of strings, default empty)

New prometheus metric:
- vectorized_kafka_rpc_connections_rejected

When a connection is accepted which exceeds one of the configured
bounds, it is dropped before reading or writing anything to the socket.

## Motivation

* Avoid hitting system resource limits (e.g. open file handles)
  related to port counts
* Prevent rogue clients from consuming unbounded memory (we allocate
  some userspace buffer space to all connections, and the kernel
  allocates some buffers too).
* Help users notice if a client is acting strangely: an application
  author opening large numbers of connections will tend to notice
  themselves when they hit the limit, rather than the operator
  of the cluster noticing when the cluster performance is impacted.
* Provide a client allow-listing/deny-listing mechanism (e.g.
  kafka_max.connections_per_ip is set to zero and
  kafka_max_connections_per_ip_overrides specifies
  permitted clients)
* Continuity of experience for Kafka users accustomed to
  max.connections.per.ip

## How

### Accepting a connection

If both configuration properties are unset, then no action is taken: this
preserves the existing unlimited behaviour.

A sharded service storing per-client tokens for open connections, clients are
mapped to shards by hash of their IP.

In kafka::protocol::apply, acquire a token at start and relinquish it at end,
probably using a RAII structure, although this function already provides a
convenient localized place to take+release a resource explicitly.

On first connection from a client, the shard servicing the connection makes a
cross-shard call to the core that holds state for this client IP hash.  This
initializes the client's state:

```c++
struct client_conn_quota {
  // The max per connection.  This is set by applying
  // the combination of per_ip and per_ip_override settings.
  uint32_t total;

  // How many connections are currently open.  This may be at
  // most `total`, although it may exceed it transiently if
  // configuration properties are decreased during operation.
  uint32_t in_use;
}
```

The connection quota state is stored in a absl::btree_map: since the overall
number of unique clients maybe high (so a flat store is risky), but the entries
are small (so a std::map would generate a lot of tiny allocations).

### Optimization: caching tokens

In the case where the client is not close to exceeding its limits, we can avoid
the overhead of a cross-core call by caching some per-client tokens on each
shard.

This cache can be pre-primed on the first client connection, if the total
tokens for the client is greater than the core count: that way, subsequent
connections from the same client will usually be accepted without the need for
a cross-core call.

### Handling a configuration change

Changes to the configuration properties affect all outstanding
client_conn_quota objects: the `total` attribute must be recalculated.

If `total` is decreased, then it is also necessary to call out to all other
shards and revoke any cached tokens that are held in excess of the new limit.

## Impact

When this feature is enabled, a per-connection latency overhead is added, as
the shard handling the connection must dispatch a call to the shard that owns
the client IP state.  A cross core call can take up to a few microseconds,
although the impact on P99 latency can be worse if the target core is very busy.

On a system where the connection count limit is significantly larger than the
core count, this overhead will be avoided in most cases, as each core will
store some locally cached tokens to allow them to accept connections.


# Guide-level explanation

## How do we teach this?

* Presume reader knows what a TCP connection is
* Explain how Kafka clients map to TCP connections: that a producer or consumer
  usually opens a connection and uses it over a long period of time, but
  a program that constructs many Kafka clients in parallel might open more
  connections.  The relationship between clients and connections is not 1:1,
  as clients may e.g. be both a producer and a consumer, and the client may
  open additonal connections for reading metadata.
* Explain why it's bad to have an unbounded number of connections from a
  client:
  * Each connection has some memory overhead that might exhaust redpanda memory
  * Each connection counts against OS-level limits (`ulimit`)
  * It's probably not what the user intended for efficient operation, as the
    Kafka protocol generally re-uses a smaller number of connections.
* Introduce the new configuration properties, including the syntax of the
  `overrides` property
  * Mention the caveat that decreasing the limits does not terminate any
    currently open connections.

## Interaction with other features

* Dependency on centralized config for live updates

## Drawbacks

### Latency overhead on first message in a new connection:

Mitigating factors:
* Sensible workloads generally operate on established connections
* No overhead if feature is not enabled.

## Alternatives Considered

### Do nothing

An application may exhaust server resources by opening a very large number of
connections (e.g. by instantiating many Kafka clients).  This can be mitigated
in environments where the application design is tightly controlled and the
server cluster is sized to match, but in real-life environments this is
unlikely to be the case & it is relatively easy for a naively-written application
to open unbounded numbers of Kafka client connections.  For example, consider
a web service that opens a Kafka connection for each request it serves, and
does not have a limit on its own concurrent request count.

### Total connection count, rather than per-client-IP

To protect redpanda from resource exhaustion, it is not necessary to
discriminate between clients: we could just have a single overall connection
count.

This prevents redpanda from having resources exhausted, but does not help
preserve availability of the system: one misbehaving server can crowd out
other clients.

### Shard-local limits

We could avoid some complexity and latency by setting a per-core connection
limit, rather than a total connection limit across all cores.  This would
enable all cores to apply their limits using local state only.

This is reasonable at small core counts, but tends to fall down at larger core
counts: consider a 128 core system, where a per-core limit to tolerate
reasonable applications (e.g. 10 connections) would result in a total limit of
1280, much higher than the user wanted.

### Per-user limits

In many environments, the client IP is not a meaningful discriminator between
workloads -- workloads are more likely to be identifiable by their service
account username.

Strictly speaking, it is not possible to limit connections by username, because
we have to accept a connection and receive some bytes to learn the username, but
it could still be useful to drop connections immediately after authentication
if a user has too many connections already.

Adding per-user limits in future is not ruled out, but the scope of this RFC is
limited to the more basic IP-based limits, to get parity with KIP-308.

### Kernel-space (eBPF/BPF/iptables) enforcement

Where the connection count for a particular source IP has been exhausted, and/or
was set to zero, we could more efficiently drop connections by configuring the
kernel to drop them for us, rather than doing it in userspace.  This kind of
efficiency is important in filtering systems that guard against DDoS situations.

The connection count limits in this RFC are primarily meant to protect against
misbehaving (but benign) applications rather than active attack, so the overhead
of enforcing in userspace is acceptable.  Avoiding kernel mechanisms also helps
with portability (including to older enterprise linux distros) & potential
permissions issues in locked-down environments that might forbid userspace
processes from using some kernel space mechanisms.

All that said, extending our userspace enforcement with lower level mechanisms
in future could be a worthwhile optimization.  In Kubernetes environments,
we would need to consider whether to implement this type of enforcement within
Redpanda pods, or within the Kubernetes network stack where external IPs are
exposed.


