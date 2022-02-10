- Feature Name: Ability to limit connection creation rate per shard
- Status: in-progress
- Start Date: 2022-02-09
- Authors: Vadim Plakhtinskiy
- Issue: 

# Executive Summary

- Set a limit on the rate with which the shard accepts new connections
- Set a limit on connection creation rate per IP


Similar to KIP-612, adapted to Redpanda's thread per core model

## What is being proposed

New configuration properties (For start we choose global config for connection rate):
- kafka_max_connection_creation_rate (optional integer, default unset)
- kafka_max_connection_creation_rate_ovverides (string, default "")


When a connection is accepted which exceeds one of the configured
bounds, it is blocking before reading or writing anything to the socket.


## Motivation

* Avoid hitting system resource limits (for example CPU)
* Avoid geting connections storms
* Allow broker doing useful work instead of handling high connection creation rate

## How

If both configuration properties are unset, then no action is taken: this
preserves the existing unlimited behavior.
 
We will track connection rate on each shard independently (without cross-core communication).
 
For limit connections rate we will use a token bucket algo. Each bucket is semaphore for counting connections in the current second. For overrides we will use a hash table from ip -> semaphore.
 
struct connection_rate {
   ss::semaphore current_rate
   ss::lower_clock last_update_time
}
 
For new connection we will do:
1. check ss::lowres_clock::now() and last_update_time, if second was changed we should signal to increase tokens to kafka_max_connection_creation_rate
2. run wait(timeout, 1) for semaphore

### How to add new tokens to semaphore
Problem: what if we have the maximum number of connections and should block new connections
waiting for the next second, who will signal them to unblock?
We should avoid this situation, and will spawn background fiber if available_units is 0 after lock.
Background fiber will signal(kafka_max_connection_creation_rate) on next second after spawn if needed (if zero available tokens)
 
We can run background fiber, which will add kafka_max_connection_creation_rate tokens to semaphore on each second.
 
Note: each signal(kafka_max_connection_creation_rate) also should update last_update_time

### Integration with current network code
We have net::server for different type tcp connection (rpc_kafka, internal_rpc, etc)
 
Idea is adding new settings to server_configuration (settings for connection rate).
On server::start it will init internal structure with connection_rate info,
 
In server::accept before
 
ssx::spawn_with_gate(_conn_gate, [this, conn]() mutable {
  return apply_proto(_proto.get(), resources(this, conn));
});
            
We will check the current connection rate.
 
If we should spawn background fiber (to increase tokens in semaphore) we will spawn it before
 
ssx::spawn_with_gate(_conn_gate, [this, conn]() mutable {
  return apply_proto(_proto.get(), resources(this, conn));
});

### Handling a configuration change

Changes to the configuration properties affect all outstanding
connection_rate objects: the `kafka_max_connection_creation_rate` attribute must be recalculated.

### Metrics
We should reprot about current rate on each shard

## Impact

1. Each connection will store start connection time
2. Use clock to comapre times, but we will use ss::lower_clock
3. Background fiber to update rate limit in semaphore

# Guide-level explanation

## How do we teach this?
* Presume reader knows what a TCP connection and connections rate is
* Explain why it's bad to have an unbounded number of connections rate
  * Each connection has some memory overhead that might exhaust redpanda memory
  * Connections storm can increase CPU and can create problem with CPU and performance problems
* Introduce the new configuration properties, including the syntax of the
 `overrides` property
  * Mention the caveat that decreasing the limits does not terminate or stopping any
   currently open connections.


## Interaction with other features

* Dependency on centralized config for live updates

# Drawbacks

The lowest effective rate the user can set is multiplied by the number of shards.
So user should think about it

## Alternatives Considered

### Do nothing

A client may exhaust server resources by opening a very large number of
connections in one second. This is okay as long as all clients are well-behaved and the
system is carefully sized for its workload, neither of which are reliably true
in real life deployments.

### Run backround fiber
We can run background fiber each second to refresh tokens count in semaphore.
In thins case we will avoid getting and comparing time for each connection,
and only fiber will run signal()

### Node limits (not shard)

It will add more compexity to share info about current rate to each core.
Also we should sharding info about ovverides and map each ip to core for saving
info about rate per ip
