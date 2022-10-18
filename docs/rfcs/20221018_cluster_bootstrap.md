- Feature Name: Cluster Bootstrap Revamp
- Status: implementation in progress
- Start Date: 2022-10-18
- Authors: Andrew Wong
- Issue: #333, #2793


# **RFC: Cluster Bootstrap**

## **Summary**

This document describes upcoming changes to the steps performed by Redpanda when instantiating a cluster, with the intent to simplify configuration of nodes and eliminate the class of incidents caused by the incorrect configuration of node IDs and seed servers. The user-facing effect of this proposal is that the configuration of each node can be made identical with one another -- we will require no special node and no per-node specifics when configuring a cluster.


## **Background**


TL;DR



* We currently require that there is one node with an empty `seed_servers` config, and this heterogeneous configuration requirement causes trouble when not implemented
* A single server (the “root” node) instantiates the cluster as a cluster of size one; all other nodes join the cluster by routing RPCs to the “seed servers”, who eventually route the RPCs to an existing cluster (typically the root node when first bootstrapping); during this period, the cluster is up and running, but is arguably in a surprising state (one node cluster)
* We require manual assignment of node IDs, which is annoying to configure and easy to get wrong, especially in cloud deployments
* It is unpleasant to bootstrap superusers today via environment variables
* There is no way to start a cluster with license

Current implementation of cluster formation:



* **Root:** becomes leader immediately, begins servicing RPCs, processes join requests and runs Raft member add operation when other servers attempt to join.
* **Seeds:** tries to connect to all seed servers to try to join. Also receives requests to join from non-seed servers, and redirects the requests if we’ve joined the cluster and we’re not the leader.
* **Non-seeds:** tries to connect to all seed servers to try to join the cluster.

The above requirements of today’s configuration has led to poor experience for customers by way of incidents as severe as having a controller topic with split brain.


## **Requirements Overview**



* Make it significantly more difficult to misconfigure a cluster in a way that starts multiple clusters or no clusters, especially in the face of a wiped node (or several).
* To that end, simplify configuration by allowing admins to configure the nodes with identical config files (within reason, users likely still want to advertise different addresses per node) — no more root servers, and no more mandatory hard coding of node IDs.
* Until a cluster has been fully started, the nodes of the cluster should perform no action that may depend on the initial cluster state.
    * Calls to the admin server API or Kafka API should return a retriable error code.
    * We should not nag because of a missing license, nor should we perform any action that depends on having a valid license.


## **Design**


### **High level design**

Since there are several issues being addressed in this project, there are a few disparate tasks involved in this overhaul of bootstrapping:



* Drop the requirement that each node configuration must include a node ID. Users may still supply node IDs, but as a part of bootstrapping a cluster, Redpanda will enforce that newly added nodes will have unique node IDs. Nodes will assign themselves a UUID (persisted in their kv-store), and register themselves with the controller leader, who assigns a unique node ID. Reusing a node ID that has already been used will be strictly disallowed.
* Remove the period of cluster formation where the cluster only has a single node. Instead, clusters will start with at least three (configurable) nodes, instantiating a controller Raft group with the seed servers list, and growing the cluster as nodes start up.
* Initial cluster configuration will be done via either environment variables, or bootstrap config file, depending on sensitivity of the configs (e.g. user credentials are secrets, licenses aren’t)
* Optionally gate the starting of a cluster on the run of a new CLI command. Admins can provide initial user credentials and a license to this command. Before the successful completion of this command, no node in the cluster will respond to Kafka requests, admin requests, etc. This waiting is more of a cherry-on-top as a more sure-fire gate to cluster formation accidents – the above three design points get us most of the way there in solving customer pain points, and will be the focus for Q3.


### **Multi-node cluster formation**

Rather than relying on a single node to be the root node, the entire seed servers list will be taken to be the initial Raft group for the controller. Once all the seed servers are up, each seed server should determine whether or not all seed servers have the same list of seed servers. If so, the nodes are primed for starting a cluster and should await instruction to do so. While waiting for start up, we may initialize most subsystems, but must not initialize the controller, a bulk of the admin server, Kafka API, to name a few.

The initialization on seed servers (non-seed servers continue with today’s join-until-success logic), in short:



* **Seeds:** tries to connect to all seed servers to ensure they all think the list of seed servers, and other cluster configs, is identical. Once this is verified, and no cluster UUID already exists, awaits user input to start its Raft instances with the seed servers list as the initial Raft group.
* **Non-seeds:** tries to connect to all seed servers to try to join, sending a UUID along with a node ID. No strong requirements on the seed servers list here, other than pointing to a member of the cluster.
* **Seeds after cluster formation:** as indicated by the presence of a non-empty controller log or a cluster UUID, follows the above non-seeds logic.

The initial seed server check will be done via a new RPC endpoint with the following request and response structure:


```
struct initial_cluster_info_request {
    // Empty.
};

struct initial_cluster_info_reply {
    // A cluster UUID, if one already exists, indicating we should join.
    string cluster_uuid;

    // Manually assigned node ID, if any, so we can form our initial mapping of UUIDs.
    int node_id;

    // UUID of the destination node, so we can form our initial mapping of UUIDs.
    string node_uuid;

    // Seeds from the destination server. Maybe useful to log if there's a problem.
    // Useful in detecting the empty_seed_servers_forms_cluster=True case.
    vector<string> seed_servers;
};
```


The subsystem that serves these RPCs must initialize before the controller forms its initial Raft group. As such, we will instantiate an RPC protocol on the seed servers only that accepts the `initial_cluster_info` calls. Once the controller has initialized, we will add the normal RPC protocol for normal runtime.

Alternate approaches to initial controller group sizing



* Start with size three; taking the first three nodes in the seed server list to be the founding controller Raft group, ensuring identical configs across each config.
    * **Downside:** will likely take longer to run through several config changes than initialize a large Raft group.
    * **Downside:** having a policy on how to pick three nodes seems fragile and difficult to evolve.
    * **Downside:** unintuitive and surprising to have only a subset of the seed servers participate in bootstrapping, and leaves room for error and races when considering nodes starting up at different rates.
* Allow waiting for a specific cluster size before starting the controller group, rather than inferring based on seed servers list. Initialize the cluster only once the expected size is reached, e.g. propagating a node list via gossip, and only starting a Raft group of that size when prompted by the init command after the gossip state indicates the cluster has the nodes expected.
    * **Upside:** some kind of gossip mechanism can be used as a foundational piece for other interesting features
    * **Downside:** added complexity, maintenance burden, support burden, of yet another distributed protocol subsystem
    * **Side note:** the proposal here can be a piece to work towards a gossip-based initialization. Such an implementation would wait for the gossip subsystem to have all nodes agree on a founding set of nodes, as well as an initial assignment of node IDs for them, after which, cluster initialization can proceed.

Alternate approaches to controller group initialization



* Rather than waiting for an explicit init command, unconditionally proceed once we know all seed servers are healthy and configured uniformly.
    * **Upside:** deployment is easier to automate because there is no special step to run init.
    * **Downside:** we no longer have an explicit step with which to provide initial cluster-wide configs.
    * **Downside:** this is still error prone in the case of all seed servers being wiped and restarted, e.g. the seed servers are haphazardly placed in the same region and there is a region failure. While more difficult, it’s still possible to restart some nodes empty and start up with two clusters (compared to the current proposal, if the seeds start empty, they would still need to await the init command, rather than start a new cluster).
* Have the default for `wait_for_init` to be false
    * **Downside:** as a default, this would be unsafe for the reasons stated above.

Alternate approaches to `initial_cluster_info` endpoint



* Use an entirely separate server bound to a different port
    * **Upside:** likely better tested approach
    * **Downside:** may require additional configuration
* Use configs for the RPC server, but once we’re done, stop the initial_cluster_info server, and then start a new server with the runtime protocol
    * **Downside:** there may be a brief blip where no RPC server is unavailable
    * **Downside:** unclear whether this approach is any better tested


### **Single-node cluster formation**

It doesn’t seem unreasonable that some clusters knowingly start out with cluster size 1, and then grow as use cases grow more mature on the cluster. Additionally, the existing user experience for developer clusters relies heavily on being able to not provide a seed server list.

To that end, we will continue to support supplying an empty seed servers list or a seed server list of size one, which will be construed to mean starting a cluster with size 1 node.

Alternatives



* Don’t support single node clusters period.
    * **Downsides:** use case doesn’t sound unreasonable, especially for folks just testing out a binary. A goal of this project is to make trying out Redpanda easy and failsafe, and single-node/local clusters seem like an important part of that experience.
* Force users to specify when they want a single node cluster by setting some `minimum_initial_cluster_size` configuration


### **Controller log initialization**

A new controller log command will be added to indicate the initial desired state of the cluster, including initial admin secrets, license, etc. This will be a new controller command with roughly the following payload:


```
struct cluster_init_cmd_data {
    string cluster_uuid;
    vector<tuple<uuid, node_id, addr>> seed_servers;
    vector<pair<credential_user, scram_credential>> user_creds;
    string license;
};
```


Upon electing the leader of the controller group, the leader should determine whether it has this message by checking its kv-store for a cluster UUID. If not, it will replicate a new message. The contents of the message must be known before controller group formation.

Only once this entry is replicated and applied to a node will that node open up its admin server, Kafka ports, etc. and continue with bootstrapping. The apply of this message must also affect the corresponding state in the members table, feature table, and security manager.

It should be noted that clusters today already initialize a cluster UUID as a part of the metrics subsystem. This old cluster UUID is used for phone home.

Alternatives



* Rather than a single message, rely on existing RPCs and send multiple messages on the controller log.
    * **Downsides:** seems unrealistic to be able to do this robustly; messages would not be atomic and would thus be fragile against lag, restarts, etc.
    * **Upsides:** reuse existing services.


### **Node ID assignment**

Upon starting up, if there is not already a UUID in the local kv-store, initialize a UUID and persist it in the local key-value. This will be the node’s UUID.

The `join_node` request messages already include an unused `node_uuid` field and an existing `node_id` field (moving forward, just a hint). The response `node_id` field will indicate what `node_id` has been assigned.

Attempts to join a cluster with the same `node_uuid` after having already joined that cluster with that UUID should fail. This would indicate a bug wherein a node that has data attempts to join a cluster twice.

Attempts to join a cluster with an in-use `node_id` should fail, if the UUID in the request differs from the one persisted in the members table.

Auto-generated `node_ids` will be monotonically increasing for now, even if decommissioning and recommissioning a node — such a node will be assigned a new `node_uuid` and a new `node_id`.

Today, nodes only send join requests after it has started up its RPC server and is ready to begin accepting Raft requests for its node ID. In the future, when nodes start up without a node ID, we need to uphold this ordering to avoid some extraneous delays in bootstrapping. To that end, assignment of node ID and joining a cluster will be two separate operations. The controller provides a means to perform operations serially and in a deterministic order, so it seems like an ideal place to assign node IDs.

When starting up with no node ID, it will send a join request to the seed servers to replicate a controller command that registers the input UUID with an unused node ID. Since the ordering of controller apply operations is deterministic, the IDs assigned are deterministic as well, and can be inferred by the order in the log with a simple counter.

Initial node IDs for the seed servers are taken to be the index in the seed servers list.

Alternatives to node ID assignment mechanism



* Don’t infer node IDs based on order, and instead use some kind of leadership-based lock to assign a node ID before replicating to followers. This is similar to how the ID allocator works.
    * **Upside:** easier to debug from just the controller log
    * **Downside:** more complicated implementation, and allows for gaps in the node ID space (e.g. in case leadership is lost and then gained again on the same node while registering a UUID)

Alternatives to initial node ID assignment.



* Initial node IDs are taken as input to the init command, like as a map from {`seed_server_addr`=>`seed_server_node_id`}.
    * **Downside:** not much benefit here, if an admin wanted to explicitly set node ID, they should do that in the node configs

Alternatives to mechanics of node ID assignment before join



* Extend Raft config change with node UUIDs
    * **Downside:** hard to uphold the invariant that we join only once we’re ready to receive Raft RPCs
    * **Downside:** likely results in more complex code, since we would be overloading the purpose of Raft config change
    * **Downside:** only the controller group would need to care about UUIDs – all other Raft groups can continue using node IDs, so making a fundamental change to the underlying Raft implementation seems like overkill
* Add an additional field to `configuration_update` that indicates new UUIDs
    * **Downside:** seems redundant with `replicas_to_add` field
    * **Upside:** no changes to foundational Raft data types

**Superuser configuration**

Continue using environment variables, and eventually implement as a part of an initialize command.

Alternatives



* Supply as a node config
    * **Downside:** this cannot be the only solution to this problem because not all deployment modes have dedicated secret storage (k8s does, on-prem probably doesn’t).
    * **Downside:** muddies the distinction between how we initialize node configs (i.e. through yaml), and how we set cluster configs (i.e. through centralized configs in the controller). Maybe this isn’t so bad.
    * **Downside:** potential to misconfigure, since these would be set on each node, and could end up being set incorrectly.


### **License configuration**

NOTE: as of v22.3, this is not yet implemented.

Supply as a part of the bootstrap config YAML, and eventually implement as a part of an initialize command.

Alternatives



* Supply via init command
* Supply as an environment variable
    * **Downside:** not a great experience for on-prem
* Supply as a node config
    * **Downside:** muddies the distinction between how we initialize node configs (i.e. through yaml), and how we set cluster configs (i.e. through centralized configs in the controller). Maybe this isn’t so bad.
    * **Downside:** potential to misconfigure, since these would be set on each node, and could end up being set incorrectly.
* Supply as an argument to the Redpanda binary
    * **Upside:** it’s a step towards not requiring two-step bootstrap altogether
    * **Downside:** needs to be included on every node


### **What should a single-node cluster look like?**

Single node clusters will not be a special case. Everything else about the cluster will still reflect the changes laid out in this RFC. Cluster UUIDs will still be assigned, node UUIDs will still be generated, joining nodes will be automatically assigned node IDs, etc.

Alternatives



* Have the behavior be identical to how it was before
    * **Downside:** would incur lots of legacy code that is difficult to wade through
    * **Downside:** knowingly unsafe
* Add a configuration `empty_seed_servers_starts_cluster` that means seed servers should attempt to join if receiving `initial_cluster_info` response with empty `seed_servers`
    * **Upside:** allows users to not change their configs
    * **Downside:** knowingly unsafe


### **Init command**

NOTE: as of v22.3, this is not yet implemented.

Only once the rest of this project is done will we consider implementing an init command.

The `rpk init` tool will trigger the request to start the cluster, and will return only after the cluster has formed successfully, returning its cluster UUID.

The tool will take as input just a single address, potentially localhost. If the node is a seed server and is awaiting initialization, it will run and forward to all other seed servers. If the node is not a seed server, it will just forward the request via RPC to each seed server. If a cluster has already been formed (as indicated by a persisted cluster UUID), the server will return immediately with a cluster UUID.

The tool will periodically retry the request until a cluster UUID is returned, at which point it will exit.

Alternatives



* No init command, and instead provide all configs (except initial user credentials) as arguments to `rpk cluster start`
    * **Upside:** significantly easier to deploy in an automated way (no need to have a one-off command that runs once)
    * **Downside:** vulnerable in the case where several nodes go down and come up completely empty (i.e. all seed servers are in a single zone, a zone failure wiped out data in that zone, all seed servers then try to form a single new cluster).


### **Security implications of init command**

NOTE: as of v22.3, this is not yet implemented.

Either run from localhost, or run with a superuser defined in the bootstrap YAML and `RP_BOOTSTRAP_USER`.


### **Automated deployment considerations**

Since we are still going to support the existing pattern of configuration, we won’t have to immediately update any k8s operator, ansible, etc. deployments. Some already have mechanisms to avoid starting up with an empty seed servers list after initial bootstrap.

Moving forward, at the very least, we should remove the generation of node ID from these deployments.

Longer term, we should be very intentional about the seed servers list, e.g. at least one seed in each AZ, to avoid creating a new cluster when an AZ goes down.


### **Reconciling existing cluster UUID with new cluster UUID**

NOTE: as of v22.3, this is not yet implemented.

There already exists a cluster UUID that is inferred by every node, as it is based on the controller log. We hash the first controller messages and timestamps, and use that as the seed for the cluster UUID, deterministically ensuring it’s identical on every node. This gets persisted in the config manager.

Moving forward, the metrics reporter should check to see if a cluster ID exists in the config, and pull it directly from the shard-local config if so. If not, it should use the cluster UUID.

