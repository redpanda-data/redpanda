## Features

### Transparent tiered storage

* [#2818](https://github.com/redpanda-data/redpanda/issues/2818) Add more detailed shadow indexing metrics. by @ztlpn in https://github.com/redpanda-data/redpanda/pull/3881
* [#3351](https://github.com/redpanda-data/redpanda/issues/3351) Fix raft bootstrap failure after topic recovery. by @Lazin in https://github.com/redpanda-data/redpanda/pull/3195
* [#3507](https://github.com/redpanda-data/redpanda/issues/3507) Fix a rare crash that could happen when doing a shadow indexing fetch concurrently with topic deletion. by @ztlpn in https://github.com/redpanda-data/redpanda/pull/3813
* [#4036](https://github.com/redpanda-data/redpanda/issues/4036) Improve rack-aware replica placement. by @Lazin in https://github.com/redpanda-data/redpanda/pull/4142

### Centralized configuration

* Added support for centralized configuration in the operator. by @nicolaferraro in https://github.com/redpanda-data/redpanda/pull/3978
* operator: added drift detection controller to prevent external change of properties set in the CR. by @nicolaferraro in https://github.com/redpanda-data/redpanda/pull/4140

### Maintenance mode

* Adds rpk commands for enabling and disabling node maintenance mode. by @dotnwat in https://github.com/redpanda-data/redpanda/pull/4255
* [#3705](https://github.com/redpanda-data/redpanda/issues/3705) [#3706](https://github.com/redpanda-data/redpanda/issues/3706) Support for placing node into a draining state in which all leadership is relinquished. by @dotnwat in https://github.com/redpanda-data/redpanda/pull/3932
* [#4435](https://github.com/redpanda-data/redpanda/issues/4435) rpk: add cluster maintenance status and enable barrier. by @dotnwat in https://github.com/redpanda-data/redpanda/pull/4480
* Report maintenance mode status for brokers. by @dotnwat in https://github.com/redpanda-data/redpanda/pull/4319
* [#4527](https://github.com/redpanda-data/redpanda/issues/4527) Added rpk cluster health command. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/4528

### Rack awareness

* [#3695](https://github.com/redpanda-data/redpanda/issues/3695) Implement rack-aware replica assignment. Add `enable_rack_awareness` parameters that enables the feature. by @Lazin in https://github.com/redpanda-data/redpanda/pull/4025

### Consumer offsets

* Kafka compatible consumer offset topic metadata. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/3902
* [#3701](https://github.com/redpanda-data/redpanda/issues/3701) Support for `__consumer_offsets` topic. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/3981
* [#4051](https://github.com/redpanda-data/redpanda/issues/4051) [#4179](https://github.com/redpanda-data/redpanda/issues/4179) Fixed consumer offset migration issue that might lead to situation in which migration would not continue. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/4095

### Idempotency

* [#3552](https://github.com/redpanda-data/redpanda/issues/3552) Update idempotency to support compacted topics. by @rystsov in https://github.com/redpanda-data/redpanda/pull/3654
* [#3826](https://github.com/redpanda-data/redpanda/issues/3826) Make idempotency compatible with Sarama. by @rystsov in https://github.com/redpanda-data/redpanda/pull/3824
* [#3827](https://github.com/redpanda-data/redpanda/issues/3827) Enable idempotency by default. by @rystsov in https://github.com/redpanda-data/redpanda/pull/4199

### Other

* [#1275](https://github.com/redpanda-data/redpanda/issues/1275) Kafka server send group topic partition offset metric to prometheus. by @ZeDRoman in https://github.com/redpanda-data/redpanda/pull/3181
* [#2166](https://github.com/redpanda-data/redpanda/issues/2166) Three new metrics have been added: `vectorized_storage_disk_total_bytes`, `vectorized_storage_disk_free_bytes`, `vectorized_storage_disk_free_space_alert`. More direct monitoring of Redpanda's disk usage and free space. A simple alert metric field `vectorized_storage_free_space_alert` which is non-zero when space is running low. Continuing improvements around full disk handling. by @ajfabbri in https://github.com/redpanda-data/redpanda/pull/3885
* [#2707](https://github.com/redpanda-data/redpanda/issues/2707) Implemented simple metrics reporter. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/3066
* [#2876](https://github.com/redpanda-data/redpanda/issues/2876) The minimum disk space allocation size is now configurable via the `segment_fallocation_step` property. The default is unchanged from the previous behavior (32MB). You may wish to decrease this property if creating large numbers of partitions on systems with limited disk space. by @ZeDRoman in https://github.com/redpanda-data/redpanda/pull/3288
* [#2987](https://github.com/redpanda-data/redpanda/issues/2987) Add get transaction request to `admin_api`. by @VadimPlh in https://github.com/redpanda-data/redpanda/pull/3145
* [#3274](https://github.com/redpanda-data/redpanda/issues/3274) [#3337](https://github.com/redpanda-data/redpanda/issues/3337) Creating topics with larger partition counts is made safer, by validating that the cluster has sufficient resources to fulfill the request before trying to create partitions. New checks for sufficient RAM and sufficient file handles can be overridden if necessary with the new `topic_memory_per_partition` and `topic_fds_per_partition` settings respectively. The defaults are to require 1MB of RAM and 10 open file handles per partition. This functionality helps to avoid situations where a redpanda cluster can become unstable when the partitions created outstrip available system resources. by @jcsp in https://github.com/redpanda-data/redpanda/pull/3398
* [#3412](https://github.com/redpanda-data/redpanda/issues/3412) Memory utilization on systems with large number of partitions can now be tweaked using configuration properties storage_read_buffer_size (default 128KiB) and storage_read_readahead_count (default 10). These properties may be decreased to more conservative values such as buffer_size=16KiB, readahead_count=1 to reduce the per-partition memory overhead and improve stability when the number of partitions is large (e.g. more than 10000). by @jcsp in https://github.com/redpanda-data/redpanda/pull/3421
* [#3544](https://github.com/redpanda-data/redpanda/issues/3544) Implement `get_transaction` request for admin api. It returns information about all transactions. by @VadimPlh in https://github.com/redpanda-data/redpanda/pull/3659
* [#3544](https://github.com/redpanda-data/redpanda/issues/3544) [#3699](https://github.com/redpanda-data/redpanda/issues/3699) Add delete partition from transaction request. by @VadimPlh in https://github.com/redpanda-data/redpanda/pull/3661
* [#3688](https://github.com/redpanda-data/redpanda/issues/3688) Added new redpanda options: `rpc_server_connection_rate_limit` - Maximum connections per second for one core; `rpc_server_connection_rate_limit_overrides` - Overrides for specific ips for maximum connections per second for one core. (It should be array of strings like `['127.0.0.1:90', '50.20.1.1:40']`). by @VadimPlh in https://github.com/redpanda-data/redpanda/pull/3922
* [#3689](https://github.com/redpanda-data/redpanda/issues/3689) Added full support for leader epoch in Redpanda. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/3788
* [#3698](https://github.com/redpanda-data/redpanda/issues/3698) It is now possible to impose limits on the number of client connections. Cluster configuration properties `kafka_connections_max` and `kafka_connections_max_per_ip` are added to control this behavior. By default both properties are switched off (i.e. the connection count is unlimited). These limits apply on a per-node basis. Note that the number of connections is not exactly equal to the number of clients: clients typically open 2-3 connections each. by @jcsp in https://github.com/redpanda-data/redpanda/pull/3901
* [#3704](https://github.com/redpanda-data/redpanda/issues/3704) Redpanda upgrades are made more robust by tracking all node versions, such that new features can wait until all nodes are up to date before activating. by @jcsp in https://github.com/redpanda-data/redpanda/pull/2938
* [#3704](https://github.com/redpanda-data/redpanda/issues/3704) The `v1/features` admin API endpoint is added, which can be used by automation scripts to query an internal logical cluster version, and feature flags for newly added functionality. by @jcsp in https://github.com/redpanda-data/redpanda/pull/2938
* [#3751](https://github.com/redpanda-data/redpanda/pull/3751) The Redpanda Admin REST API now includes optional username/password authentication. This is in addition to the existing mTLS option, which remains available. To ensure backward compatibility for existing systems, username/password authentication is disabled by default, and may be enabled using the `admin_api_require_auth` cluster configuration property. The `enable_admin_api` configuration property is deprecated and will be ignored. The Admin API is now necessary for making subsequent Redpanda configuration changes, and security conscious users have the option to enable TLS&password authentication. by @jcsp in https://github.com/redpanda-data/redpanda/pull/3819
* [#3790](https://github.com/redpanda-data/redpanda/issues/3790) Introduces an optional field `preferredAddressType` to the cluster CRD. Allows to specify the preferred node address type to advertise for Kafka API when the subdomain is empty. by @dimitriscruz in https://github.com/redpanda-data/redpanda/pull/3794
* [#3892](https://github.com/redpanda-data/redpanda/issues/3892) Adds an option for configuring a bootstrapping load-balancer targeting the external Kafka API listener. by @dimitriscruz in https://github.com/redpanda-data/redpanda/pull/3896
* [#3991](https://github.com/redpanda-data/redpanda/issues/3991) Added set of metrics to increase visibility of partition movement process. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/4014
* [#4073](https://github.com/redpanda-data/redpanda/issues/4073) Improves `rpk topic consume -o`, allowing consuming by time, and allows consuming to exit when the ends of partitions are reached. by @twmb in https://github.com/redpanda-data/redpanda/pull/4091
* [#4146](https://github.com/redpanda-data/redpanda/issues/4146) Schema Registry: Support references for Avro schema. by @BenPope in https://github.com/redpanda-data/redpanda/pull/4154
* [#4301](https://github.com/redpanda-data/redpanda/issues/4301) Add `/v1/debug/reset_leaders` request to reset leaders info on node. by @VadimPlh in https://github.com/redpanda-data/redpanda/pull/4237
* [#4301](https://github.com/redpanda-data/redpanda/issues/4301) New admin api request `/v1/debug/partition_leaders_table`. by @VadimPlh in https://github.com/redpanda-data/redpanda/pull/4259
* [#4333](https://github.com/redpanda-data/redpanda/pull/4333) rpk: add broker version info in `redpanda admin broker ls`. by @r-vasquez in https://github.com/redpanda-data/redpanda/pull/4409
* [#4402](https://github.com/redpanda-data/redpanda/issues/4402) operator: added hooks for rolling restarts and upgrades. by @nicolaferraro in https://github.com/redpanda-data/redpanda/pull/4385
* operator: Allow Redpanda resources to be directly configured. by @BenPope in https://github.com/redpanda-data/redpanda/pull/3773
* schema_registry: Support `GET /subjects/{subject}/versions/{version}/referencedBy`. by @BenPope in https://github.com/redpanda-data/redpanda/pull/3299
* Add cluster level configuration parameters `cloud_storage_enable_remote_read` and `cloud_storage_enable_remote_write` that can be used to enable shadow indexing for all topics. by @Lazin in https://github.com/redpanda-data/redpanda/pull/3233
* Add settings for `max_old_gen_size`. by @VadimPlh in https://github.com/redpanda-data/redpanda/pull/3167
* Admin API requests sent to non-leader nodes are redirected more reliably to the leader. by @jcsp in https://github.com/redpanda-data/redpanda/pull/3571
* Cluster status stanza will have version field for convenience. by @pvsune in https://github.com/redpanda-data/redpanda/pull/4207
* Configuration property `kafka_connections_max_overrides` is added, enabling setting connection count limits on individual client IPs. by @jcsp in https://github.com/redpanda-data/redpanda/pull/4221
* Kafka: return shadow indexing configs in a topic describe handler. by @LenaAn in https://github.com/redpanda-data/redpanda/pull/3192
* Kubernetes operator: All Redpanda nodes created through the Kubernetes operator will have a default cloud storage maximum upload interval (`cloud_storage_segment_max_upload_interval_sec`) of 30 minutes. by @0x5d in https://github.com/redpanda-data/redpanda/pull/3218
* Made `min_free_memory` parameter of background reclaimer configurable. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/3410
* New admin API endpoint `PUT /v1/features/<feature>`, for use on future feature flags which may not be enabled by default. by @jcsp in https://github.com/redpanda-data/redpanda/pull/3936
* Redpanda now has an internal store for configuration properties and many properties can be set without restarting redpanda. When you upgrade Redpanda, your existing configuration will be imported automatically. Please consult the documentation for more detail on this feature. New rpk subcommands for handling cluster configuration: `rpk cluster config [edit|import|export|set|get|status|force-reset|lint]`. by @jcsp in https://github.com/redpanda-data/redpanda/pull/3760
* Returning partition leader_epoch in record batches. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/4030
* Several numeric configuration properties have improved bounds checking, preventing cluster instability resulting from invalid property values. by @jcsp in https://github.com/redpanda-data/redpanda/pull/3637

## Bug Fixes

* [#4423](https://github.com/redpanda-data/redpanda/issues/4423) Fixes assertion in rpc after parsing failure. by @dotnwat in https://github.com/redpanda-data/redpanda/pull/4355
* [#1892](https://github.com/redpanda-data/redpanda/issues/1892) [#3866](https://github.com/redpanda-data/redpanda/issues/3866) [#3884](https://github.com/redpanda-data/redpanda/issues/3884) [#4045](https://github.com/redpanda-data/redpanda/issues/4045) Fixes for node operations fuzzy test. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/4089
* [#2246](https://github.com/redpanda-data/redpanda/issues/2246) Fixes a corner case that might happen when partition was re-balanced. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/3242
* [#2246](https://github.com/redpanda-data/redpanda/issues/2246) Fixes for node operations fuzzy tests. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/2737
* [#2397](https://github.com/redpanda-data/redpanda/issues/2397) `list_offsets` by time now ignores control batches. by @BenPope in https://github.com/redpanda-data/redpanda/pull/4246
* [#2619](https://github.com/redpanda-data/redpanda/issues/2619) k8s: After cluster config is changed in CR, the redpanda pods will get restarted for the config to get used. by @alenkacz in https://github.com/redpanda-data/redpanda/pull/3262
* [#2911](https://github.com/redpanda-data/redpanda/issues/2911) Various error-level log messages are fixed. by @jcsp in https://github.com/redpanda-data/redpanda/pull/3543
* [#2987](https://github.com/redpanda-data/redpanda/issues/2987) Add mark tx expired request to `admin_api` by @VadimPlh in https://github.com/redpanda-data/redpanda/pull/3460
* [#2989](https://github.com/redpanda-data/redpanda/issues/2989) Fix rare data corruption when compacting multiple segments. by @ztlpn in https://github.com/redpanda-data/redpanda/pull/3636
* [#3030](https://github.com/redpanda-data/redpanda/issues/3030) When adding new nodes to the cluster, the added nodes will no longer attempt to serve Kafka requests before they are up to date with the cluster metadata. This avoids a case where clients might experience transient timeouts if accessing the cluster during node joins. by @jcsp in https://github.com/redpanda-data/redpanda/pull/3716
* [#3079](https://github.com/redpanda-data/redpanda/issues/3079) [#3382](https://github.com/redpanda-data/redpanda/issues/3382) Fixed incorrect return error code for topic deletion when topic does not exist. by @dotnwat in https://github.com/redpanda-data/redpanda/pull/3413
* [#3098](https://github.com/redpanda-data/redpanda/issues/3098) Health monitor abortable refresh. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/3132
* [#3211](https://github.com/redpanda-data/redpanda/issues/3211) Schema Registry: Fix a crash during compatibility checks. by @BenPope in https://github.com/redpanda-data/redpanda/pull/3214
* [#3235](https://github.com/redpanda-data/redpanda/issues/3235) Logs at debug level for authorization failures that are expected (e.g. when performing authorization to establish client visibility rather than authorizing for a specific resource requested by a client). Logs the authenticated principal when authorization fails. by @dotnwat in https://github.com/redpanda-data/redpanda/pull/3234
* [#3263](https://github.com/redpanda-data/redpanda/issues/3263) k8s: fixed bug with nodeport handling for clusters that have external connectivity with fixed port. by @alenkacz in https://github.com/redpanda-data/redpanda/pull/3268
* [#3277](https://github.com/redpanda-data/redpanda/issues/3277) [#3328](https://github.com/redpanda-data/redpanda/issues/3328) Fixes for node decommissioning in face of failures. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/3355
* [#3301](https://github.com/redpanda-data/redpanda/issues/3301) k8s: fixed a bug with attempting to patch PDB selector when not required. by @alenkacz in https://github.com/redpanda-data/redpanda/pull/3303
* [#3323](https://github.com/redpanda-data/redpanda/issues/3323) Fix rare crash that could happen when log segments eviction happened concurrently with fetching near the start of the log. by @ztlpn in https://github.com/redpanda-data/redpanda/pull/3372
* [#3336](https://github.com/redpanda-data/redpanda/issues/3336) Fixed assertition when handling fetch requests. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/4271
* [#3360](https://github.com/redpanda-data/redpanda/issues/3360) Fix the deadlock that can be triggered by uploading the manifest right before a shutdown. by @Lazin in https://github.com/redpanda-data/redpanda/pull/3769
* [#3383](https://github.com/redpanda-data/redpanda/issues/3383) k8s: fixed bug in external port binding when port is specified in configuration. by @alenkacz in https://github.com/redpanda-data/redpanda/pull/3385
* [#3400](https://github.com/redpanda-data/redpanda/issues/3400) Move offset translator state. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/3433
* [#3407](https://github.com/redpanda-data/redpanda/issues/3407) wasm: Fix bug for incorrect calculation of record batch size. by @graphcareful in https://github.com/redpanda-data/redpanda/pull/3411
* [#3409](https://github.com/redpanda-data/redpanda/issues/3409) Consuming from a very large number of partitions at once is now subject to a per-request size limit, reducing the risk of exhausting memory if a client specifies a partition count and per-partition size limit that is greater than available RAM. The size limit per-fetch is set via the `kafka_max_bytes_per_fetch` configuration property, default 64MiB. by @jcsp in https://github.com/redpanda-data/redpanda/pull/3420
* [#3428](https://github.com/redpanda-data/redpanda/issues/3428) Improved stability when doing large kafka fetch requests under low memory conditions. by @jcsp in https://github.com/redpanda-data/redpanda/pull/3435
* [#3432](https://github.com/redpanda-data/redpanda/issues/3432) Fixed listing brokers when some of the nodes are down. Fixed describing groups when some of the nodes are down. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/3422
* [#3474](https://github.com/redpanda-data/redpanda/issues/3474) cluster: Handle `gate_closed_exception` in `handle_leadership_notification`. by @VadimPlh in https://github.com/redpanda-data/redpanda/pull/3488
* [#3476](https://github.com/redpanda-data/redpanda/issues/3476) [#3880](https://github.com/redpanda-data/redpanda/issues/3880) Stopping consumer group pending operations. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/4048
* [#3486](https://github.com/redpanda-data/redpanda/issues/3486) Fix an issue where nodes may have stale leadership metadata for a short period after a node restarts.  @jcsp in https://github.com/redpanda-data/redpanda/pull/3487
* [#3494](https://github.com/redpanda-data/redpanda/issues/3494) k8s: fix how schema reg node cert is mounted. by @simon0191 in https://github.com/redpanda-data/redpanda/pull/3496
* [#3528](https://github.com/redpanda-data/redpanda/issues/3528) Fixed possible deadlock of raft groups. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/3537
* [#3539](https://github.com/redpanda-data/redpanda/issues/3539) Fix for possible segmentation fault that might happen when moving group underling partition. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/3553
* [#3559](https://github.com/redpanda-data/redpanda/issues/3559) Schema Registry: Fix a crash when publishing multiple protobuf schema. by @BenPope in https://github.com/redpanda-data/redpanda/pull/3596
* [#3562](https://github.com/redpanda-data/redpanda/issues/3562) kafka: use descriptive error type for auth fails. by @NyaliaLui in https://github.com/redpanda-data/redpanda/pull/3536
* [#3581](https://github.com/redpanda-data/redpanda/issues/3581) [#3583](https://github.com/redpanda-data/redpanda/issues/3583) Pandaproxy: Creating a consumer with an existing name now fails with `{"error_code": 40902}`. Pandaproxy: An inactive consumer is now timed out after `pandaproxy.consumer_instance_timeout_ms`. by @BenPope in https://github.com/redpanda-data/redpanda/pull/3584
* [#3588](https://github.com/redpanda-data/redpanda/issues/3588) Improved handling of configurations where `advertised_kafka_api` or `kafka_api` property has different names between nodes, for example during a configuration change & rolling restart. by @jcsp in https://github.com/redpanda-data/redpanda/pull/3589
* [#3615](https://github.com/redpanda-data/redpanda/issues/3615) RPK commands that use the Redpanda admin API are more robust when a node is offline or a leadership transfer is in process. by @jcsp in https://github.com/redpanda-data/redpanda/pull/3635
* [#3633](https://github.com/redpanda-data/redpanda/issues/3633) Schema Registry: Support protobuf encoded protobuf schema for compatibility with Protobuf Serializers. by @BenPope in https://github.com/redpanda-data/redpanda/pull/3663
* [#3639](https://github.com/redpanda-data/redpanda/issues/3639) Kubernetes operator: the webhook now guards against decrease of the number of assigned cores. by @nicolaferraro in https://github.com/redpanda-data/redpanda/pull/3640
* [#3644](https://github.com/redpanda-data/redpanda/issues/3644) Checks for node CPU count decreases are more robust, to guard against partition unavailability resulting from incorrectly decreasing the CPU count of an existing redpanda node. by @jcsp in https://github.com/redpanda-data/redpanda/pull/3645
* [#3720](https://github.com/redpanda-data/redpanda/issues/3720) Metrics reporter will work with TLS secured endpoints requiring SNI extension. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/3721
* [#3772](https://github.com/redpanda-data/redpanda/issues/3772) rpk: add missing loggers. by @daisukebe in https://github.com/redpanda-data/redpanda/pull/3950
* [#4045](https://github.com/redpanda-data/redpanda/issues/4045) Fixed cleaning up consumer groups state. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/4192
* [#4071](https://github.com/redpanda-data/redpanda/issues/4071) Fix stolen heartbits during big timeout for reconnect. by @VadimPlh in https://github.com/redpanda-data/redpanda/pull/4180
* [#4113](https://github.com/redpanda-data/redpanda/issues/4113) s/parser: fixed reading batches with header_crc equal to 0. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/4099
* [#4120](https://github.com/redpanda-data/redpanda/issues/4120) Schema Registry: Support default null type in union for Avro. by @BenPope in https://github.com/redpanda-data/redpanda/pull/4129
* [#4171](https://github.com/redpanda-data/redpanda/issues/4171) `delete_retention_ms` interprets `-1` as infinite retention, i.e. never delete data. This is Kafka-compatible and in line with existing documentation. by @LenaAn in https://github.com/redpanda-data/redpanda/pull/4227
* [#4185](https://github.com/redpanda-data/redpanda/issues/4185) Fixed possible deadlock in `raft::mux_state_machine` by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/4153
* [#4224](https://github.com/redpanda-data/redpanda/issues/4224) Fix retention settings not working with acks=1 and enabled shadow indexing. by @ztlpn in https://github.com/redpanda-data/redpanda/pull/4277
* [#4228](https://github.com/redpanda-data/redpanda/issues/4228) [#4236](https://github.com/redpanda-data/redpanda/issues/4236) Fixes issue in leadership draining that may allow some raft group leadership to return. by @dotnwat in https://github.com/redpanda-data/redpanda/pull/4284
* [#4308](https://github.com/redpanda-data/redpanda/issues/4308) ListOffset returns the earliest offset with timestamp greater or equal to the timestamp specifed. If no such an offset is found, offsetsForTimes() method in Consumer should return null. See [KIP-79](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=65868090). by @LenaAn in https://github.com/redpanda-data/redpanda/pull/4322
* [#4310](https://github.com/redpanda-data/redpanda/issues/4310) Accept partition_count, replication_factor and redpanda.datapolicy in alter config handler. by @ZeDRoman in https://github.com/redpanda-data/redpanda/pull/4313
* [#4494](https://github.com/redpanda-data/redpanda/issues/4494) storage: assertion failure in offset_translator_state.cc. by @ztlpn in https://github.com/redpanda-data/redpanda/pull/4497
* [#4352](https://github.com/redpanda-data/redpanda/issues/4352) vote_stm: do not step down when replicating configuration failed. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/4342
* [#4266](https://github.com/redpanda-data/redpanda/issues/4266) dissemination: do not query topic metadata for ntp. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/4389
* [#4181](https://github.com/redpanda-data/redpanda/issues/4181) k/fetch: validate fetch offset against high watermark. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/4567
* Do not update connections with old config. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/4225
* Fix bug in topic recovery. by @Lazin in https://github.com/redpanda-data/redpanda/pull/4312
* Fix consumer group recovery. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/3732
* Fix operator can't be deployed via Helm Chart when another `kubebuilder` operator is deployed to the same namespace. by @rawkode in https://github.com/redpanda-data/redpanda/pull/3871
* Fix partial truncation. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/3722
* Fix self leadership transfer. by @VadimPlh in https://github.com/redpanda-data/redpanda/pull/3446
* Fixed assertion triggered when fetching from empty topics with transactions enabled. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/3159
* Fixed bug that may lead to situation in which partition will not be able to elect a leader since follower heartbeats are suppressed. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/3259
* Fixed cleanup policy application when using cloud storage or transactions. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/3743
* Fixed data loss in shadow indexing archived data that could occur after quick partition leadership transfer back and forth between two nodes. Compatibility note: previous redpanda versions won't be able to read shadow indexing data archived by newer versions. by @ztlpn in https://github.com/redpanda-data/redpanda/pull/3365
* Fixed error preventing redpanda from starting if the only partition movement operation involved x-core move. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/3558
* Fixed incorrect handling of failed snapshot delivery that may lead to situation in which snapshot is being redelivered in tight loop. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/3245
* Fixed regression that caused an assertion to be triggered during Fetch request handling. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/3587
* Fixed time based offset queries in `ListOffsetsRequest` handler. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/3161
* Fixes a consistency issue with transactions. by @rystsov in https://github.com/redpanda-data/redpanda/pull/3232
* Fixes a panic in `rpk group seek` if there was an error during the offset commit. by @twmb in https://github.com/redpanda-data/redpanda/pull/3404
* Fixes a potential bug in consumer groups in which a pending member is stuck in a group because redpanda did not set an expiration time for pending members. by @dotnwat in https://github.com/redpanda-data/redpanda/pull/3761
* Fixes falsely aborted transactions. by @rystsov in https://github.com/redpanda-data/redpanda/pull/3616
* Makes Redpanda transactions compatible with Sarama. by @rystsov in https://github.com/redpanda-data/redpanda/pull/3189
* The cluster metrics reporter now works on single node redpanda clusters as well as multi-node redpanda clusters. by @jcsp in https://github.com/redpanda-data/redpanda/pull/3675
* Using partition `revision_id` to clear out partition leadership metadata by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/3834
* Validation of replica set passed into the move partition admin API. Fixed removing partition that is being moved. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/3846

## Improvements

* [#2142](https://github.com/redpanda-data/redpanda/issues/2142) [#2568](https://github.com/redpanda-data/redpanda/issues/2568) Better handling of consumer group related errors. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/3205
* [#3269](https://github.com/redpanda-data/redpanda/issues/3269) redpanda/cluster: Improve logging for `leader_balancer`. by @BenPope in https://github.com/redpanda-data/redpanda/pull/3271
* [#3270](https://github.com/redpanda-data/redpanda/issues/3270) [#3304](https://github.com/redpanda-data/redpanda/issues/3304) Gracefully handle a situation when segment in S3 is truncated. Before client would be stuck in a cycle trying to proceed, now client will get an error. by @LenaAn in https://github.com/redpanda-data/redpanda/pull/3280
* [#3333](https://github.com/redpanda-data/redpanda/issues/3333) Downgrade log message severity in case of http client disconnect since it's a normal mode of operation. by @LenaAn in https://github.com/redpanda-data/redpanda/pull/3356
* [#3429](https://github.com/redpanda-data/redpanda/issues/3429) [#3561](https://github.com/redpanda-data/redpanda/issues/3561) Better stack traces in case of crash on low memory. by @LenaAn in https://github.com/redpanda-data/redpanda/pull/3570
* [#3542](https://github.com/redpanda-data/redpanda/pull/3542) `under_replicated_partitions` metric now reflects the number of follower which are actually behind the leader. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/3667
* [#3548](https://github.com/redpanda-data/redpanda/issues/3548) Shadow indexing memory utilization was optimised. by @Lazin in https://github.com/redpanda-data/redpanda/pull/3607
* [#3964](https://github.com/redpanda-data/redpanda/issues/3964) "Fetch requested very large response" log messages are reduced from INFO to DEBUG severity. by @jcsp in https://github.com/redpanda-data/redpanda/pull/3965
* [#4083](https://github.com/redpanda-data/redpanda/issues/4083) Improved behavior for restarted raft groups that reduces election churn. by @dotnwat in https://github.com/redpanda-data/redpanda/pull/4151
* [#4149](https://github.com/redpanda-data/redpanda/issues/4149) Alter topic configuration not fail if gets unsupported properties. by @ZeDRoman in https://github.com/redpanda-data/redpanda/pull/4223
* [#4172](https://github.com/redpanda-data/redpanda/issues/4172) Leader rebalancing is linear time in the total number of shards. by @travisdowns in https://github.com/redpanda-data/redpanda/pull/4218
* [#4187](https://github.com/redpanda-data/redpanda/issues/4187) Faster leader election during partition moves. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/4157
* [#4210](https://github.com/redpanda-data/redpanda/issues/4210) Make debugging raft issues easier. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/4206
* [#4376](https://github.com/redpanda-data/redpanda/issues/4375) rpk: improve output on configuration updates. by @dotnwat in https://github.com/redpanda-data/redpanda/pull/4515
* [#3939](https://github.com/redpanda-data/redpanda/issues/3939) Support mTLS principal propagation. by @BenPope in https://github.com/redpanda-data/redpanda/pull/4549
* k8s/operator: Reserve 10% of memory for the OS by default, to avoid `OOMKill`. by @BenPope in https://github.com/redpanda-data/redpanda/pull/3831
* rpk: try to send write requests to Leader but fallback to broadcast to all brokers. by @simon0191 in https://github.com/redpanda-data/redpanda/pull/3565
* Ability to monitor bytes read/written to the partition. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/3530
* Add compressed in-memory index for segments accessed via Shadow Indexing. by @Lazin in https://github.com/redpanda-data/redpanda/pull/3830
* Add schema registry to kubernetes samples. by @BenPope in https://github.com/redpanda-data/redpanda/pull/3869
* Adjust `rpk` help text. by @Deflaimun in https://github.com/redpanda-data/redpanda/pull/3465
* Changed decay coefficient of target priority. This way the target priority will decay faster allowing other nodes to became leader with less failed leader election rounds. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/4070
* Cloud storage key is now redacted when logging redpanda configuration. by @jcsp in https://github.com/redpanda-data/redpanda/pull/3219
* Faster recovery of partition replicas which are behind the leader. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/2683
* Improved error handling when manipulating JSON in low memory situations. by @BenPope in https://github.com/redpanda-data/redpanda/pull/4076
* Logging low storage space condition, to help alert users to adjust retention policies, even if they don't have other external monitoring systems set up. by @ajfabbri in https://github.com/redpanda-data/redpanda/pull/3715
* Logging verbosity during leadership transfer is decreased. by @jcsp in https://github.com/redpanda-data/redpanda/pull/3315
* Pandaproxy REST: Improve header validation and Swagger API. by @BenPope in https://github.com/redpanda-data/redpanda/pull/3664
* Querying partition leader via Metadata API will return correct data. by @mmaslankaprv in https://github.com/redpanda-data/redpanda/pull/3676
* Remove some misleading error-level log messages that occurred during node shutdown. by @jcsp in https://github.com/redpanda-data/redpanda/pull/3226
* Replace ZSTD `allocation_error` to `bad_alloc`. Catch all exception in fiber inside `rps_simple_protocol`. by @VadimPlh in https://github.com/redpanda-data/redpanda/pull/3567
* The `append_chunk_size` configuration property now has an upper bound of 32MiB, to avoid issues when this was erroneously set to very high values. by @jcsp in https://github.com/redpanda-data/redpanda/pull/4107
* Use updated seastar. by @tchaikov in https://github.com/redpanda-data/redpanda/pull/4304
* coproc: Move materialized partitions by @graphcareful in https://github.com/redpanda-data/redpanda/pull/3212
* coproc: New abstraction for safe shutdown of materialized logs by @graphcareful in https://github.com/redpanda-data/redpanda/pull/2940
* coproc: Yield if inputs haven't been hydrated by @graphcareful in https://github.com/redpanda-data/redpanda/pull/3576
* rpk: Various improvements to CLI and upgraded base franz-go. by @twmb in https://github.com/redpanda-data/redpanda/pull/4576

**Full Changelog**: https://github.com/redpanda-data/redpanda/compare/v21.11.15..d6bc057df1050d138cff92b9ee38b9cb38c0e50c
