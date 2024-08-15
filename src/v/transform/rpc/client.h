/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/errc.h"
#include "cluster/fwd.h"
#include "config/property.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "model/transform.h"
#include "rpc/connection_cache.h"
#include "transform/rpc/deps.h"
#include "transform/rpc/service.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/sharded.hh>

#include <absl/container/flat_hash_map.h>

#include <memory>
#include <type_traits>

namespace transform::rpc {

/**
 * A client for the transform rpc service.
 *
 * This is a sharded service that exists on every core, requests that can be
 * serviced locally will not go through the rpc boundary but will directly go to
 * the local service.
 */
class client {
public:
    client(
      model::node_id self,
      std::unique_ptr<partition_leader_cache>,
      std::unique_ptr<topic_metadata_cache>,
      std::unique_ptr<topic_creator>,
      std::unique_ptr<cluster_members_cache>,
      ss::sharded<::rpc::connection_cache>*,
      ss::sharded<local_service>*,
      config::binding<size_t>);
    client(client&&) = delete;
    client& operator=(client&&) = delete;
    client(const client&) = delete;
    client& operator=(const client&) = delete;
    ~client() = default;

    /**
     * Produce the following record batches to the specified topic partition in
     * the kafka namespace.
     *
     * This implementation will retry failed produce requests rather
     * aggressively, (without idempotency at the time of writing) so take note
     * that this can easily produce duplicate data.
     */
    ss::future<cluster::errc>
      produce(model::topic_partition, ss::chunked_fifo<model::record_batch>);

    /**
     * Write a Wasm binary to our internal topic and return metadata for it.
     *
     * This RPC is used as part of the deploy path of new transforms in the
     * control plane.
     */
    ss::future<result<stored_wasm_binary_metadata, cluster::errc>>
    store_wasm_binary(
      model::wasm_binary_iobuf, model::timeout_clock::duration timeout);

    /**
     * Delete a wasm binary.
     *
     * This is done by writing an empty record with the key (that is returned in
     * the metadata of the store RPC), and letting the topic be compacted later.
     */
    ss::future<cluster::errc>
    delete_wasm_binary(uuid_t key, model::timeout_clock::duration timeout);

    /**
     * Reads a wasm binary at the offset that was returned in the metadata of
     * the store RPC).
     */
    ss::future<result<model::wasm_binary_iobuf, cluster::errc>>
    load_wasm_binary(model::offset, model::timeout_clock::duration timeout);

    /**
     * Read the offset (if present) for this key.
     *
     * This is used to resume data transforms from the latest position when a
     * processor is started.
     */
    ss::future<
      result<std::optional<model::transform_offsets_value>, cluster::errc>>
      offset_fetch(model::transform_offsets_key);

    /**
     * Find the partition that owns this transform_offsets_key for persisting
     * transform offsets.
     */
    ss::future<result<model::partition_id, cluster::errc>>
      find_coordinator(model::transform_offsets_key);

    /**
     * Batch commit a group of offsets that all belong to a given coordinator.
     */
    ss::future<cluster::errc> batch_offset_commit(
      model::partition_id coordinator,
      absl::btree_map<
        model::transform_offsets_key,
        model::transform_offsets_value>);

    /**
     * Generate a status report for all nodes in the cluster of transforms in
     * their current state.
     *
     * This report also includes the local node.
     *
     * The report is sparse, meaning that if a transform is not running in a
     * cluster we don't fill in a value for it. If default values for missing
     * metadata the report is required, the caller should fill that in.
     *
     */
    ss::future<model::cluster_transform_report> generate_report();

    /**
     * Create the topic for transform logs output.
     */
    ss::future<cluster::errc> create_transform_logs_topic();

    /**
     * List all the tracked offsets for all transforms within the cluster.
     */
    ss::future<result<model::transform_offsets_map, cluster::errc>>
    list_committed_offsets();

    /**
     * Delete all committed offsets for this transform ID.
     */
    ss::future<cluster::errc>
    delete_committed_offsets(absl::btree_set<model::transform_id> ids);

    ss::future<> start();
    ss::future<> stop();

private:
    ss::future<model::cluster_transform_report>
      generate_one_report(model::node_id);
    ss::future<result<model::cluster_transform_report, cluster::errc>>
      generate_remote_report(model::node_id);

    ss::future<cluster::errc> do_produce_once(produce_request);
    ss::future<produce_reply> do_local_produce(produce_request);
    ss::future<produce_reply>
      do_remote_produce(model::node_id, produce_request);

    ss::future<result<stored_wasm_binary_metadata, cluster::errc>>
    do_store_wasm_binary_once(
      model::wasm_binary_iobuf, model::timeout_clock::duration timeout);
    ss::future<result<stored_wasm_binary_metadata, cluster::errc>>
    do_local_store_wasm_binary(
      model::wasm_binary_iobuf, model::timeout_clock::duration timeout);
    ss::future<result<stored_wasm_binary_metadata, cluster::errc>>
    do_remote_store_wasm_binary(
      model::node_id,
      model::wasm_binary_iobuf,
      model::timeout_clock::duration timeout);

    ss::future<cluster::errc> do_delete_wasm_binary_once(
      uuid_t key, model::timeout_clock::duration timeout);
    ss::future<cluster::errc> do_local_delete_wasm_binary(
      uuid_t key, model::timeout_clock::duration timeout);
    ss::future<cluster::errc> do_remote_delete_wasm_binary(
      model::node_id, uuid_t key, model::timeout_clock::duration timeout);

    ss::future<result<model::wasm_binary_iobuf, cluster::errc>>
    do_load_wasm_binary_once(
      model::offset, model::timeout_clock::duration timeout);
    ss::future<result<model::wasm_binary_iobuf, cluster::errc>>
    do_local_load_wasm_binary(
      model::offset, model::timeout_clock::duration timeout);
    ss::future<result<model::wasm_binary_iobuf, cluster::errc>>
    do_remote_load_wasm_binary(
      model::node_id, model::offset, model::timeout_clock::duration timeout);

    ss::future<std::optional<model::node_id>> compute_wasm_binary_ntp_leader();
    ss::future<bool> try_create_wasm_binary_ntp();
    ss::future<bool> try_create_transform_offsets_topic();

    ss::future<cluster::errc> try_create_transform_logs_topic();

    ss::future<result<model::partition_id, cluster::errc>>
      find_coordinator_once(model::transform_offsets_key);
    ss::future<cluster::errc> batch_offset_commit_once(
      model::partition_id coordinator,
      absl::btree_map<
        model::transform_offsets_key,
        model::transform_offsets_value>);
    ss::future<
      result<std::optional<model::transform_offsets_value>, cluster::errc>>
      offset_fetch_once(model::transform_offsets_key);

    ss::future<find_coordinator_response>
      do_local_find_coordinator(find_coordinator_request);
    ss::future<offset_commit_response>
      do_local_offset_commit(offset_commit_request);
    ss::future<offset_fetch_response>
      do_local_offset_fetch(offset_fetch_request);

    ss::future<find_coordinator_response>
      do_remote_find_coordinator(model::node_id, find_coordinator_request);
    ss::future<offset_commit_response>
      do_remote_offset_commit(model::node_id, offset_commit_request);
    ss::future<offset_fetch_response>
      do_remote_offset_fetch(model::node_id, offset_fetch_request);

    ss::future<result<model::transform_offsets_map, cluster::errc>>
      do_list_committed_offsets(model::partition_id);
    ss::future<result<model::transform_offsets_map, cluster::errc>>
      do_list_committed_offsets_once(model::partition_id);
    ss::future<result<model::transform_offsets_map, cluster::errc>>
      do_local_list_committed_offsets(model::partition_id);
    ss::future<result<model::transform_offsets_map, cluster::errc>>
    do_remote_list_committed_offsets(
      model::node_id,
      model::partition_id,
      model::timeout_clock::duration timeout);

    ss::future<cluster::errc> do_delete_committed_offsets(
      model::partition_id, absl::btree_set<model::transform_id>);
    ss::future<cluster::errc> do_delete_committed_offsets_once(
      model::partition_id, absl::btree_set<model::transform_id>);
    ss::future<cluster::errc> do_local_delete_committed_offsets(
      model::partition_id, absl::btree_set<model::transform_id>);
    ss::future<cluster::errc> do_remote_delete_committed_offsets(
      model::node_id,
      model::partition_id,
      absl::btree_set<model::transform_id>,
      model::timeout_clock::duration timeout);

    template<typename Func>
    std::invoke_result_t<Func> retry(Func&&);

    ss::future<> update_wasm_binary_size();

    model::node_id _self;
    std::unique_ptr<cluster_members_cache> _cluster_members;
    // need partition_leaders_table to know which node owns the partitions
    std::unique_ptr<partition_leader_cache> _leaders;
    std::unique_ptr<topic_metadata_cache> _topic_metadata;
    std::unique_ptr<topic_creator> _topic_creator;
    ss::sharded<::rpc::connection_cache>* _connections;
    ss::sharded<local_service>* _local_service;
    ss::abort_source _as;
    ss::gate _gate;
    mutex _wasm_binary_max_size_updater_mu{
      "client::wasm_binary_max_size_updater"};
    config::binding<size_t> _max_wasm_binary_size;
};

} // namespace transform::rpc
