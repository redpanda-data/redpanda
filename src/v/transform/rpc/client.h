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
      ss::sharded<local_service>*);
    client(client&&) = delete;
    client& operator=(client&&) = delete;
    client(const client&) = delete;
    client& operator=(const client&) = delete;
    ~client() = default;

    ss::future<cluster::errc>
      produce(model::topic_partition, ss::chunked_fifo<model::record_batch>);

    ss::future<result<stored_wasm_binary_metadata, cluster::errc>>
    store_wasm_binary(iobuf, model::timeout_clock::duration timeout);

    ss::future<cluster::errc>
    delete_wasm_binary(uuid_t key, model::timeout_clock::duration timeout);

    ss::future<result<iobuf, cluster::errc>>
    load_wasm_binary(model::offset, model::timeout_clock::duration timeout);

    ss::future<result<model::partition_id, cluster::errc>>
      find_coordinator(model::transform_offsets_key);

    ss::future<
      result<std::optional<model::transform_offsets_value>, cluster::errc>>
      offset_fetch(model::transform_offsets_key);

    ss::future<cluster::errc> batch_offset_commit(
      model::partition_id coordinator,
      absl::btree_map<
        model::transform_offsets_key,
        model::transform_offsets_value>);

    ss::future<model::cluster_transform_report> generate_report();

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
    do_store_wasm_binary_once(iobuf, model::timeout_clock::duration timeout);
    ss::future<result<stored_wasm_binary_metadata, cluster::errc>>
    do_local_store_wasm_binary(iobuf, model::timeout_clock::duration timeout);
    ss::future<result<stored_wasm_binary_metadata, cluster::errc>>
    do_remote_store_wasm_binary(
      model::node_id, iobuf, model::timeout_clock::duration timeout);

    ss::future<cluster::errc> do_delete_wasm_binary_once(
      uuid_t key, model::timeout_clock::duration timeout);
    ss::future<cluster::errc> do_local_delete_wasm_binary(
      uuid_t key, model::timeout_clock::duration timeout);
    ss::future<cluster::errc> do_remote_delete_wasm_binary(
      model::node_id, uuid_t key, model::timeout_clock::duration timeout);

    ss::future<result<iobuf, cluster::errc>> do_load_wasm_binary_once(
      model::offset, model::timeout_clock::duration timeout);
    ss::future<result<iobuf, cluster::errc>> do_local_load_wasm_binary(
      model::offset, model::timeout_clock::duration timeout);
    ss::future<result<iobuf, cluster::errc>> do_remote_load_wasm_binary(
      model::node_id, model::offset, model::timeout_clock::duration timeout);

    ss::future<std::optional<model::node_id>> compute_wasm_binary_ntp_leader();
    ss::future<bool> try_create_wasm_binary_ntp();
    ss::future<> try_create_transform_offsets_topic();

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

    template<typename Func>
    std::invoke_result_t<Func> retry(Func&&);

    model::node_id _self;
    std::unique_ptr<cluster_members_cache> _cluster_members;
    // need partition_leaders_table to know which node owns the partitions
    std::unique_ptr<partition_leader_cache> _leaders;
    std::unique_ptr<topic_metadata_cache> _topic_metadata;
    std::unique_ptr<topic_creator> _topic_creator;
    ss::sharded<::rpc::connection_cache>* _connections;
    ss::sharded<local_service>* _local_service;
    ss::abort_source _as;
};

} // namespace transform::rpc
