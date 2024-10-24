// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "model/transform.h"
#include "transform/rpc/deps.h"
#include "transform/rpc/rpc_service.h"
#include "transform/rpc/serde.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/sharded.hh>

#include <memory>

namespace transform::rpc {

/**
 * A per core sharded service that handles custom data path requests for data
 * transforms and storage of wasm binaries.
 */
class local_service {
public:
    local_service(
      std::unique_ptr<topic_metadata_cache> metadata_cache,
      std::unique_ptr<partition_manager> partition_manager,
      std::unique_ptr<reporter>);

    ss::future<ss::chunked_fifo<transformed_topic_data_result>> produce(
      ss::chunked_fifo<transformed_topic_data> topic_data,
      model::timeout_clock::duration timeout);

    ss::future<result<stored_wasm_binary_metadata, cluster::errc>>
    store_wasm_binary(
      model::wasm_binary_iobuf, model::timeout_clock::duration timeout);

    ss::future<cluster::errc>
    delete_wasm_binary(uuid_t key, model::timeout_clock::duration timeout);

    ss::future<result<model::wasm_binary_iobuf, cluster::errc>>
    load_wasm_binary(model::offset, model::timeout_clock::duration timeout);

    ss::future<find_coordinator_response>
      find_coordinator(find_coordinator_request);
    ss::future<offset_commit_response> offset_commit(offset_commit_request);
    ss::future<offset_fetch_response> offset_fetch(offset_fetch_request);

    ss::future<model::cluster_transform_report> compute_node_local_report();

    ss::future<result<model::transform_offsets_map, cluster::errc>>
      list_committed_offsets(list_commits_request);

    ss::future<cluster::errc> delete_committed_offsets(
      model::partition_id, absl::btree_set<model::transform_id>);

private:
    ss::future<transformed_topic_data_result>
      produce(transformed_topic_data, model::timeout_clock::duration);

    ss::future<result<model::offset, cluster::errc>> produce(
      model::any_ntp auto,
      ss::chunked_fifo<model::record_batch>,
      model::timeout_clock::duration);

    ss::future<result<model::wasm_binary_iobuf, cluster::errc>>
      consume_wasm_binary_reader(
        model::record_batch_reader, model::timeout_clock::duration);

    std::unique_ptr<topic_metadata_cache> _metadata_cache;
    std::unique_ptr<partition_manager> _partition_manager;
    std::unique_ptr<reporter> _reporter;
};

/**
 * A networked wrapper for the local service.
 */
class network_service final : public impl::transform_rpc_service {
public:
    network_service(
      ss::scheduling_group sc,
      ss::smp_service_group ssg,
      ss::sharded<local_service>* service)
      : impl::transform_rpc_service{sc, ssg}
      , _service(service) {}

    ss::future<produce_reply>
    produce(produce_request, ::rpc::streaming_context&) override;

    ss::future<store_wasm_binary_reply> store_wasm_binary(
      store_wasm_binary_request, ::rpc::streaming_context&) override;

    ss::future<load_wasm_binary_reply> load_wasm_binary(
      load_wasm_binary_request, ::rpc::streaming_context&) override;

    ss::future<delete_wasm_binary_reply> delete_wasm_binary(
      delete_wasm_binary_request, ::rpc::streaming_context&) override;

    ss::future<find_coordinator_response> find_coordinator(
      find_coordinator_request, ::rpc::streaming_context&) override;

    ss::future<offset_commit_response>
    offset_commit(offset_commit_request, ::rpc::streaming_context&) override;

    ss::future<offset_fetch_response>
    offset_fetch(offset_fetch_request, ::rpc::streaming_context&) override;

    ss::future<list_commits_reply> list_committed_offsets(
      list_commits_request, ::rpc::streaming_context&) override;

    ss::future<delete_commits_reply> delete_committed_offsets(
      delete_commits_request, ::rpc::streaming_context&) override;

    ss::future<generate_report_reply> generate_report(
      generate_report_request, ::rpc::streaming_context&) override;

private:
    ss::sharded<local_service>* _service;
};

} // namespace transform::rpc
