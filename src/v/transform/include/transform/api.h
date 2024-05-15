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

#include "base/outcome.h"
#include "base/seastarx.h"
#include "cluster/fwd.h"
#include "features/fwd.h"
#include "model/metadata.h"
#include "model/transform.h"
#include "raft/fwd.h"
#include "transform/fwd.h"
#include "transform/logging/fwd.h"
#include "wasm/fwd.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/noncopyable_function.hh>

#include <system_error>

namespace transform {

/** Request options for listing committed offsets. */
struct list_committed_offsets_options {
    // If true, show transforms that we don't have metadata for, these likely
    // represent transforms that have been deleted.
    bool show_unknown = false;
};

/**
 * The transform service is responsible for intersecting the current state of
 * plugins and topic partitions and ensures that the corresponding wasm
 * transform is running for each leader partition (on the input topic).
 *
 * This service is mostly responsible for interfacing the rest of the system
 * with the transform control plane (transform::manager), and forwarding the
 * correct events from the rest of the system into the control plane.
 *
 * Instances on every shard.
 */
class service : public ss::peering_sharded_service<service> {
public:
    service(
      wasm::caching_runtime* runtime,
      model::node_id self,
      ss::sharded<cluster::plugin_frontend>* plugin_frontend,
      ss::sharded<features::feature_table>* feature_table,
      ss::sharded<raft::group_manager>* group_manager,
      ss::sharded<cluster::topic_table>* topic_table,
      ss::sharded<cluster::partition_manager>* partition_manager,
      ss::sharded<rpc::client>* rpc_client,
      ss::sharded<cluster::metadata_cache>* metadata_cache,
      ss::scheduling_group sg,
      size_t memory_limit);
    service(const service&) = delete;
    service(service&&) = delete;
    service& operator=(const service&) = delete;
    service& operator=(service&&) = delete;
    ~service();

    ss::future<> start();
    ss::future<> stop();

    /**
     * Deploy a transform to the cluster.
     */
    ss::future<std::error_code>
      deploy_transform(model::transform_metadata, model::wasm_binary_iobuf);

    /**
     * Delete a transform from the cluster.
     */
    ss::future<std::error_code> delete_transform(model::transform_name);

    /**
     * List all transforms from the entire cluster.
     */
    ss::future<model::cluster_transform_report> list_transforms();

    /**
     * List the committed offsets for each transform/partition.
     *
     * NOTE: This information is **not** guarenteed to be consistent and may
     * provide an information for different offsets at different points in time.
     */
    ss::future<result<
      ss::chunked_fifo<model::transform_committed_offset>,
      std::error_code>>
      list_committed_offsets(list_committed_offsets_options);

    /**
     * Delete all offsets from transforms that do not exist.
     *
     * NOTE: This should only be ran while new transforms are not deploying, as
     * it is possible to delete transforms for newly deployed transforms as our
     * control plane operations are eventually consistent, so the shard that is
     * performing the garbage collection may have out of date information (and
     * gathering all the offsets that exist can result in an inconsistent view
     * of information anyways).
     */
    ss::future<std::error_code> garbage_collect_committed_offsets();

    /**
     * Patch the metadata for the given transform.
     */
    ss::future<std::error_code> patch_transform_metadata(
      model::transform_name, model::transform_metadata_patch data);

    /**
     * Create a reporter of the transform subsystem.
     */
    static std::unique_ptr<rpc::reporter>
    create_reporter(ss::sharded<service>*);

private:
    void register_notifications();
    void unregister_notifications();

    ss::future<> cleanup_wasm_binary(uuid_t);

    ss::future<ss::optimized_optional<ss::shared_ptr<wasm::engine>>>
      create_engine(model::transform_metadata);

    ss::future<
      ss::optimized_optional<ss::foreign_ptr<ss::shared_ptr<wasm::factory>>>>
      get_factory(model::transform_metadata);

    friend class wrapped_service_reporter;
    ss::future<model::cluster_transform_report> compute_node_local_report();
    model::cluster_transform_report compute_default_report();

    ss::gate _gate;

    wasm::caching_runtime* _runtime;
    model::node_id _self;
    ss::sharded<cluster::plugin_frontend>* _plugin_frontend;
    ss::sharded<features::feature_table>* _feature_table;
    ss::sharded<raft::group_manager>* _group_manager;
    ss::sharded<cluster::topic_table>* _topic_table;
    ss::sharded<cluster::partition_manager>* _partition_manager;
    ss::sharded<rpc::client>* _rpc_client;
    ss::sharded<cluster::metadata_cache>* _metadata_cache;
    std::unique_ptr<manager<ss::lowres_clock>> _manager;
    std::unique_ptr<commit_batcher<ss::lowres_clock>> _batcher;
    std::vector<ss::deferred_action<ss::noncopyable_function<void()>>>
      _notification_cleanups;
    ss::scheduling_group _sg;
    std::unique_ptr<logging::manager<ss::lowres_clock>> _log_manager;

    // The total amount of memory available to transforms
    size_t _total_memory_limit;
};

} // namespace transform
