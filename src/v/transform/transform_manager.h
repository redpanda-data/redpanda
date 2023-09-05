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

#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/ktp.h"
#include "model/metadata.h"
#include "model/transform.h"
#include "ssx/work_queue.h"
#include "transform/fwd.h"
#include "transform/io.h"
#include "wasm/fwd.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/util/bool_class.hh>

namespace transform {

using ntp_leader = ss::bool_class<struct is_ntp_leader>;

// This allows reading the existing transforms by input topic or by ID.
//
// This allows us to swap out the data source for plugins in tests easily.
class registry {
public:
    registry() = default;
    registry(const registry&) = delete;
    registry& operator=(const registry&) = delete;
    registry(registry&&) = default;
    registry& operator=(registry&&) = default;
    virtual ~registry() = default;

    // Get all the partitions that are leaders and this shard is responsible
    // for
    virtual absl::flat_hash_set<model::partition_id>
      get_leader_partitions(model::topic_namespace_view) const = 0;

    // Get all the transforms with this ns_tp as the input source.
    virtual absl::flat_hash_map<model::transform_id, model::transform_metadata>
      lookup_by_input_topic(model::topic_namespace_view) const = 0;

    // Lookup a transform by ID
    virtual std::optional<model::transform_metadata>
      lookup_by_id(model::transform_id) const = 0;
};

// An interface for creating processors.
//
// Mostly used by tests to inject custom processors that report their lifetime
// to test infrastructure.
class processor_factory {
public:
    processor_factory() = default;
    processor_factory(const processor_factory&) = default;
    processor_factory(processor_factory&&) = delete;
    processor_factory& operator=(const processor_factory&) = default;
    processor_factory& operator=(processor_factory&&) = delete;
    virtual ~processor_factory() = default;

    // Create a processor with the given metadata and input partition.
    virtual ss::future<std::unique_ptr<processor>> create_processor(
      model::transform_id, model::ntp, model::transform_metadata, probe*)
      = 0;
};

class processor_table;

// transform manager is responsible for managing the lifetime of a processor and
// starting/stopping processors when various lifecycle events happen in the
// system, such as leadership changes, or deployments of new transforms.
//
// There is a manager per core and it only handles the transforms where the
// transform's source ntp is leader on the same shard.
//
// Internally, the manager operates on a single queue and lifecycle changes
// cannot proceed concurrently, this way we don't have to try and juggle the
// futures if a wasm::engine is starting up and a request comes in to tear it
// down, we'll just handle them in the order they where submitted to the
// manager. Note that it maybe possible to allow for **each** processor to have
// it's own queue in the manager, but until it's proven to be required, a per
// shard queue is used.
class manager {
public:
    manager(std::unique_ptr<registry>, std::unique_ptr<processor_factory>);
    manager(const manager&) = delete;
    manager& operator=(const manager&) = delete;
    manager(manager&&) = delete;
    manager& operator=(manager&&) = delete;
    ~manager();

    ss::future<> start();
    ss::future<> stop();

    // Called when this shard's ownership of an ntp leader changes
    void on_leadership_change(model::ntp, ntp_leader);
    // Called everytime a transform changes
    void on_plugin_change(model::transform_id);
    // Called when processors have errors
    void on_transform_error(
      model::transform_id, model::ntp, model::transform_metadata);

    // Exposed for testing, but drains all the pending operations.
    //
    // Any future here should resolve before calling `stop`.
    ss::future<> drain_queue_for_test();

private:
    // All these private methods must be call "on" the queue.

    // Implmentation of `on_leadership_change`
    ss::future<> handle_leadership_change(model::ntp, ntp_leader);
    // Implementation of `on_plugin_change`
    ss::future<> handle_plugin_change(model::transform_id);
    // Implementation of `on_transform_error`
    ss::future<> handle_transform_error(
      model::transform_id, model::ntp, model::transform_metadata);
    // Attempt to start a processor if the existing one is idle or there is no
    // currently running processor.
    ss::future<> start_processor(model::ntp, model::transform_id);
    // Create a processor - this should only be called if it there is no
    // existing processor for this ntp + id.
    ss::future<> create_processor(
      model::ntp, model::transform_id, model::transform_metadata);

    ssx::work_queue _queue;
    std::unique_ptr<registry> _registry;
    std::unique_ptr<processor_table> _processors;
    std::unique_ptr<processor_factory> _processor_factory;
};
} // namespace transform
