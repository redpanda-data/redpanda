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
#include "base/seastarx.h"
#include "cluster/commands.h"
#include "cluster/fwd.h"
#include "cluster/plugin_table.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "rpc/connection_cache.h"
#include "utils/mutex.h"

#include <absl/container/flat_hash_set.h>

#include <variant>

#pragma once

namespace cluster {

/**
 * The plugin frontend takes care of all (WASM) plugin related functionality and
 * servicing requests from the rest of the cluster.
 *
 * Instance per core.
 */
class plugin_frontend : public ss::peering_sharded_service<plugin_frontend> {
private:
    using transform_cmd
      = std::variant<transform_update_cmd, transform_remove_cmd>;

public:
    // The plugin frontend takes the corresponding services for it's core.
    //
    // The exception being the controller_stm which is only nonnull for the
    // service on core 0.
    plugin_frontend(
      model::node_id,
      partition_leaders_table*,
      plugin_table*,
      topic_table*,
      controller_stm*,
      rpc::connection_cache*,
      ss::abort_source*);

    using notification_id = plugin_table::notification_id;
    using notification_callback = plugin_table::notification_callback;
    struct mutation_result {
        // The UUID of the modified transform, mostly useful for cleanup
        // of wasm_binaries.
        // See transform_metadata::uuid
        uuid_t uuid;
        errc ec;
    };

    // Create or update a transform by name.
    ss::future<errc> upsert_transform(
      model::transform_metadata, model::timeout_clock::time_point);

    // Remove a transform by name.
    ss::future<mutation_result>
      remove_transform(model::transform_name, model::timeout_clock::time_point);

    // Register for updates going forward.
    //
    // This is a core local operation, you can register for updates to the
    // transforms table. If you want to lookup the transform you can then do
    // that in the callback.
    notification_id register_for_updates(notification_callback);

    // Unregister a previously registered notification.
    void unregister_for_updates(notification_id);

    // Lookup a transform by ID. Useful when paired with registering for
    // updates.
    std::optional<model::transform_metadata>
      lookup_transform(model::transform_id) const;

    // Lookup a transform by name.
    std::optional<model::transform_metadata>
    lookup_transform(const model::transform_name&) const;

    // Lookup transforms for input topics.
    absl::btree_map<model::transform_id, model::transform_metadata>
      lookup_transforms_by_input_topic(model::topic_namespace_view) const;

    // Get a snapshot of all the transforms that currently exist.
    absl::btree_map<model::transform_id, model::transform_metadata>
    all_transforms() const;

private:
    // Perform a mutation request, check if this node is the cluster leader and
    // ensuring that the right core is used for local mutations.
    ss::future<mutation_result>
      do_mutation(transform_cmd, model::timeout_clock::time_point);

    // If this node is not the leader, dispatch this mutation to `node_id` as
    // the cluster's leader.
    ss::future<mutation_result> dispatch_mutation_to_remote(
      model::node_id, transform_cmd, model::timeout_clock::duration);

    // Performs (and validates) a mutation command to be inserted into the
    // controller log.
    //
    // This must take place on the controller shard on the cluster leader.
    ss::future<mutation_result>
      do_local_mutation(transform_cmd, model::timeout_clock::time_point);

    // Ensures that the mutation is valid.
    //
    // This must take place on the controller shard on the cluster leader while
    // holding _mu to ensure consistency.
    errc validate_mutation(const transform_cmd&);

public:
    /**
     * A validator of commands that come through, the validation must happen on
     * the controller leader, core 0 so that these is a consistent view of the
     * data in the plugin table.
     *
     * Mostly exposed seperately for unit testing.
     */
    class validator {
    public:
        validator(
          topic_table*,
          plugin_table*,
          absl::flat_hash_set<model::topic>,
          size_t max_transforms);

        // Ensures that the mutation is valid.
        //
        // NOTE: At the time of writing the topic validation is best effort,
        // it is possible to race with mutations in the topics_frontend and
        // topics can be deleted or have partitions added that wouldn't be
        // valid otherwise.
        errc validate_mutation(const transform_cmd&);

    private:
        // Would adding this input->output transform cause a cycle?
        bool would_cause_cycle(
          model::topic_namespace_view, model::topic_namespace);

        topic_table* _topics;
        plugin_table* _table;
        absl::flat_hash_set<model::topic> _no_sink_topics;
        size_t _max_transforms;
    };

private:
    // Can only be accessed on the current shard.
    model::node_id _self;
    partition_leaders_table* _leaders;
    rpc::connection_cache* _connections;
    plugin_table* _table;
    topic_table* _topics;
    ss::abort_source* _abort_source;

    // is null if not on shard0
    controller_stm* _controller;
    mutex _mu{"plugin_frontend::mu"};
};
} // namespace cluster
