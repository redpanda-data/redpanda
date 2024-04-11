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

#include "model/metadata.h"
#include "model/transform.h"

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_set.h>

namespace cluster {
/**
 * The plugin table exists on every core and holds the underlying data for all
 * plugin metadata.
 */
class plugin_table {
    using map_t
      = absl::btree_map<model::transform_id, model::transform_metadata>;

public:
    plugin_table() = default;
    plugin_table(const plugin_table&) = delete;
    plugin_table& operator=(const plugin_table&) = delete;
    plugin_table(plugin_table&&) = default;
    plugin_table& operator=(plugin_table&&) = default;
    ~plugin_table() = default;

    using notification_id = named_type<size_t, struct plugin_notif_id_tag>;
    using notification_callback
      = ss::noncopyable_function<void(model::transform_id)>;

    // Snapshot (copy) of all the transforms
    map_t all_transforms() const;
    // Number of transforms
    size_t size() const;

    // Lookups
    std::optional<model::transform_metadata>
      find_by_name(std::string_view) const;
    std::optional<model::transform_metadata>
    find_by_name(const model::transform_name&) const;
    std::optional<model::transform_id> find_id_by_name(std::string_view) const;
    std::optional<model::transform_id>
    find_id_by_name(const model::transform_name&) const;
    std::optional<model::transform_metadata>
      find_by_id(model::transform_id) const;
    // All the transforms that use this topic as input
    map_t find_by_input_topic(model::topic_namespace_view) const;
    // All the transforms that output to this topic
    map_t find_by_output_topic(model::topic_namespace_view) const;

    // Create or update transform metadata.
    void upsert_transform(model::transform_id id, model::transform_metadata);
    // remove transform metadata by name.
    void remove_transform(const model::transform_name&);
    // reset all transforms to be the following snapshot of the table.
    // notifications are only fired for the delta of changes.
    void reset_transforms(map_t);

    notification_id register_for_updates(notification_callback);

    void unregister_for_updates(notification_id);

private:
    struct name_less_cmp {
        using is_transparent = void;
        bool operator()(
          const model::transform_name&, const model::transform_name&) const;
        bool
        operator()(const std::string_view&, const model::transform_name&) const;
        bool
        operator()(const model::transform_name&, const std::string_view&) const;
    };
    using name_index_t = absl::
      btree_map<model::transform_name, model::transform_id, name_less_cmp>;

    struct topic_map_less_cmp {
        using is_transparent = void;
        bool operator()(
          const model::topic_namespace&, const model::topic_namespace&) const;
        bool operator()(
          const model::topic_namespace_view&,
          const model::topic_namespace_view&) const;
    };
    using topic_index_t = absl::btree_map<
      model::topic_namespace,
      absl::flat_hash_set<model::transform_id>,
      topic_map_less_cmp>;

    void run_callbacks(model::transform_id);
    map_t
    find_by_topic(const topic_index_t&, model::topic_namespace_view) const;

    map_t _underlying;
    name_index_t _name_index;
    topic_index_t _input_topic_index;
    topic_index_t _output_topic_index;
    absl::flat_hash_map<notification_id, notification_callback> _callbacks;
    notification_id _latest_id{0};
};
} // namespace cluster
