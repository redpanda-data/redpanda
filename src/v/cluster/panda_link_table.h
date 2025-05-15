/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "model/panda_link.h"

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
namespace cluster {
class panda_link_table {
    using map_t
      = absl::btree_map<model::panda_link_id, model::panda_link_metadata>;

public:
    panda_link_table() = default;

    panda_link_table(const panda_link_table&) = delete;
    panda_link_table& operator=(const panda_link_table&) = delete;
    panda_link_table(panda_link_table&&) = delete;
    panda_link_table& operator=(panda_link_table&&) = delete;
    ~panda_link_table() = default;

    using notification_id
      = named_type<size_t, struct panda_link_notification_id_tag>;
    using notification_callback
      = ss::noncopyable_function<void(model::panda_link_id)>;

    /// Snapshot copy of all the links
    map_t all_links() const;
    /// Size of the links
    size_t size() const;
    /// Restores the panda link table from a snapshot
    void reset_links(map_t);

    std::optional<model::panda_link_metadata>
      find_by_name(std::string_view) const;
    std::optional<model::panda_link_metadata>
    find_by_name(const model::panda_link_name&) const;
    std::optional<model::panda_link_id> find_id_by_name(std::string_view) const;
    std::optional<model::panda_link_id>
    find_id_by_name(const model::panda_link_name&) const;
    std::optional<model::panda_link_metadata>
      find_by_id(model::panda_link_id) const;

    void upsert_link(model::panda_link_id, model::panda_link_metadata);
    void remove_link(const model::panda_link_name& name);

    notification_id register_for_updates(notification_callback);
    void unregister_for_updates(notification_id);

private:
    void run_callbacks(model::panda_link_id);

private:
    struct name_less_cmp {
        using is_transparent = void;
        bool operator()(
          const model::panda_link_name&, const model::panda_link_name&) const;
        bool operator()(const model::panda_link_name&, std::string_view) const;
        bool operator()(std::string_view, const model::panda_link_name&) const;
    };

    using name_index_t = absl::
      btree_map<model::panda_link_name, model::panda_link_id, name_less_cmp>;

    map_t _underlying;
    name_index_t _name_index;

    absl::flat_hash_map<notification_id, notification_callback> _callbacks;
    notification_id _latest_id{0};
};
} // namespace cluster
