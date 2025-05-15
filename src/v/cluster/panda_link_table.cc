/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/panda_link_table.h"

#include <seastar/core/chunked_fifo.hh>

#include <optional>

namespace cluster {
using model::panda_link_id;
using model::panda_link_metadata;
using model::panda_link_name;

panda_link_table::map_t panda_link_table::all_links() const {
    return _underlying;
}

size_t panda_link_table::size() const { return _underlying.size(); }

void panda_link_table::reset_links(map_t snap) {
    name_index_t snap_name_index;

    ss::chunked_fifo<panda_link_id> all_deletes;
    ss::chunked_fifo<panda_link_id> all_inserts;
    ss::chunked_fifo<panda_link_id> all_changed;

    for (const auto& [k, v] : _underlying) {
        auto it = snap.find(k);
        if (it == snap.end()) {
            all_deletes.push_back(k);
        } else if (v != it->second) {
            all_changed.push_back(k);
        }
    }

    for (const auto& [k, v] : snap) {
        if (_underlying.find(k) == _underlying.end()) {
            all_inserts.push_back(k);
        }
        auto it = snap_name_index.insert({v.name, k});
        if (!it.second) {
            throw std::logic_error(ss::format(
              "panda link id={} is attempting to use a name {} which is "
              "already registered to {}",
              k,
              v.name,
              it.first->second));
        }
    }
    _underlying = std::move(snap);
    _name_index = std::move(snap_name_index);

    for (const auto& deleted : all_deletes) {
        run_callbacks(deleted);
    }
    for (const auto& updated : all_changed) {
        run_callbacks(updated);
    }
    for (const auto& inserted : all_inserts) {
        run_callbacks(inserted);
    }
}

std::optional<panda_link_metadata>
panda_link_table::find_by_name(std::string_view name) const {
    auto id = find_id_by_name(name);
    if (!id.has_value()) {
        return std::nullopt;
    }

    auto meta = find_by_id(id.value());
    vassert(
      meta.has_value(),
      "Inconsistent name index for {} expected id {}",
      name,
      id.value());

    return meta;
}

std::optional<panda_link_metadata>
panda_link_table::find_by_name(const panda_link_name& name) const {
    return find_by_name(std::string_view{name()});
}

std::optional<panda_link_id>
panda_link_table::find_id_by_name(std::string_view name) const {
    auto it = _name_index.find(name);
    if (it == _name_index.end()) {
        return std::nullopt;
    }
    return it->second;
}

std::optional<panda_link_id>
panda_link_table::find_id_by_name(const panda_link_name& name) const {
    return find_id_by_name(std::string_view{name()});
}

std::optional<panda_link_metadata>
panda_link_table::find_by_id(panda_link_id id) const {
    auto it = _underlying.find(id);
    if (it == _underlying.end()) {
        return std::nullopt;
    }
    return it->second;
}

void panda_link_table::upsert_link(panda_link_id id, panda_link_metadata meta) {
    auto it = _name_index.find(std::string_view(meta.name()));
    if (it != _name_index.end()) {
        if (it->second != id) {
            throw std::logic_error(ss::format(
              "Panda link id={} is attempting to use a name {} which is "
              "already registered to {}",
              id,
              meta.name,
              it->second));
        }
    } else {
        _name_index.emplace(meta.name, id);
    }
    _underlying.insert_or_assign(id, std::move(meta));
    run_callbacks(id);
}

void panda_link_table::remove_link(const panda_link_name& name) {
    auto name_it = _name_index.find(std::string_view{name()});
    if (name_it == _name_index.end()) {
        return;
    }

    auto id = name_it->second;
    auto it = _underlying.find(id);

    vassert(
      it != _underlying.end(),
      "Inconsistent name index for {} expected id {}",
      name,
      id);

    _name_index.erase(name_it);
    _underlying.erase(it);
    run_callbacks(id);
}

panda_link_table::notification_id
panda_link_table::register_for_updates(notification_callback cb) {
    auto it = _callbacks.insert({++_latest_id, std::move(cb)});
    vassert(it.second, "Invalid duplicate in callbacks");
    return _latest_id;
}

void panda_link_table::unregister_for_updates(notification_id id) {
    _callbacks.erase(id);
}

void panda_link_table::run_callbacks(panda_link_id id) {
    for (const auto& [_, cb] : _callbacks) {
        cb(id);
    }
}

bool panda_link_table::name_less_cmp::operator()(
  const panda_link_name& lhs, const panda_link_name& rhs) const {
    return lhs < rhs;
}

bool panda_link_table::name_less_cmp::operator()(
  const panda_link_name& lhs, std::string_view rhs) const {
    return lhs() < rhs;
}

bool panda_link_table::name_less_cmp::operator()(
  std::string_view lhs, const panda_link_name& rhs) const {
    return lhs < rhs();
}
} // namespace cluster
