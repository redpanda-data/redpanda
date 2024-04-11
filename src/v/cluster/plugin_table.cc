/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "plugin_table.h"

#include "base/vassert.h"
#include "cluster/types.h"
#include "model/metadata.h"
#include "model/transform.h"

namespace cluster {

using model::transform_id;
using model::transform_metadata;
using model::transform_name;

plugin_table::map_t plugin_table::all_transforms() const {
    map_t all;
    for (const auto& entry : _underlying) {
        all.emplace(entry.first, entry.second);
    }
    return all;
}

size_t plugin_table::size() const { return _underlying.size(); }

std::optional<transform_id>
plugin_table::find_id_by_name(std::string_view name) const {
    auto it = _name_index.find(name);
    if (it == _name_index.end()) {
        return std::nullopt;
    }
    return it->second;
}

std::optional<transform_id>
plugin_table::find_id_by_name(const transform_name& name) const {
    return find_id_by_name(std::string_view(name()));
}

std::optional<transform_metadata>
plugin_table::find_by_name(const transform_name& name) const {
    return find_by_name(std::string_view(name()));
}

std::optional<transform_metadata>
plugin_table::find_by_name(std::string_view name) const {
    auto id = find_id_by_name(name);
    if (!id.has_value()) {
        return std::nullopt;
    }
    auto meta = find_by_id(id.value());
    vassert(
      meta.has_value(),
      "inconsistent name index for {} expected id {}",
      name,
      id);
    return meta;
}

std::optional<transform_metadata>
plugin_table::find_by_id(transform_id id) const {
    auto it = _underlying.find(id);
    if (it == _underlying.end()) {
        return std::nullopt;
    }
    return it->second;
}

plugin_table::map_t
plugin_table::find_by_input_topic(model::topic_namespace_view tn) const {
    return find_by_topic(_input_topic_index, tn);
}

plugin_table::map_t
plugin_table::find_by_output_topic(model::topic_namespace_view tn) const {
    return find_by_topic(_output_topic_index, tn);
}

plugin_table::map_t plugin_table::find_by_topic(
  const topic_index_t& index, model::topic_namespace_view tn) const {
    auto it = index.find(tn);
    if (it == index.end()) {
        return {};
    }
    map_t output;
    for (auto id : it->second) {
        auto meta = find_by_id(id);
        vassert(
          meta.has_value(),
          "inconsistent topic index for {} expected id {}",
          tn,
          id);
        output.emplace(id, *meta);
    }
    return output;
}

void plugin_table::upsert_transform(transform_id id, transform_metadata meta) {
    auto it = _name_index.find(std::string_view(meta.name()));
    if (it != _name_index.end()) {
        if (it->second != id) {
            throw std::logic_error(ss::format(
              "transform meta id={} is attempting to use a name {} which is "
              "already registered to {}",
              id,
              meta.name,
              it->second));
        }
    } else {
        _name_index.emplace(meta.name, id);
    }
    // Topics cannot change over the lifetime of a transform, so we don't need
    // to worry about updates specially here, additionally duplicates are
    // allowed, the plugin_frontend asserts that there are no loops in
    // transforms.
    _input_topic_index[meta.input_topic].emplace(id);

    for (const auto& output_topic : meta.output_topics) {
        _output_topic_index[output_topic].emplace(id);
    }
    _underlying.insert_or_assign(id, std::move(meta));
    run_callbacks(id);
}

void plugin_table::remove_transform(const transform_name& name) {
    auto name_it = _name_index.find(std::string_view(name()));
    if (name_it == _name_index.end()) {
        return;
    }
    auto id = name_it->second;
    auto it = _underlying.find(id);
    vassert(
      it != _underlying.end(),
      "inconsistency: name index index had a record with name: {} id: {}",
      name,
      id);
    const auto& meta = it->second;
    // Delete input topic index entries
    [this, &meta, id]() {
        auto input_it = _input_topic_index.find(meta.input_topic);
        vassert(
          input_it != _input_topic_index.end()
            && input_it->second.erase(id) > 0,
          "inconsistency: missing transform input index entry: {} id: {}",
          meta.input_topic,
          id);
        if (input_it->second.empty()) {
            _input_topic_index.erase(input_it);
        }
    }();
    for (const auto& output_topic : it->second.output_topics) {
        auto output_it = _output_topic_index.find(output_topic);
        vassert(
          output_it != _output_topic_index.end()
            && output_it->second.erase(id) > 0,
          "inconsistency: missing transform output index entry: {} id: {}",
          output_topic,
          id);
        if (output_it->second.empty()) {
            _output_topic_index.erase(output_it);
        }
    }
    _name_index.erase(name_it);
    _underlying.erase(it);
    run_callbacks(id);
}

void plugin_table::reset_transforms(plugin_table::map_t snap) {
    name_index_t snap_name_index;
    topic_index_t snap_input_index;
    topic_index_t snap_output_index;

    // Perform a map diff to figure out which transforms need change callbacks
    // fired.
    ss::chunked_fifo<transform_id> all_deletes;
    ss::chunked_fifo<transform_id> all_changed;
    ss::chunked_fifo<transform_id> all_inserted;
    for (const auto& [k, v] : _underlying) {
        auto it = snap.find(k);
        if (it == snap.end()) {
            all_deletes.push_back(k);
        } else if (v != it->second) {
            all_changed.push_back(k);
        }
        // Otherwise unchanged
    }
    for (const auto& [k, v] : snap) {
        if (!_underlying.contains(k)) {
            all_inserted.push_back(k);
        }
        // build the name index
        auto it = snap_name_index.insert({v.name, k});
        if (!it.second) {
            throw std::logic_error(ss::format(
              "transform meta id={} is attempting to use a name {} which is "
              "already registered to {}",
              k,
              v.name,
              it.first->first));
        }
        // build topic indexes
        snap_input_index[v.input_topic].emplace(k);
        for (const auto& output_topic : v.output_topics) {
            snap_output_index[output_topic].emplace(k);
        }
    }

    // Do the actual swap
    _underlying = std::move(snap);
    _name_index = std::move(snap_name_index);
    _input_topic_index = std::move(snap_input_index);
    _output_topic_index = std::move(snap_output_index);

    for (transform_id deleted : all_deletes) {
        run_callbacks(deleted);
    }
    for (transform_id updated : all_changed) {
        run_callbacks(updated);
    }
    for (transform_id inserted : all_inserted) {
        run_callbacks(inserted);
    }
}

plugin_table::notification_id
plugin_table::register_for_updates(plugin_table::notification_callback cb) {
    auto it = _callbacks.insert({++_latest_id, std::move(cb)});
    vassert(it.second, "invalid duplicate in callbacks");
    return _latest_id;
}

void plugin_table::unregister_for_updates(notification_id id) {
    _callbacks.erase(id);
}

void plugin_table::run_callbacks(transform_id id) {
    for (const auto& [_, cb] : _callbacks) {
        cb(id);
    }
}

bool plugin_table::name_less_cmp::operator()(
  const model::transform_name& lhs, const std::string_view& rhs) const {
    return lhs() < rhs;
}

bool plugin_table::name_less_cmp::operator()(
  const std::string_view& lhs, const model::transform_name& rhs) const {
    return lhs < rhs();
}

bool plugin_table::name_less_cmp::operator()(
  const model::transform_name& lhs, const model::transform_name& rhs) const {
    return lhs < rhs;
}

bool plugin_table::topic_map_less_cmp::operator()(
  const model::topic_namespace& lhs, const model::topic_namespace& rhs) const {
    return lhs < rhs;
}

bool plugin_table::topic_map_less_cmp::operator()(
  const model::topic_namespace_view& lhs,
  const model::topic_namespace_view& rhs) const {
    return lhs < rhs;
}

} // namespace cluster
