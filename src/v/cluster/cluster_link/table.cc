/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/cluster_link/table.h"

#include "base/vassert.h"
#include "cluster/cluster_link/table_utils.h"
#include "cluster/commands.h"
#include "cluster/errc.h"
#include "cluster/logger.h"
#include "cluster_link/model/types.h"

namespace cluster::cluster_link {

using ::cluster_link::model::add_mirror_topic_cmd;
using ::cluster_link::model::delete_mirror_topic_cmd;
using ::cluster_link::model::id_t;
using ::cluster_link::model::metadata;
using ::cluster_link::model::metadata_ptr;
using ::cluster_link::model::mirror_topic_status;
using ::cluster_link::model::name_t;
using ::cluster_link::model::update_cluster_link_configuration_cmd;
using ::cluster_link::model::update_mirror_topic_status_cmd;

namespace {
static constexpr auto accepted_commands = cluster::make_commands_list<
  cluster::cluster_link_upsert_cmd,
  cluster::cluster_link_remove_cmd,
  cluster::cluster_link_add_mirror_topic_cmd,
  cluster::cluster_link_delete_mirror_topic_cmd,
  cluster::cluster_link_update_mirror_topic_status_cmd,
  cluster::cluster_link_update_mirror_topic_properties_cmd,
  cluster::cluster_link_update_cluster_link_configuration_cmd>();

table::link_revision_index_t
copy_revisions(const table::link_revision_index_t& revisions) {
    table::link_revision_index_t copy;
    copy.reserve(revisions.size());
    for (const auto& [id, rev] : revisions) {
        copy.emplace(id, rev);
    }
    return copy;
}
} // namespace

table::link_revision_index_t table::all_link_revisions() const {
    return copy_revisions(_link_revision_index);
}

size_t table::size() const { return _link_metadata.size(); }

void table::reset_links(
  map_t links,
  link_revision_index_t link_to_revision,
  model::revision_id snapshot_revision) {
    name_index_t snap_name_index;
    topic_name_index_t snap_topic_name_index;

    chunked_vector<id_t> all_deletes;
    chunked_vector<id_t> all_inserts;
    chunked_vector<id_t> all_changes;

    for (const auto& [k, v] : _link_metadata) {
        auto it = links.find(k);
        if (it == links.end()) {
            all_deletes.push_back(k);
        } else if (v != it->second) {
            all_changes.push_back(k);
        }
    }

    for (const auto& [id, metadata] : links) {
        if (!_link_metadata.contains(id)) {
            all_inserts.push_back(id);
        }
        if (!link_to_revision.contains(id)) {
            // this is unexpected but we move on by using the snapshot revision.
            vlog(
              cluster::clusterlog.error,
              "Link id {} from snapshot is missing a revision, setting to {}",
              id,
              snapshot_revision);
            link_to_revision.emplace(id, snapshot_revision);
        }
        auto it = snap_name_index.emplace(metadata->name, id);
        if (!it.second) {
            throw std::logic_error(
              fmt::format(
                "cluster link id={} is attempting to use a name {} which is "
                "already registered to {}",
                id,
                metadata->name,
                it.first->second));
        }
        for (const auto& t : metadata->state.mirror_topics) {
            auto topic_it = snap_topic_name_index.emplace(t.first, id);
            if (!topic_it.second) {
                throw std::logic_error(
                  fmt::format(
                    "cluster link id={} is attempting to use a topic {} which "
                    "is "
                    "already registered to {}",
                    id,
                    t.first,
                    topic_it.first->second));
            }
        }
    }

    _link_metadata = std::move(links);
    _link_revision_index = std::move(link_to_revision);
    vassert(
      _link_metadata.size() == _link_revision_index.size(),
      "Inconsistent cluster link and revision index sizes, have {} links and "
      "{} revisions",
      _link_metadata.size(),
      _link_revision_index.size());
    _name_index = std::move(snap_name_index);
    _topic_name_index = std::move(snap_topic_name_index);

    auto get_revision = [this, &snapshot_revision](id_t id) {
        auto it = _link_revision_index.find(id);
        if (it != _link_revision_index.end()) {
            return it->second;
        }
        return snapshot_revision;
    };

    for (const auto& deleted : all_deletes) {
        run_callbacks(deleted, get_revision(deleted));
    }
    for (const auto& inserted : all_inserts) {
        run_callbacks(inserted, get_revision(inserted));
    }
    for (const auto& changed : all_changes) {
        run_callbacks(changed, get_revision(changed));
    }
}
metadata_ptr table::find_link_by_name(const name_t& name) const {
    auto id = find_id_by_name(name);
    if (!id) {
        return nullptr;
    }
    auto meta = find_link_by_id(id.value());
    vassert(
      meta != nullptr,
      "Inconsistent name index for {} expected id {}",
      name,
      id.value());

    return meta;
}

metadata_ptr table::find_link_by_id(id_t id) const {
    auto it = _link_metadata.find(id);
    if (it == _link_metadata.end()) {
        return nullptr;
    }
    return it->second;
}

std::optional<id_t> table::find_id_by_name(const name_t& name) const {
    auto it = _name_index.find(name);
    if (it == _name_index.end()) {
        return std::nullopt;
    }
    return it->second;
}

std::optional<id_t> table::find_id_by_topic(model::topic_view tp) const {
    auto it = _topic_name_index.find(tp);
    if (it == _topic_name_index.end()) {
        return std::nullopt;
    }
    return it->second;
}

std::optional<mirror_topic_status>
table::find_mirror_topic_status(model::topic_view tp) const {
    auto id = find_id_by_topic(tp);
    if (!id) {
        return std::nullopt;
    }
    auto meta = find_link_by_id(id.value());
    vassert(
      meta != nullptr,
      "Inconsistent topic index for {} expected id {}",
      tp,
      id.value());

    auto it = meta->state.mirror_topics.find(tp);
    vassert(
      it != meta->state.mirror_topics.end(),
      "Inconsistent topic index for {} expected to exist in metadata id {}",
      tp,
      id.value());
    return it->second.status;
}

std::optional<::model::revision_id> table::get_link_last_update_revision(
  const ::cluster_link::model::id_t& id) const {
    auto it = _link_revision_index.find(id);
    if (it == _link_revision_index.end()) {
        return std::nullopt;
    }
    return it->second;
}

chunked_vector<id_t> table::get_all_link_ids() const {
    auto keys = _link_metadata | std::views::transform([](const auto& pair) {
                    return pair.first;
                });

    return {keys.begin(), keys.end()};
}

bool table::is_batch_applicable(const model::record_batch& b) const {
    return b.header().type == model::record_batch_type::cluster_link;
}

ss::future<std::error_code> table::apply_update(model::record_batch b) {
    auto offset = b.base_offset();
    auto revision = model::revision_id(b.last_offset());
    auto cmd = co_await deserialize(std::move(b), accepted_commands);
    auto results = co_await container().map([cmd = std::move(cmd),
                                             offset,
                                             revision](table& table) mutable {
        return ss::visit(
          cmd,
          [&table, offset, revision](
            const cluster::cluster_link_upsert_cmd& upsert)
            -> ss::future<cluster::cluster_link::errc> {
              auto existing_id = table.find_id_by_name(upsert.value.name);
              return table.upsert_link(
                existing_id.value_or(id_t{offset}), upsert.value, revision);
          },
          [&table, revision](const cluster::cluster_link_remove_cmd& remove)
            -> ss::future<cluster::cluster_link::errc> {
              return ss::make_ready_future<cluster::cluster_link::errc>(
                table.remove_link(remove.value.link_name, revision));
          },
          [&table,
           revision](const cluster::cluster_link_add_mirror_topic_cmd& add)
            -> ss::future<cluster::cluster_link::errc> {
              return table.add_mirror_topic(add.key, add.value, revision);
          },
          [&table, revision](
            const cluster::cluster_link_update_mirror_topic_status_cmd& state) {
              return table.update_mirror_topic_state(
                state.key, state.value, revision);
          },
          [&table, revision](
            const cluster::cluster_link_update_mirror_topic_properties_cmd&
              state) {
              return table.update_mirror_topic_properties(
                state.key, state.value, revision);
          },
          [&table,
           revision](const cluster::cluster_link_delete_mirror_topic_cmd& cmd) {
              return table.delete_mirror_topic(cmd.key, cmd.value, revision);
          },
          [&table, revision](
            const cluster::cluster_link_update_cluster_link_configuration_cmd&
              cmd) {
              return table.update_cluster_link_configuration(
                cmd.key, cmd.value, revision);
          });
    });
    auto first_res = results.front();
    auto state_consistent = std::ranges::all_of(
      results, [first_res](cluster::cluster_link::errc res) {
          return first_res == res;
      });

    vassert(
      state_consistent,
      "State inconsistency across shards detected, expected result: {}, "
      "have: {}",
      first_res,
      results);

    co_return first_res;
}

ss::future<> table::fill_snapshot(cluster::controller_snapshot& snap) const {
    // Make a local shallow copy of the links so we don't run into any iterator
    // issues while doing the async deep copy links
    map_t links;
    links.reserve(_link_metadata.size());
    for (const auto& [id, md_ptr] : _link_metadata) {
        links.emplace(id, md_ptr);
    }
    snap.cluster_links.links = co_await copy_links_for_snapshot(
      std::move(links));
    snap.cluster_links.link_revisions = all_link_revisions();
}

ss::future<> table::apply_snapshot(
  model::offset offset, const cluster::controller_snapshot& snap) {
    return container().invoke_on_all(
      [&snap, snap_revision = model::revision_id(offset)](
        table& table) -> ss::future<> {
          return copy_links_from_snapshot(snap.cluster_links.links)
            .then([&snap, snap_revision, &table](table::map_t new_links) {
                table.reset_links(
                  std::move(new_links),
                  copy_revisions(snap.cluster_links.link_revisions),
                  snap_revision);
            });
      });
}

table::notification_id table::register_for_updates(notification_callback cb) {
    auto it = _callbacks.insert({++_latest_id, std::move(cb)});
    vassert(it.second, "Invalid duplicate in callbacks");
    return _latest_id;
}

void table::unregister_for_updates(notification_id id) { _callbacks.erase(id); }

bool table::cluster_link_active() const {
    // TODO: Update when we have proper states for cluster links
    return !_link_metadata.empty();
}

void table::run_callbacks(id_t id, model::revision_id revision) {
    for (const auto& [_, cb] : _callbacks) {
        cb(id, revision);
    }
}

ss::future<cluster::cluster_link::errc>
table::upsert_link(id_t id, const metadata& meta, model::revision_id revision) {
    for (const auto& t : meta.state.mirror_topics) {
        auto link_id = find_id_by_topic(t.first);
        if (link_id && link_id.value() != id) {
            vlog(
              cluster::clusterlog.info,
              "Unable to upsert link {} ({}) as topic {} is already registered "
              "to link {}",
              meta.name,
              id,
              t.first,
              link_id.value());
            co_return errc::topic_being_mirrored_by_other_link;
        }
        _topic_name_index.emplace(t.first, id);
    }
    auto name_it = _name_index.find(meta.name);
    if (name_it != _name_index.end()) {
        if (name_it->second != id) {
            vlog(
              cluster::clusterlog.error,
              "cluster link id={} is attempting to use a name {} which is "
              "already registered to {}",
              id,
              meta.name,
              name_it->second);
            co_return cluster::cluster_link::errc::service_error;
        }
    } else {
        _name_index.emplace(meta.name, id);
    }

    // Reconcile if topics have been removed
    std::erase_if(_topic_name_index, [id, &meta](const auto& it) {
        return it.second == id && !meta.state.mirror_topics.contains(it.first);
    });

    auto meta_ptr = ss::make_lw_shared<metadata>(co_await meta.copy());

    _link_metadata.insert_or_assign(id, std::move(meta_ptr));
    _link_revision_index[id] = revision;
    run_callbacks(id, revision);
    co_return cluster::cluster_link::errc::success;
}

cluster::cluster_link::errc
table::remove_link(const name_t& name, model::revision_id revision) {
    auto name_it = _name_index.find(name);
    if (name_it == _name_index.end()) {
        return cluster::cluster_link::errc::success;
    }

    auto id = name_it->second;

    std::erase_if(
      _topic_name_index, [id](const auto& it) { return it.second == id; });

    auto it = _link_metadata.find(id);
    vassert(
      it != _link_metadata.end(),
      "Inconsistent name index for {} expected id {}",
      name,
      id);
    _name_index.erase(name_it);
    _link_revision_index.erase(id);
    _link_metadata.erase(it);

    run_callbacks(id, revision);
    return cluster::cluster_link::errc::success;
}

ss::future<cluster::cluster_link::errc> table::add_mirror_topic(
  id_t id, const add_mirror_topic_cmd& cmd, model::revision_id revision) {
    auto link_id = find_id_by_topic(cmd.topic);
    if (link_id) {
        if (link_id.value() != id) {
            vlog(
              cluster::clusterlog.info,
              "Unable to add mirror topic {} to link {} as it is already "
              "registered to link {}",
              cmd.topic,
              id,
              link_id.value());
            co_return errc::topic_being_mirrored_by_other_link;
        }
        vlog(
          cluster::clusterlog.info,
          "Link {} is already mirroring topic {}",
          id,
          cmd.topic);
        co_return errc::topic_already_being_mirrored;
    }

    if (!find_link_by_id(id)) {
        vlog(cluster::clusterlog.info, "Link {} not found", id);
        co_return errc::does_not_exist;
    }

    auto link_metadata = ss::make_lw_shared<metadata>(
      co_await _link_metadata[id]->copy());

    link_metadata->state.mirror_topics.insert({cmd.topic, cmd.metadata.copy()});
    _link_metadata[id] = std::move(link_metadata);
    _topic_name_index.emplace(cmd.topic, id);
    _link_revision_index[id] = revision;
    run_callbacks(id, revision);
    co_return errc::success;
}

ss::future<cluster::cluster_link::errc> table::update_mirror_topic_state(
  id_t id,
  const update_mirror_topic_status_cmd& cmd,
  model::revision_id revision) {
    auto link_id = find_id_by_topic(cmd.topic);
    if (!link_id) {
        vlog(
          cluster::clusterlog.info,
          "Unable to update mirror topic {} state as it is not registered",
          cmd.topic);
        co_return errc::topic_not_being_mirrored;
    } else if (link_id.value() != id) {
        vlog(
          cluster::clusterlog.info,
          "Unable to update mirror topic {} state as it is registered to "
          "link {}",
          cmd.topic,
          link_id.value());
        co_return errc::topic_being_mirrored_by_other_link;
    }

    auto link_meta = _link_metadata[link_id.value()];
    auto it = link_meta->state.mirror_topics.find(cmd.topic);
    vassert(
      it != link_meta->state.mirror_topics.end(),
      "Inconsistent topic index for {} expected to exist in metadata id {}",
      cmd.topic,
      id);

    auto link_metadata = ss::make_lw_shared<metadata>(
      co_await link_meta->copy());
    link_metadata->state.mirror_topics[cmd.topic].status = cmd.status;
    _link_metadata[id] = std::move(link_metadata);

    _link_revision_index[id] = revision;
    run_callbacks(id, revision);
    co_return errc::success;
}

ss::future<cluster::cluster_link::errc> table::update_mirror_topic_properties(
  ::cluster_link::model::id_t id,
  const ::cluster_link::model::update_mirror_topic_properties_cmd& cmd,
  model::revision_id revision) {
    auto link_id = find_id_by_topic(cmd.topic);
    if (!link_id) {
        vlog(
          cluster::clusterlog.warn,
          "Unable to update mirror topic {} properties as it is not registered",
          cmd.topic);
        co_return errc::topic_not_being_mirrored;
    } else if (link_id.value() != id) {
        vlog(
          cluster::clusterlog.warn,
          "Unable to update mirror topic {} properties as it is registered to "
          "link {}",
          cmd.topic,
          link_id.value());
        co_return errc::topic_being_mirrored_by_other_link;
    }

    auto link_meta = _link_metadata[link_id.value()];
    auto it = link_meta->state.mirror_topics.find(cmd.topic);
    vassert(
      it != link_meta->state.mirror_topics.end(),
      "Inconsistent topic index for {} expected to exist in metadata id {}",
      cmd.topic,
      id);

    auto link_metadata = ss::make_lw_shared<metadata>(
      co_await link_meta->copy());

    auto& md = link_metadata->state.mirror_topics.find(cmd.topic)->second;
    md.partition_count = cmd.partition_count;
    md.replication_factor = cmd.replication_factor;
    md.topic_configs.clear();
    md.topic_configs.reserve(cmd.topic_configs.size());
    // Copy the topic configs from the command
    for (const auto& [key, value] : cmd.topic_configs) {
        md.topic_configs.emplace(key, value);
    }
    _link_metadata[id] = std::move(link_metadata);
    _link_revision_index[id] = revision;
    run_callbacks(id, revision);
    co_return errc::success;
}

ss::future<cluster::cluster_link::errc> table::delete_mirror_topic(
  id_t id, const delete_mirror_topic_cmd& cmd, model::revision_id revision) {
    auto link_id = find_id_by_topic(cmd.topic);
    if (!link_id.has_value()) {
        vlog(
          cluster::clusterlog.info,
          "Unable to delete mirror topic {} from link {}. Topic not found",
          cmd.topic,
          id);
        co_return errc::topic_not_being_mirrored;
    }
    if (link_id.value() != id) {
        vlog(
          cluster::clusterlog.info,
          "Unable to delete mirror topic {} from link {}. It is registered to "
          "link {}",
          cmd.topic,
          id,
          link_id.value());
        co_return errc::topic_being_mirrored_by_other_link;
    }

    if (!find_link_by_id(id)) {
        vlog(cluster::clusterlog.info, "Link {} not found", id);
        co_return errc::does_not_exist;
    }

    auto link_metadata = ss::make_lw_shared<metadata>(
      co_await _link_metadata[id]->copy());
    link_metadata->state.mirror_topics.erase(cmd.topic);

    _link_metadata[id] = std::move(link_metadata);

    _topic_name_index.erase(cmd.topic);
    _link_revision_index[id] = revision;
    run_callbacks(id, revision);
    co_return errc::success;
}

ss::future<cluster::cluster_link::errc>
table::update_cluster_link_configuration(
  id_t id,
  const update_cluster_link_configuration_cmd& cmd,
  model::revision_id revision) {
    if (!_link_metadata.contains(id)) {
        vlog(cluster::clusterlog.warn, "Link id {} not found", id);
        co_return errc::does_not_exist;
    }

    auto link_metadata = ss::make_lw_shared<metadata>(
      co_await _link_metadata[id]->copy());

    link_metadata->connection = cmd.connection;
    link_metadata->configuration = cmd.link_config.copy();

    _link_metadata[id] = std::move(link_metadata);
    _link_revision_index[id] = revision;

    run_callbacks(id, revision);
    co_return errc::success;
}
} // namespace cluster::cluster_link
