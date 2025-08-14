/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/metastore/simple_metastore.h"

#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/metastore/state.h"
#include "cloud_topics/level_one/metastore/state_update.h"
#include "cloud_topics/logger.h"

#include <seastar/core/coroutine.hh>

namespace experimental::cloud_topics::l1 {

object_id simple_object_builder::get_or_create_object_for(
  const model::topic_id_partition&) {
    // The simple metastore isn't partitioned at all, so have all partitions
    // blindly share any existing object.
    if (pending_objects_.empty()) {
        auto oid = create_object_id();
        pending_objects_[oid] = {};
        return oid;
    }
    return pending_objects_.begin()->first;
}

std::expected<void, metastore::object_metadata_builder::error>
simple_object_builder::add(
  object_id oid, metastore::object_metadata::ntp_metadata ntp_meta) {
    auto it = pending_objects_.find(oid);
    if (it == pending_objects_.end()) {
        return std::unexpected(
          error{fmt::format("Object {} is not a pending object", oid)});
    }
    auto& pending_metas = it->second;
    pending_metas.emplace_back(ntp_meta);
    return {};
}

std::expected<void, metastore::object_metadata_builder::error>
simple_object_builder::finish(object_id oid, size_t footer_pos) {
    auto it = pending_objects_.find(oid);
    if (it == pending_objects_.end()) {
        return std::unexpected(
          error{fmt::format("Object {} is not a pending object", oid)});
    }
    finished_objects_.emplace_back(metastore::object_metadata{
      .oid = oid,
      .footer_pos = footer_pos,
      .ntp_metas = std::move(it->second),
    });
    pending_objects_.erase(it);
    return {};
}

std::expected<
  chunked_vector<metastore::object_metadata>,
  metastore::object_metadata_builder::error>
simple_object_builder::release() {
    if (!pending_objects_.empty()) {
        return std::unexpected(error{fmt::format(
          "Builder still has {} pending object", pending_objects_.size())});
    }
    return std::exchange(finished_objects_, {});
}

new_object make_new_object(const metastore::object_metadata& o) {
    new_object new_o{
      .oid = o.oid,
      .footer_pos = o.footer_pos,
    };
    for (const auto& c : o.ntp_metas) {
        auto& extents = new_o.extent_metas[c.tidp.topic_id];
        extents[c.tidp.partition] = new_object::metadata{
          .base_offset = c.base_offset,
          .last_offset = c.last_offset,
          .max_timestamp = c.max_timestamp,
          .filepos = c.pos,
          .len = c.size,
        };
    }
    return new_o;
}

std::unique_ptr<metastore::object_metadata_builder>
simple_metastore::object_builder() {
    return std::make_unique<simple_object_builder>();
}

ss::future<std::expected<metastore::offsets_response, metastore::errc>>
simple_metastore::get_offsets(const model::topic_id_partition& tpr) {
    co_return get_offsets(state_, tpr);
}

std::expected<metastore::offsets_response, metastore::errc>
simple_metastore::get_offsets(
  const state& state, const model::topic_id_partition& tpr) {
    auto prt_ref = state.partition_state(tpr);
    if (!prt_ref.has_value()) {
        vlog(cd_log.debug, "Partition {} not tracked", tpr);
        return std::unexpected(metastore::errc::missing_ntp);
    }
    const auto& prt = prt_ref->get();
    return offsets_response{
      .start_offset = prt.start_offset,
      .next_offset = prt.next_offset,
    };
}

ss::future<std::expected<metastore::add_response, metastore::errc>>
simple_metastore::add_objects(
  std::unique_ptr<metastore::object_metadata_builder> builder) {
    auto* simple_builder = dynamic_cast<simple_object_builder*>(builder.get());
    auto objects_res = simple_builder->release();
    if (!objects_res.has_value()) {
        vlog(cd_log.error, "Failed to add: {}", objects_res.error());
        co_return std::unexpected(metastore::errc::invalid_request);
    }
    co_return co_await add_objects(objects_res.value());
}

ss::future<std::expected<metastore::add_response, metastore::errc>>
simple_metastore::add_objects(const chunked_vector<object_metadata>& objects) {
    chunked_vector<new_object> new_objects;
    for (const auto& o : objects) {
        new_objects.emplace_back(make_new_object(o));
    }
    add_response resp;
    auto update_res = add_objects_update::build(
      state_, std::move(new_objects), &resp.corrected_next_offsets);
    if (!update_res.has_value()) {
        vlog(cd_log.debug, "Object add failed: {}", update_res.error());
        co_return std::unexpected(metastore::errc::invalid_request);
    }
    auto apply_res = update_res->apply(state_);
    vassert(
      apply_res.has_value(),
      "Apply must succeed if can_apply() is true: {}",
      apply_res.error());
    co_return resp;
}

ss::future<std::expected<void, metastore::errc>>
simple_metastore::replace_objects(
  std::unique_ptr<metastore::object_metadata_builder> builder) {
    auto* simple_builder = dynamic_cast<simple_object_builder*>(builder.get());
    auto objects_res = simple_builder->release();
    if (!objects_res.has_value()) {
        vlog(cd_log.error, "Failed to replace: {}", objects_res.error());
        co_return std::unexpected(metastore::errc::invalid_request);
    }
    co_return co_await replace_objects(objects_res.value());
}

ss::future<std::expected<void, metastore::errc>>
simple_metastore::replace_objects(
  const chunked_vector<object_metadata>& objects) {
    chunked_vector<new_object> new_objects;
    for (const auto& o : objects) {
        new_objects.emplace_back(make_new_object(o));
    }
    auto update_res = replace_objects_update::build(
      state_, std::move(new_objects));
    if (!update_res.has_value()) {
        vlog(cd_log.debug, "Object replacement failed: {}", update_res.error());
        co_return std::unexpected(metastore::errc::invalid_request);
    }
    auto apply_res = update_res->apply(state_);
    vassert(apply_res.has_value(), "Apply must succeed if can_apply() is true");
    co_return std::expected<void, metastore::errc>{};
}

ss::future<std::expected<metastore::object_response, metastore::errc>>
simple_metastore::get_first_ge(
  const model::topic_id_partition& tpr, kafka::offset o) {
    co_return get_first_ge(state_, tpr, o);
}

std::expected<metastore::object_response, metastore::errc>
simple_metastore::get_first_ge(
  const state& state, const model::topic_id_partition& tpr, kafka::offset o) {
    auto prt_ref = state.partition_state(tpr);
    if (!prt_ref.has_value()) {
        vlog(cd_log.debug, "Partition {} not tracked", tpr);
        return std::unexpected(metastore::errc::missing_ntp);
    }
    auto& prt = prt_ref->get();
    auto it = std::ranges::lower_bound(
      prt.extents, o, std::less<>{}, &extent::last_offset);
    if (it != prt.extents.end()) {
        auto object_it = state.objects.find(it->oid);
        if (object_it == state.objects.end()) {
            return std::unexpected(metastore::errc::out_of_range);
        }
        auto footer_pos = object_it->second.footer_pos;
        return metastore::object_response{
          .oid = it->oid,
          .footer_pos = footer_pos,
        };
    }
    return std::unexpected(metastore::errc::out_of_range);
}

ss::future<std::expected<metastore::object_response, metastore::errc>>
simple_metastore::get_first_ge(
  const model::topic_id_partition& tpr, model::timestamp ts) {
    co_return get_first_ge(state_, tpr, ts);
}

std::expected<metastore::object_response, metastore::errc>
simple_metastore::get_first_ge(
  const state& state,
  const model::topic_id_partition& tpr,
  model::timestamp ts) {
    auto prt_ref = state.partition_state(tpr);
    if (!prt_ref.has_value()) {
        vlog(cd_log.debug, "Partition {} not tracked", tpr);
        return std::unexpected(metastore::errc::missing_ntp);
    }
    auto& prt = prt_ref->get();
    for (const auto& obj : prt.extents) {
        if (obj.max_timestamp >= ts) {
            auto object_it = state.objects.find(obj.oid);
            if (object_it == state.objects.end()) {
                return std::unexpected(metastore::errc::out_of_range);
            }
            auto footer_pos = object_it->second.footer_pos;
            return metastore::object_response{
              .oid = obj.oid,
              .footer_pos = footer_pos,
            };
        }
    }
    return std::unexpected(metastore::errc::out_of_range);
}

ss::future<std::expected<void, metastore::errc>>
simple_metastore::compact_objects(
  const chunked_vector<object_metadata>& objects,
  const compaction_map_t& compaction_metas) {
    chunked_vector<new_object> new_objects;
    for (const auto& o : objects) {
        new_objects.emplace_back(make_new_object(o));
    }
    chunked_hash_map<model::topic_id_partition, compaction_state_update>
      compaction_updates;
    for (const auto& [tp, cm] : compaction_metas) {
        compaction_state_update p_update;
        if (cm.new_cleaned_range.has_value()) {
            p_update.new_cleaned_range.emplace(
              compaction_state_update::cleaned_range{
                .base_offset = cm.new_cleaned_range->base_offset,
                .last_offset = cm.new_cleaned_range->last_offset,
                .has_tombstones = cm.new_cleaned_range->has_tombstones,
              });
        }
        p_update.removed_tombstones_ranges = cm.removed_tombstones_ranges;
        p_update.cleaned_at = cm.cleaned_at;
        compaction_updates[tp] = std::move(p_update);
    }

    auto update_res = replace_objects_update::build(
      state_, std::move(new_objects), std::move(compaction_updates));
    if (!update_res.has_value()) {
        vlog(cd_log.debug, "Object replacement failed: {}", update_res.error());
        co_return std::unexpected(metastore::errc::invalid_request);
    }
    auto apply_res = update_res->apply(state_);
    vassert(apply_res.has_value(), "Apply must succeed if can_apply() is true");
    co_return std::expected<void, metastore::errc>{};
}

ss::future<std::expected<void, metastore::errc>>
simple_metastore::compact_objects(
  std::unique_ptr<metastore::object_metadata_builder> builder,
  const compaction_map_t& compaction_metas) {
    auto* simple_builder = dynamic_cast<simple_object_builder*>(builder.get());
    auto objects_res = simple_builder->release();
    if (!objects_res.has_value()) {
        vlog(cd_log.error, "Failed to compact: {}", objects_res.error());
        co_return std::unexpected(metastore::errc::invalid_request);
    }
    co_return co_await compact_objects(objects_res.value(), compaction_metas);
}

ss::future<
  std::expected<metastore::compaction_offsets_response, metastore::errc>>
simple_metastore::get_compaction_offsets(
  const model::topic_id_partition& tp,
  model::timestamp tombstone_removal_upper_bound_ts) {
    co_return get_compaction_offsets(
      state_, tp, tombstone_removal_upper_bound_ts);
}
std::expected<metastore::compaction_offsets_response, metastore::errc>
simple_metastore::get_compaction_offsets(
  const state& state,
  const model::topic_id_partition& tp,
  model::timestamp tombstone_removal_upper_bound_ts) {
    auto prt_ref = state.partition_state(tp);
    if (!prt_ref.has_value()) {
        vlog(cd_log.debug, "Partition {} not tracked", tp);
        return std::unexpected(metastore::errc::missing_ntp);
    }
    auto& prt = prt_ref->get();
    compaction_offsets_response resp;
    if (prt.start_offset >= prt.next_offset) {
        // The log is empty, nothing to compact.
        return resp;
    }
    if (!prt.compaction_state.has_value()) {
        // Nothing has been compacted yet, the whole log is dirty.
        resp.dirty_ranges.insert(
          prt.start_offset, kafka::prev_offset(prt.next_offset));
        return resp;
    }

    // Iterate through the clean ranges to produce the dirty ranges.
    const auto& cmp_state = *prt.compaction_state;
    auto offsets_stream = cmp_state.cleaned_ranges.make_stream();
    auto dirty_base_candidate = prt.start_offset;
    while (offsets_stream.has_next()) {
        auto cleaned_range = offsets_stream.next();
        if (cleaned_range.base_offset > dirty_base_candidate) {
            resp.dirty_ranges.insert(
              dirty_base_candidate,
              kafka::prev_offset(cleaned_range.base_offset));
        }
        dirty_base_candidate = kafka::next_offset(cleaned_range.last_offset);
    }
    auto prt_last_offset = kafka::prev_offset(prt.next_offset);
    if (dirty_base_candidate < prt_last_offset) {
        resp.dirty_ranges.insert(dirty_base_candidate, prt_last_offset);
    }

    // Collect the ranges that may have tombstones removed.
    for (const auto& r : cmp_state.cleaned_ranges_with_tombstones) {
        if (r.cleaned_with_tombstones_at <= tombstone_removal_upper_bound_ts) {
            resp.removable_tombstone_ranges.insert(
              r.base_offset, r.last_offset);
        }
    }
    return resp;
}

} // namespace experimental::cloud_topics::l1
