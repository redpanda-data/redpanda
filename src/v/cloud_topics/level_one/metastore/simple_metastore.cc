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
#include "cloud_topics/level_one/metastore/offset_interval_set.h"
#include "cloud_topics/level_one/metastore/state.h"
#include "cloud_topics/level_one/metastore/state_update.h"
#include "cloud_topics/logger.h"
#include "container/chunked_vector.h"
#include "model/fundamental.h"

#include <seastar/core/coroutine.hh>

namespace cloud_topics::l1 {

namespace {
term_state_update_t make_terms_update(const metastore::term_offset_map_t& m) {
    term_state_update_t ret;
    for (const auto& [tp, tp_terms] : m) {
        chunked_vector<term_start> term_updates;
        for (const auto& ts : tp_terms) {
            term_updates.emplace_back(
              term_start{.term_id = ts.term, .start_offset = ts.first_offset});
        }
        ret[tp] = std::move(term_updates);
    }
    return ret;
}
} // namespace

ss::future<std::expected<object_id, simple_object_builder::error>>
simple_object_builder::get_or_create_object_for(
  const model::topic_id_partition& tidp) {
    // The simple metastore isn't partitioned at all, so have all partitions
    // blindly share any existing object.
    if (pending_objects_.empty()) {
        co_return co_await create_object_for(tidp);
    }
    co_return pending_objects_.begin()->first;
}

ss::future<std::expected<object_id, simple_object_builder::error>>
simple_object_builder::create_object_for(const model::topic_id_partition&) {
    auto oid = create_object_id();
    preregister_objects_update prereg{
      .registered_at = model::timestamp::now(),
    };
    prereg.object_ids.push_back(oid);
    auto res = prereg.apply(*state_);
    vassert(res.has_value(), "preregister_objects_update::apply must succeed");
    pending_objects_[oid] = {};
    co_return oid;
}

std::expected<void, simple_object_builder::error>
simple_object_builder::remove_pending_object(object_id oid) {
    auto it = pending_objects_.find(oid);
    if (it == pending_objects_.end()) {
        return std::unexpected(
          error{fmt::format("Object {} is not a pending object", oid)});
    }
    pending_objects_.erase(it);
    return {};
}

std::expected<void, metastore::object_metadata_builder::error>
simple_object_builder::add(
  object_id oid, metastore::object_metadata::ntp_metadata ntp_meta) {
    auto it = pending_objects_.find(oid);
    if (it == pending_objects_.end()) {
        return std::unexpected(
          error{fmt::format("Object {} is not a pending object", oid)});
    }
    if (ntp_meta.base_offset > ntp_meta.last_offset) {
        return std::unexpected(
          error{fmt::format(
            "Metadata has inverted offsets for partition {}, object {}: "
            "base_offset {} > last_offset {}",
            ntp_meta.tidp,
            oid,
            ntp_meta.base_offset,
            ntp_meta.last_offset)});
    }
    auto& pending_metas = it->second;
    pending_metas.emplace_back(ntp_meta);
    return {};
}

std::expected<void, metastore::object_metadata_builder::error>
simple_object_builder::finish(
  object_id oid, size_t footer_pos, size_t object_size) {
    auto it = pending_objects_.find(oid);
    if (it == pending_objects_.end()) {
        return std::unexpected(
          error{fmt::format("Object {} is not a pending object", oid)});
    }
    finished_objects_.emplace_back(
      metastore::object_metadata{
        .oid = oid,
        .footer_pos = footer_pos,
        .object_size = object_size,
        .ntp_metas = std::move(it->second),
      });
    pending_objects_.erase(it);
    return {};
}

bool simple_object_builder::is_empty() const {
    return finished_objects_.empty();
}

std::expected<
  chunked_vector<metastore::object_metadata>,
  metastore::object_metadata_builder::error>
simple_object_builder::release() {
    if (!pending_objects_.empty()) {
        return std::unexpected(
          error{fmt::format(
            "Builder still has {} pending object(s)",
            pending_objects_.size())});
    }
    return std::exchange(finished_objects_, {});
}

new_object make_new_object(const metastore::object_metadata& o) {
    new_object new_o{
      .oid = o.oid,
      .footer_pos = o.footer_pos,
      .object_size = o.object_size,
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

ss::future<std::expected<
  std::unique_ptr<metastore::object_metadata_builder>,
  metastore::errc>>
simple_metastore::object_builder() {
    co_return std::make_unique<simple_object_builder>(&state_);
}

void simple_metastore::preregister_objects(
  const chunked_vector<object_id>& object_ids) {
    preregister_objects_update prereg{
      .registered_at = model::timestamp::now(),
    };
    for (const auto& oid : object_ids) {
        if (!state_.objects.contains(oid)) {
            prereg.object_ids.push_back(oid);
        }
    }
    if (prereg.object_ids.empty()) {
        return;
    }
    auto res = prereg.apply(state_);
    vassert(res.has_value(), "preregister_objects_update::apply must succeed");
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

ss::future<std::expected<metastore::size_response, metastore::errc>>
simple_metastore::get_size(const model::topic_id_partition& tpr) {
    co_return get_size(state_, tpr);
}

std::expected<metastore::size_response, metastore::errc>
simple_metastore::get_size(
  const state& state, const model::topic_id_partition& tpr) {
    auto prt_ref = state.partition_state(tpr);
    if (!prt_ref.has_value()) {
        vlog(cd_log.debug, "Partition {} not tracked", tpr);
        return std::unexpected(metastore::errc::missing_ntp);
    }
    const auto& prt = prt_ref->get();
    return size_response{
      .size = prt.calculate_size(),
      .num_extents = prt.extents.size(),
    };
}

ss::future<std::expected<metastore::add_response, metastore::errc>>
simple_metastore::add_objects(
  const metastore::object_metadata_builder& builder,
  const term_offset_map_t& terms) {
    auto& simple_builder = dynamic_cast<const simple_object_builder&>(builder);
    if (!simple_builder.pending_objects_.empty()) {
        vlog(
          cd_log.error,
          "Failed to add: builder still has {} pending object(s)",
          simple_builder.pending_objects_.size());
        co_return std::unexpected(metastore::errc::invalid_request);
    }
    co_return co_await add_objects(simple_builder.finished_objects_, terms);
}

ss::future<std::expected<metastore::add_response, metastore::errc>>
simple_metastore::add_objects(
  const chunked_vector<object_metadata>& objects,
  const term_offset_map_t& terms) {
    chunked_vector<new_object> new_objects;
    for (const auto& o : objects) {
        new_objects.emplace_back(make_new_object(o));
    }
    auto terms_update = make_terms_update(terms);
    add_response resp;
    auto update_res = add_objects_update::build(
      state_,
      std::move(new_objects),
      std::move(terms_update),
      &resp.corrected_next_offsets);
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
  const metastore::object_metadata_builder& builder) {
    auto& simple_builder = dynamic_cast<const simple_object_builder&>(builder);
    if (!simple_builder.pending_objects_.empty()) {
        vlog(
          cd_log.error,
          "Failed to replace: builder still has {} pending object(s)",
          simple_builder.pending_objects_.size());
        co_return std::unexpected(metastore::errc::invalid_request);
    }
    co_return co_await replace_objects(simple_builder.finished_objects_);
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

ss::future<std::expected<void, metastore::errc>>
simple_metastore::set_start_offset(
  const model::topic_id_partition& tp, kafka::offset requested_o) {
    auto update_res = set_start_offset_update::build(state_, tp, requested_o);
    if (!update_res.has_value()) {
        vlog(cd_log.debug, "Set start offset failed: {}", update_res.error());
        co_return std::unexpected(metastore::errc::invalid_request);
    }
    auto apply_res = update_res->apply(state_);
    vassert(
      apply_res.has_value(),
      "Apply must succeed if can_apply() is true: {}",
      apply_res.error());
    co_return std::expected<void, metastore::errc>{};
}

ss::future<std::expected<metastore::topic_removal_response, metastore::errc>>
simple_metastore::remove_topics(const chunked_vector<model::topic_id>& topics) {
    auto update_res = remove_topics_update::build(state_, topics.copy());
    if (!update_res.has_value()) {
        vlog(cd_log.debug, "Topics removal failed: {}", update_res.error());
        co_return std::unexpected(metastore::errc::invalid_request);
    }
    auto apply_res = update_res->apply(state_);
    vassert(
      apply_res.has_value(),
      "Apply must succeed if can_apply() is true: {}",
      apply_res.error());
    topic_removal_response resp;
    for (const auto& t : topics) {
        if (state_.topic_to_state.contains(t)) {
            vlog(cd_log.error, "Topics removal didn't remove topic {}", t);
            resp.not_removed.insert(t);
        }
    }
    vassert(
      resp.not_removed.empty(),
      "Topic removal in the simple_metastore is expected to always remove all "
      "requested topics, {} not removed",
      resp.not_removed.size());
    co_return resp;
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
        auto object_size = object_it->second.object_size;
        return metastore::object_response{
          .oid = it->oid,
          .footer_pos = footer_pos,
          .object_size = object_size,
          .first_offset = it->base_offset,
          .last_offset = it->last_offset,
        };
    }
    return std::unexpected(metastore::errc::out_of_range);
}

ss::future<std::expected<metastore::object_response, metastore::errc>>
simple_metastore::get_first_ge(
  const model::topic_id_partition& tpr, kafka::offset o, model::timestamp ts) {
    co_return get_first_ge(state_, tpr, o, ts);
}

std::expected<metastore::object_response, metastore::errc>
simple_metastore::get_first_ge(
  const state& state,
  const model::topic_id_partition& tpr,
  kafka::offset o,
  model::timestamp ts) {
    auto prt_ref = state.partition_state(tpr);
    if (!prt_ref.has_value()) {
        vlog(cd_log.debug, "Partition {} not tracked", tpr);
        return std::unexpected(metastore::errc::missing_ntp);
    }
    auto& prt = prt_ref->get();
    for (const auto& obj : prt.extents) {
        if (o > obj.last_offset) {
            continue;
        }
        if (obj.max_timestamp >= ts) {
            auto object_it = state.objects.find(obj.oid);
            if (object_it == state.objects.end()) {
                return std::unexpected(metastore::errc::out_of_range);
            }
            auto footer_pos = object_it->second.footer_pos;
            auto object_size = object_it->second.object_size;
            return metastore::object_response{
              .oid = obj.oid,
              .footer_pos = footer_pos,
              .object_size = object_size,
              .first_offset = obj.base_offset,
              .last_offset = obj.last_offset,
            };
        }
    }
    return std::unexpected(metastore::errc::out_of_range);
}

ss::future<std::expected<kafka::offset, metastore::errc>>
simple_metastore::get_first_offset_for_bytes(
  const model::topic_id_partition& tpr, uint64_t size) {
    co_return get_first_offset_for_bytes(state_, tpr, size);
}

std::expected<kafka::offset, metastore::errc>
simple_metastore::get_first_offset_for_bytes(
  const state& state, const model::topic_id_partition& tpr, uint64_t size) {
    auto prt_ref = state.partition_state(tpr);
    if (!prt_ref.has_value()) {
        vlog(cd_log.debug, "Partition {} not tracked", tpr);
        return std::unexpected(metastore::errc::missing_ntp);
    }
    auto& prt = prt_ref->get();
    kafka::offset offset = prt.next_offset;
    if (size == 0) {
        return offset;
    }
    for (const auto& obj : std::views::reverse(prt.extents)) {
        offset = obj.base_offset;
        size -= std::min(size, obj.len);
        if (size == 0) {
            return offset;
        }
    }
    return std::unexpected(metastore::errc::out_of_range);
}

ss::future<std::expected<kafka::offset, metastore::errc>>
simple_metastore::get_end_offset_for_term(
  const model::topic_id_partition& tp, model::term_id requested_term) {
    co_return get_end_offset_for_term(state_, tp, requested_term);
}
std::expected<kafka::offset, metastore::errc>
simple_metastore::get_end_offset_for_term(
  const state& state,
  const model::topic_id_partition& tp,
  model::term_id requested_term) {
    auto prt_ref = state.partition_state(tp);
    if (!prt_ref.has_value()) {
        vlog(cd_log.debug, "Partition {} not tracked", tp);
        return std::unexpected(metastore::errc::missing_ntp);
    }
    auto& prt = prt_ref->get();
    if (prt.term_starts.empty()) {
        return std::unexpected(metastore::errc::missing_ntp);
    }

    auto first_ge_it = std::ranges::lower_bound(
      prt.term_starts, requested_term, std::less<>{}, &term_start::term_id);
    if (first_ge_it == prt.term_starts.end()) {
        // All terms are below `requested_term`.
        return std::unexpected(metastore::errc::out_of_range);
    }
    if (first_ge_it->term_id > requested_term) {
        // If we've already found a higher term, return its start; that marks
        // the end (last + 1) of the requested term.
        return first_ge_it->start_offset;
    }
    vassert(
      first_ge_it->term_id == requested_term,
      "lower_bound() return val not >= {}: {}",
      requested_term,
      first_ge_it->term_id);
    auto next_higher = std::next(first_ge_it);
    if (next_higher == prt.term_starts.end()) {
        // The requested term is the last term in L1. The end offset (last + 1)
        // is equivalent to the next offset of L1.
        return prt.next_offset;
    }
    // The term's end offset is equivalent to the start of the next term.
    return next_higher->start_offset;
}

ss::future<std::expected<model::term_id, metastore::errc>>
simple_metastore::get_term_for_offset(
  const model::topic_id_partition& tp, kafka::offset requested_o) {
    co_return get_term_for_offset(state_, tp, requested_o);
}

std::expected<model::term_id, metastore::errc>
simple_metastore::get_term_for_offset(
  const state& state,
  const model::topic_id_partition& tp,
  kafka::offset requested_o) {
    auto prt_ref = state.partition_state(tp);
    if (!prt_ref.has_value()) {
        vlog(cd_log.debug, "Partition {} not tracked", tp);
        return std::unexpected(metastore::errc::missing_ntp);
    }
    auto& prt = prt_ref->get();
    if (prt.term_starts.empty()) {
        return std::unexpected(metastore::errc::missing_ntp);
    }
    // Past the next offset return OOR, but return a valid term for the exact
    // next offset, which may be the HWM.
    if (requested_o > prt.next_offset) {
        return std::unexpected(metastore::errc::out_of_range);
    }

    // Find the first > the requested, aka first >= the next.
    auto next_o = kafka::next_offset(requested_o);
    auto first_gt_it = std::ranges::lower_bound(
      prt.term_starts, next_o, std::less<>{}, &term_start::start_offset);
    if (first_gt_it == prt.term_starts.begin()) {
        // All term starts are above `requested_o`.
        return std::unexpected(metastore::errc::out_of_range);
    }
    auto last_le_it = std::prev(first_gt_it);
    return last_le_it->term_id;
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
        if (!cm.new_cleaned_ranges.empty()) {
            auto& new_cleaned_ranges = cm.new_cleaned_ranges;
            chunked_vector<compaction_state_update::cleaned_range> ranges;
            ranges.reserve(new_cleaned_ranges.size());
            for (const auto& cleaned_range : new_cleaned_ranges) {
                ranges.push_back(
                  {.base_offset = cleaned_range.base_offset,
                   .last_offset = cleaned_range.last_offset,
                   .has_tombstones = cleaned_range.has_tombstones});
            }
            p_update.new_cleaned_ranges = std::move(ranges);
        }
        p_update.removed_tombstones_ranges = cm.removed_tombstones_ranges;
        p_update.cleaned_at = cm.cleaned_at;
        p_update.expected_compaction_epoch
          = partition_state::compaction_epoch_t{cm.expected_compaction_epoch()};
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
  const metastore::object_metadata_builder& builder,
  const compaction_map_t& compaction_metas) {
    auto& simple_builder = dynamic_cast<const simple_object_builder&>(builder);
    if (!simple_builder.pending_objects_.empty()) {
        vlog(
          cd_log.error,
          "Failed to compact: builder still has {} pending object(s)",
          simple_builder.pending_objects_.size());
        co_return std::unexpected(metastore::errc::invalid_request);
    }
    co_return co_await compact_objects(
      simple_builder.finished_objects_, compaction_metas);
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
    if (dirty_base_candidate <= prt_last_offset) {
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

std::expected<double, metastore::errc> simple_metastore::get_dirty_ratio(
  const state& state, const model::topic_id_partition& tp) {
    auto prt_ref = state.partition_state(tp);

    if (!prt_ref.has_value()) {
        vlog(cd_log.debug, "Partition {} not tracked", tp);
        return std::unexpected(errc::missing_ntp);
    }

    const auto& prt = prt_ref->get();

    const auto& compaction_state = prt.compaction_state;

    if (!compaction_state.has_value()) {
        return 1.0;
    }

    // Compute
    size_t total_size{0};
    size_t dirty_size{0};
    const auto& cleaned_ranges = compaction_state->cleaned_ranges;
    for (const auto& extent : prt.extents) {
        total_size += extent.len;
        auto b = extent.base_offset;
        if (unlikely(b < prt.start_offset)) {
            // The extent is partially truncated
            b = prt.start_offset;
        }
        auto e = extent.last_offset;
        if (!cleaned_ranges.covers(b, e)) {
            dirty_size += extent.len;
        }
    }

    return total_size == 0 ? 0.0
                           : static_cast<double>(dirty_size)
                               / static_cast<double>(total_size);
}

std::expected<std::optional<model::timestamp>, metastore::errc>
simple_metastore::get_earliest_dirty_ts(
  const state& state, const model::topic_id_partition& tp) {
    auto prt_ref = state.partition_state(tp);

    if (!prt_ref.has_value()) {
        vlog(cd_log.debug, "Partition {} not tracked", tp);
        return std::unexpected(errc::missing_ntp);
    }

    const auto& prt = prt_ref->get();

    const auto& compaction_state = prt.compaction_state;

    // Get cleaned ranges if compaction has been run.
    offset_interval_set cleaned_ranges;
    if (compaction_state.has_value()) {
        cleaned_ranges = compaction_state->cleaned_ranges;
    }

    // Iterate through all extents to find the minimum timestamp among dirty
    // extents.
    std::optional<model::timestamp> earliest_dirty_ts;
    for (const auto& extent : prt.extents) {
        auto base = extent.base_offset;
        if (base < prt.start_offset) {
            // The extent is partially truncated.
            base = prt.start_offset;
        }
        auto last = extent.last_offset;

        if (!cleaned_ranges.covers(base, last)) {
            // This extent is dirty. Track the minimum timestamp.
            if (
              !earliest_dirty_ts.has_value()
              || extent.max_timestamp < *earliest_dirty_ts) {
                earliest_dirty_ts = extent.max_timestamp;
            }
        }
    }

    return earliest_dirty_ts;
}

std::expected<metastore::compaction_epoch, metastore::errc>
simple_metastore::get_compaction_epoch(
  const state& state, const model::topic_id_partition& tp) {
    auto prt_ref = state.partition_state(tp);

    if (!prt_ref.has_value()) {
        vlog(cd_log.debug, "Partition {} not tracked", tp);
        return std::unexpected(errc::missing_ntp);
    }

    const auto& prt = prt_ref->get();
    return metastore::compaction_epoch{prt.compaction_epoch()};
}

ss::future<std::expected<metastore::compaction_info_response, metastore::errc>>
simple_metastore::get_compaction_info(const compaction_info_spec& log) {
    co_return get_compaction_info(
      state_, log.tidp, log.tombstone_removal_upper_bound_ts);
}

std::expected<metastore::compaction_info_response, metastore::errc>
simple_metastore::get_compaction_info(
  const state& state,
  const model::topic_id_partition& tidp,
  model::timestamp ts) {
    auto dirty_ratio = get_dirty_ratio(state, tidp);
    if (!dirty_ratio.has_value()) {
        return std::unexpected(dirty_ratio.error());
    }

    auto earliest_dirty_ts = get_earliest_dirty_ts(state, tidp);
    if (!earliest_dirty_ts.has_value()) {
        return std::unexpected(earliest_dirty_ts.error());
    }

    auto compact_offsets = get_compaction_offsets(state, tidp, ts);
    if (!compact_offsets.has_value()) {
        return std::unexpected(compact_offsets.error());
    }

    auto compaction_epoch = get_compaction_epoch(state, tidp);
    if (!compaction_epoch.has_value()) {
        return std::unexpected(compaction_epoch.error());
    }

    auto log_offsets = get_offsets(state, tidp);
    if (!log_offsets.has_value()) {
        return std::unexpected(log_offsets.error());
    }

    return compaction_info_response{
      .dirty_ratio = dirty_ratio.value(),
      .earliest_dirty_ts = earliest_dirty_ts.value(),
      .offsets_response = std::move(compact_offsets).value(),
      .compaction_epoch = compaction_epoch.value(),
      .start_offset = log_offsets.value().start_offset};
}

ss::future<std::expected<metastore::compaction_info_map, metastore::errc>>
simple_metastore::get_compaction_infos(
  const chunked_vector<compaction_info_spec>& logs) {
    compaction_info_map infos;
    for (const auto& log : logs) {
        infos.emplace(log.tidp, co_await get_compaction_info(log));
    }
    co_return infos;
}

ss::future<std::expected<metastore::extent_metadata_response, metastore::errc>>
simple_metastore::get_extent_metadata_forwards(
  const model::topic_id_partition& tp,
  kafka::offset min_offset,
  kafka::offset max_offset,
  size_t max_num_extents,
  include_object_metadata include_object_metadata) {
    co_return get_extent_metadata_forwards(
      state_,
      tp,
      min_offset,
      max_offset,
      max_num_extents,
      include_object_metadata);
}

std::expected<metastore::extent_metadata_response, metastore::errc>
simple_metastore::get_extent_metadata_forwards(
  const state& state,
  const model::topic_id_partition& tp,
  kafka::offset min_offset,
  kafka::offset max_offset,
  size_t max_num_extents,
  include_object_metadata include_object_metadata) {
    auto prt_ref = state.partition_state(tp);

    if (!prt_ref.has_value()) {
        vlog(cd_log.debug, "Partition {} not tracked", tp);
        return std::unexpected(errc::missing_ntp);
    }

    const auto& prt = prt_ref->get();

    extent_metadata_vec extents;
    bool end_of_stream = true;

    auto min_it = std::ranges::lower_bound(
      prt.extents, min_offset, std::less<>{}, &extent::last_offset);
    for (auto it = min_it; it != prt.extents.end(); ++it) {
        auto& ext = *it;
        if (ext.base_offset > max_offset) {
            break;
        }

        extent_metadata em{
          .base_offset = ext.base_offset,
          .last_offset = ext.last_offset,
          .max_timestamp = ext.max_timestamp};

        if (include_object_metadata) {
            auto object_it = state.objects.find(ext.oid);
            if (object_it == state.objects.end()) {
                vlog(
                  cd_log.error,
                  "Missing object metadata for oid {} in extent "
                  "({}~{})",
                  ext.oid,
                  ext.base_offset,
                  ext.last_offset);
                return std::unexpected(errc::out_of_range);
            }
            em.object_info = extent_object_info{
              .oid = ext.oid,
              .footer_pos = object_it->second.footer_pos,
              .object_size = object_it->second.object_size,
            };
        }

        extents.push_back(std::move(em));

        if (extents.size() >= max_num_extents) {
            end_of_stream = false;
            break;
        }
    }

    return extent_metadata_response{
      .extents = std::move(extents), .end_of_stream = end_of_stream};
}

ss::future<std::expected<metastore::extent_metadata_response, metastore::errc>>
simple_metastore::get_extent_metadata_backwards(
  const model::topic_id_partition& tp,
  kafka::offset min_offset,
  kafka::offset max_offset,
  size_t max_num_extents) {
    co_return get_extent_metadata_backwards(
      state_, tp, min_offset, max_offset, max_num_extents);
}

std::expected<metastore::extent_metadata_response, metastore::errc>
simple_metastore::get_extent_metadata_backwards(
  const state& state,
  const model::topic_id_partition& tp,
  kafka::offset min_offset,
  kafka::offset max_offset,
  size_t max_num_extents) {
    auto prt_ref = state.partition_state(tp);

    if (!prt_ref.has_value()) {
        vlog(cd_log.debug, "Partition {} not tracked", tp);
        return std::unexpected(errc::missing_ntp);
    }

    const auto& prt = prt_ref->get();

    extent_metadata_vec extents;
    bool end_of_stream = true;

    auto max_it = std::ranges::lower_bound(
      prt.extents, max_offset, std::less<>{}, &extent::last_offset);
    if (max_it != prt.extents.end() && max_it->base_offset > max_offset) {
        if (max_it != prt.extents.begin()) {
            --max_it;
        } else {
            // No extents.
            return extent_metadata_response{};
        }
    }
    auto max_rit = max_it == prt.extents.end()
                     ? std::make_reverse_iterator(prt.extents.end())
                     : std::make_reverse_iterator(std::next(max_it));
    for (auto it = max_rit; it != prt.extents.rend(); ++it) {
        auto& ext = *it;
        if (ext.last_offset < min_offset) {
            break;
        }

        extents.push_back(
          {.base_offset = ext.base_offset,
           .last_offset = ext.last_offset,
           .max_timestamp = ext.max_timestamp});

        if (extents.size() >= max_num_extents) {
            end_of_stream = false;
            break;
        }
    }

    return extent_metadata_response{
      .extents = std::move(extents), .end_of_stream = end_of_stream};
}

} // namespace cloud_topics::l1
