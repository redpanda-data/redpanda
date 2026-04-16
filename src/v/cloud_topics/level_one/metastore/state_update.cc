/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/metastore/state_update.h"

#include "base/vlog.h"
#include "cloud_topics/level_one/metastore/state_update_utils.h"
#include "cloud_topics/logger.h"
#include "model/fundamental.h"

namespace cloud_topics::l1 {

namespace {

using extent_iter_t = partition_state::extent_set_t::const_iterator;
struct extent_range {
    extent_iter_t base_it;
    extent_iter_t last_it;
};
std::optional<extent_range> get_range(
  const partition_state::extent_set_t& extents,
  kafka::offset base,
  kafka::offset last,
  kafka::offset log_start_offset) {
    auto base_it = extents.begin();
    // If base < log_start_offset, we are considering replacing an extent below
    // the log start offset. We allow for misalignment between the start offset
    // and the first extent in the partition, since we need to support partial
    // truncations- this also means we don't need replacement extents to be
    // aligned to existing extents below the start offset. Fallthrough here
    // instead of returning `std::nullopt`- we will still need to validate the
    // alignment of the range's last offset.
    if (base >= log_start_offset) {
        base_it = std::ranges::lower_bound(
          extents, base, std::less<>{}, &extent::base_offset);
        if (base_it == extents.end() || base_it->base_offset != base) {
            return std::nullopt;
        }
    }
    // Check that the range's last offset aligns with an existing extent.
    auto last_it = std::ranges::lower_bound(
      extents, last, std::less<>{}, &extent::last_offset);
    if (last_it == extents.end() || last_it->last_offset != last) {
        return std::nullopt;
    }
    return extent_range{base_it, last_it};
}

void remove_extents_below_start_offset_for_tp(
  state& state, model::topic_id_partition tp, std::string_view ctx) {
    auto& p_state
      = state.topic_to_state[tp.topic_id].pid_to_state[tp.partition];
    auto start_offset = p_state.start_offset;
    while (!p_state.extents.empty()) {
        if (p_state.extents.begin()->last_offset >= start_offset) {
            break;
        }
        // The front extent falls entirely below the new start offset, meaning
        // it's can be removed.
        auto begin_it = p_state.extents.begin();
        auto oid = begin_it->oid;
        auto obj_it = state.objects.find(oid);
        if (obj_it == state.objects.end()) {
            // Unexpected, but benign.
            continue;
        }
        obj_it->second.removed_data_size += begin_it->len;
        vlog(
          cd_log.debug,
          "{} for {}: {}, removing extent {} [{}, {}]",
          ctx,
          tp,
          start_offset,
          begin_it->oid,
          begin_it->base_offset,
          begin_it->last_offset);
        p_state.extents.erase(begin_it);
    }
}

} // namespace

size_t
new_object::collect_extents_by_tidp(sorted_extents_by_tidp_t* ret) const {
    size_t total_data_size = 0;
    for (const auto& [tid, p_extents] : extent_metas) {
        for (const auto& [p, extent_meta] : p_extents) {
            auto& ret_extents = (*ret)[model::topic_id_partition(tid, p)];
            total_data_size += extent_meta.len;
            ret_extents.insert(
              extent{
                .base_offset = extent_meta.base_offset,
                .last_offset = extent_meta.last_offset,
                .max_timestamp = extent_meta.max_timestamp,
                .filepos = extent_meta.filepos,
                .len = extent_meta.len,
                .oid = oid,
              });
        }
    }
    return total_data_size;
}

std::expected<add_objects_update, stm_update_error> add_objects_update::build(
  const state& state,
  chunked_vector<new_object> objects,
  term_state_update_t terms,
  chunked_hash_map<model::topic_id_partition, kafka::offset>* corrections) {
    add_objects_update update{
      .new_objects = std::move(objects),
      .new_terms = std::move(terms),
    };
    auto allowed = update.can_apply(state, corrections);
    if (!allowed.has_value()) {
        return std::unexpected(allowed.error());
    }
    return update;
}

std::expected<std::monostate, stm_update_error> add_objects_update::can_apply(
  const state& state,
  chunked_hash_map<model::topic_id_partition, kafka::offset>* corrections) {
    if (new_objects.empty()) {
        return std::unexpected(stm_update_error{"No objects requested"});
    }
    if (new_terms.empty()) {
        return std::unexpected(
          stm_update_error{"Missing term info in request"});
    }
    sorted_extents_by_tidp_t new_extents;
    for (const auto& o : new_objects) {
        auto it = state.objects.find(o.oid);
        if (it == state.objects.end()) {
            return std::unexpected(
              stm_update_error{
                fmt::format("Object {} not pre-registered", o.oid)});
        }
        if (!it->second.is_preregistration) {
            return std::unexpected(
              stm_update_error{fmt::format("Object {} already exists", o.oid)});
        }
        o.collect_extents_by_tidp(&new_extents);
    }

    chunked_hash_map<model::topic_id_partition, kafka::offset>
      corrected_next_offsets;
    for (const auto& [tidp, extents] : new_extents) {
        // TODO: maybe we need some mount operation that adopts a partition log
        // and allows it to start a specific offset.
        auto p_state = state.partition_state(tidp);
        auto expected_next = p_state ? p_state->get().next_offset
                                     : kafka::offset{0};

        if (extents.begin()->base_offset != expected_next) {
            // If the start of the new extents for this partition aren't
            // aligned, allow the operation to succeed, but the expectation is
            // when applying, we'll "drop" these extents.
            corrected_next_offsets[tidp] = expected_next;
            continue;
        }
        for (const auto& extent : extents) {
            if (extent.base_offset > extent.last_offset) {
                return std::unexpected(stm_update_error(
                  fmt::format(
                    "Input object has inverted extent for partition {}: "
                    "base_offset {} > last_offset {}",
                    tidp,
                    extent.base_offset,
                    extent.last_offset)));
            }
            if (extent.base_offset != expected_next) {
                return std::unexpected(stm_update_error(
                  fmt::format(
                    "Input object breaks partition {} offset ordering: "
                    "expected next: {}, actual: {}",
                    tidp,
                    expected_next,
                    extent.base_offset())));
            }
            expected_next = kafka::next_offset(extent.last_offset);
        }
        if (!new_terms.contains(tidp)) {
            return std::unexpected(
              stm_update_error{fmt::format("Missing term info for {}", tidp)});
        }
    }
    // Now that we've validated the offsets of our extents, validate the terms.
    for (const auto& [tp, req_entries] : new_terms) {
        if (req_entries.empty()) {
            return std::unexpected(
              stm_update_error{
                fmt::format("Empty terms requested for {}", tp)});
        }
        auto extents_it = new_extents.find(tp);
        if (extents_it == new_extents.end() || extents_it->second.empty()) {
            return std::unexpected(
              stm_update_error{fmt::format(
                "Terms provided for a partition that has no extents", tp)});
        }
        // Now check that the term entries are in increasing order.
        auto max_term_so_far = model::term_id{-1};
        auto max_offset_so_far = kafka::offset{-1};
        for (const auto& entry : req_entries) {
            if (
              entry.term_id <= max_term_so_far
              || entry.start_offset <= max_offset_so_far) {
                return std::unexpected(
                  stm_update_error{fmt::format(
                    "Invalid term for {}: term={}, offset={}, "
                    "max_term_so_far={}, max_offset_so_far={}",
                    tp,
                    entry.term_id,
                    entry.start_offset,
                    max_term_so_far,
                    max_offset_so_far)});
            }
            max_term_so_far = entry.term_id;
            max_offset_so_far = entry.start_offset;
        }
        if (corrected_next_offsets.contains(tp)) {
            // Don't bother checking the terms against the extents if we
            // already know the extent offsets are off.
            continue;
        }
        auto new_extents_start_offset
          = extents_it->second.begin()->base_offset();
        auto new_terms_start_offset = req_entries.begin()->start_offset;
        if (new_extents_start_offset != new_terms_start_offset) {
            return std::unexpected(
              stm_update_error{fmt::format(
                "Extent start and term start do not match for {}: {} != {}",
                tp,
                new_extents_start_offset,
                new_terms_start_offset)});
        }
        auto new_extents_last_offset = extents_it->second.rbegin()->last_offset;
        auto new_terms_last_start_offset = req_entries.back().start_offset;
        if (new_extents_last_offset < new_terms_last_start_offset) {
            return std::unexpected(
              stm_update_error{fmt::format(
                "Extents end below a requested new term for {}: {} < {}",
                tp,
                new_extents_last_offset,
                new_terms_last_start_offset)});
        }
        auto p_state = state.partition_state(tp);
        // Check that the incoming term entries can be appended to our state
        // without violating ordering requirements.
        if (p_state.has_value() && !p_state->get().term_starts.empty()) {
            auto p_last_entry = *p_state->get().term_starts.rbegin();
            auto req_first_entry = req_entries.begin();

            // NOTE: it's valid for the first requested term to be equal to the
            // last term (e.g. if leadership has not changed). The same cannot
            // be said about offsets, hence the difference in comparator.
            if (req_first_entry->term_id < p_last_entry.term_id) {
                return std::unexpected(
                  stm_update_error{fmt::format(
                    "New term for {} must be >= last term: {} < {}",
                    tp,
                    req_first_entry->term_id,
                    p_last_entry.term_id)});
            }
            if (req_first_entry->start_offset <= p_last_entry.start_offset) {
                return std::unexpected(
                  stm_update_error{fmt::format(
                    "New term for {} must start after last term: {} <= {}",
                    tp,
                    req_first_entry->start_offset,
                    p_last_entry.start_offset)});
            }
        }
    }
    if (corrections) {
        *corrections = std::move(corrected_next_offsets);
    }
    return std::monostate{};
}

std::expected<std::monostate, stm_update_error>
add_objects_update::apply(state& state) {
    auto allowed = can_apply(state);
    if (!allowed.has_value()) {
        return std::unexpected(allowed.error());
    }
    sorted_extents_by_tidp_t extents_by_tp;
    for (const auto& o : new_objects) {
        o.collect_extents_by_tidp(&extents_by_tp);
        state.objects[o.oid] = object_entry{
          .total_data_size = 0,
          .removed_data_size = 0,
          .footer_pos = o.footer_pos,
          .object_size = o.object_size,
          .last_updated = model::timestamp::now(),
          .is_preregistration = false,
        };
    }
    for (const auto& [tidp, extents] : extents_by_tp) {
        auto p_state = state.partition_state(tidp);
        auto expected_next = p_state ? p_state->get().next_offset
                                     : kafka::offset{0};
        if (extents.begin()->base_offset == expected_next) {
            auto& t_state = state.topic_to_state[tidp.topic_id];
            auto& p_state = t_state.pid_to_state[tidp.partition];
            // We've validated that all extents form a contiguous offset space.
            // Accept them all.
            for (const auto& e : extents) {
                p_state.extents.emplace(e);
            }
            p_state.next_offset = kafka::next_offset(
              p_state.extents.rbegin()->last_offset);
            for (const auto& extent : extents) {
                state.objects[extent.oid].total_data_size += extent.len;
                vlog(
                  cd_log.debug,
                  "Object {} added to {} [{}, {}]",
                  extent.oid,
                  tidp,
                  extent.base_offset,
                  extent.last_offset);
            }

            // Also append the terms.
            auto req_terms_it = new_terms.find(tidp);
            if (req_terms_it == new_terms.end()) {
                // Conservative error -- this should have been validated in
                // can_apply().
                return std::unexpected(
                  stm_update_error{fmt::format(
                    "Expected term updates for applied extents: {}", tidp)});
            }
            const auto& req_terms = req_terms_it->second;
            auto new_term_it = req_terms.begin();
            if (
              !p_state.term_starts.empty()
              && req_terms.begin()->term_id
                   <= p_state.term_starts.rbegin()->term_id) {
                // If the first added term matches the back of the latest
                // tracked term, it isn't a new term.
                ++new_term_it;
            }
            p_state.term_starts.insert(new_term_it, req_terms.end());
        } else {
            // The incoming extents don't align with the next position. "Drop"
            // them all.
            for (const auto& extent : extents) {
                auto& obj_entry = state.objects[extent.oid];
                obj_entry.removed_data_size += extent.len;
                obj_entry.total_data_size += extent.len;
                vlog(
                  cd_log.debug,
                  "Extent of {} in {} [{}, {}] rejected, {} != expected next: "
                  "{}",
                  tidp,
                  extent.oid,
                  extent.base_offset,
                  extent.last_offset,
                  extents.begin()->base_offset,
                  expected_next);
            }
        }
    }
    return std::monostate{};
}

std::expected<std::monostate, stm_update_error>
replace_objects_update::can_apply(const state& state) {
    if (new_objects.empty()) {
        return std::unexpected(stm_update_error{"No objects requested"});
    }
    sorted_extents_by_tidp_t new_extents_by_tp;
    for (const auto& o : new_objects) {
        auto it = state.objects.find(o.oid);
        if (it == state.objects.end()) {
            return std::unexpected(
              stm_update_error{
                fmt::format("Object {} not pre-registered", o.oid)});
        }
        if (!it->second.is_preregistration) {
            return std::unexpected(
              stm_update_error{fmt::format("Object {} already exists", o.oid)});
        }
        o.collect_extents_by_tidp(&new_extents_by_tp);
    }
    for (const auto& [tidp, extents] : new_extents_by_tp) {
        for (const auto& extent : extents) {
            if (extent.base_offset > extent.last_offset) {
                return std::unexpected(stm_update_error(
                  fmt::format(
                    "Input object has inverted extent for partition {}: "
                    "base_offset {} > last_offset {}",
                    tidp,
                    extent.base_offset,
                    extent.last_offset)));
            }
        }
    }

    auto contiguous_intervals_by_tp = contiguous_intervals_for_extents(
      new_extents_by_tp);
    if (!contiguous_intervals_by_tp.has_value()) {
        return std::unexpected(
          stm_update_error(contiguous_intervals_by_tp.error()));
    }

    for (const auto& [tidp, intervals] : contiguous_intervals_by_tp.value()) {
        for (const auto& interval : intervals) {
            auto req_base = interval.base_offset;
            auto req_last = interval.last_offset;

            auto p_state = state.partition_state(tidp);
            if (!p_state) {
                return std::unexpected(stm_update_error(
                  fmt::format("Partition {} not tracked by state", tidp)));
            }

            // Check that the new range's offset aligns with existing extents
            // above the log's start offset.
            const auto& prt = p_state->get();
            auto iters = get_range(
              prt.extents, req_base, req_last, prt.start_offset);
            if (!iters.has_value()) {
                return std::unexpected(stm_update_error(
                  fmt::format(
                    "Partition {} doesn't contain extents that span exactly "
                    "[{}, {}]",
                    tidp,
                    req_base,
                    req_last)));
            }
        }
    }
    if (compaction_updates.empty()) {
        return std::monostate{};
    }

    for (const auto& [t, t_req] : compaction_updates) {
        for (const auto& [p, compaction_update] : t_req) {
            model::topic_id_partition tidp{t, p};
            auto p_ref = state.partition_state(tidp);
            const auto& p_state = p_ref->get();
            // Validate the expected compaction epoch. If it does not match the
            // current compaction epoch, there has been a concurrent race.
            if (
              compaction_update.expected_compaction_epoch
              != p_state.compaction_epoch) {
                return std::unexpected(stm_update_error(
                  fmt::format(
                    "Expected compaction epoch {} does not match the current "
                    "compaction epoch {} for {}",
                    compaction_update.expected_compaction_epoch,
                    p_state.compaction_epoch,
                    tidp)));
            }

            // Validate that any cleaned ranges actually correspond to the
            // new extents.
            auto new_extent_iter = new_extents_by_tp.find(tidp);
            if (new_extent_iter == new_extents_by_tp.end()) {
                return std::unexpected(stm_update_error(
                  fmt::format(
                    "New cleaned range does not refer to partition with "
                    "extent: {}",
                    tidp)));
            }
            if (!compaction_update.new_cleaned_ranges.empty()) {
                const auto& req_cleaned_ranges
                  = compaction_update.new_cleaned_ranges;
                // Check that the new extents span the start of the log to the
                // end of the new clean range.
                auto& [_, new_extents] = *new_extent_iter;
                auto req_last = new_extents.rbegin()->last_offset;
                auto clean_last = req_cleaned_ranges.back().last_offset;
                if (clean_last > req_last) {
                    return std::unexpected(stm_update_error(
                      fmt::format(
                        "Cleaned range for {} does not match requested new "
                        "extents' last_offset {} > {}",
                        tidp,
                        clean_last,
                        req_last)));
                }
                auto req_extents_base = new_extents.begin()->base_offset;
                auto clean_base = req_cleaned_ranges.front().base_offset;
                if (req_extents_base > clean_base) {
                    return std::unexpected(stm_update_error(
                      fmt::format(
                        "Cleaned range start_offset for {} is not covered by "
                        "extents: {} > {}",
                        tidp,
                        req_extents_base,
                        clean_base)));
                }

                // Check that the extents replace down to the start of the log.
                // By definition, this is a requirement of cleaning the log.
                if (req_extents_base > p_state.start_offset) {
                    return std::unexpected(stm_update_error(
                      fmt::format(
                        "Cleaned range for {} does not replace to the "
                        "beginning "
                        "of the log: {} > {}",
                        tidp,
                        req_extents_base,
                        p_state.start_offset)));
                }

                // Check that ranges with tombstones don't overlap with existing
                // ranges with tombstones.
                for (const auto& req_cleaned_range : req_cleaned_ranges) {
                    if (
                      req_cleaned_range.has_tombstones
                      && p_state.compaction_state.has_value()
                      && !p_state.compaction_state->may_add(
                        compaction_state::cleaned_range_with_tombstones{
                          .base_offset = req_cleaned_range.base_offset,
                          .last_offset = req_cleaned_range.last_offset,
                          .cleaned_with_tombstones_at
                          = compaction_update.cleaned_at,
                        })) {
                        return std::unexpected(stm_update_error(
                          fmt::format(
                            "Cleaned range for {} has tombstones and overlaps "
                            "with an existing cleaned range with tombstones: "
                            "[{}, {}]",
                            tidp,
                            req_cleaned_range.base_offset,
                            req_cleaned_range.last_offset)));
                    }
                }
            }

            // Check that the requested range with removed tombstones is
            // tracked as actually having tombstones.
            auto req_range_removed_tombstones
              = compaction_update.removed_tombstones_ranges.make_stream();
            while (req_range_removed_tombstones.has_next()) {
                auto req_range = req_range_removed_tombstones.next();
                if (
                  !p_state.compaction_state.has_value()
                  || !p_state.compaction_state
                        ->has_contiguous_range_with_tombstones(
                          req_range.base_offset, req_range.last_offset)) {
                    return std::unexpected(stm_update_error(
                      fmt::format(
                        "Tombstone-removed range [{}, {}] for {} is not "
                        "tracked as having tombstones",
                        req_range.base_offset,
                        req_range.last_offset,
                        tidp)));
                }
            }
        }
    }
    return std::monostate{};
}

std::expected<std::monostate, stm_update_error>
replace_objects_update::apply(state& state) {
    auto allowed = can_apply(state);
    if (!allowed.has_value()) {
        return std::unexpected(allowed.error());
    }
    sorted_extents_by_tidp_t new_extents_by_tp;
    for (const auto& o : new_objects) {
        o.collect_extents_by_tidp(&new_extents_by_tp);
        state.objects[o.oid] = object_entry{
          .total_data_size = 0,
          .removed_data_size = 0,
          .footer_pos = o.footer_pos,
          .object_size = o.object_size,
          .last_updated = model::timestamp::now(),
          .is_preregistration = false,
        };
    }

    auto contiguous_intervals_by_tp
      = contiguous_intervals_for_extents(new_extents_by_tp).value();

    for (const auto& [tidp, intervals] : contiguous_intervals_by_tp) {
        auto& p_state
          = state.topic_to_state[tidp.topic_id].pid_to_state[tidp.partition];
        for (const auto& interval : intervals) {
            auto requested_base = interval.base_offset;
            auto requested_last = interval.last_offset;
            auto iters = get_range(
              p_state.extents,
              requested_base,
              requested_last,
              p_state.start_offset);
            auto [base_it, last_it] = *iters;
            auto end_it = std::next(last_it);
            for (auto iter = base_it; iter != end_it; ++iter) {
                auto& old_extent = *iter;
                state.objects[old_extent.oid].removed_data_size
                  += old_extent.len;
                vlog(
                  cd_log.debug,
                  "Removing extent of {} in {} [{}, {}]",
                  tidp,
                  old_extent.oid,
                  old_extent.base_offset,
                  old_extent.last_offset);
            }

            p_state.extents.erase(base_it, end_it);
        }
    }

    for (const auto& [tidp, new_extents] : new_extents_by_tp) {
        auto& p_state
          = state.topic_to_state[tidp.topic_id].pid_to_state[tidp.partition];
        // NOTE: we don't need to update the start or next offsets since we've
        // validated that the new extents replace exact ranges.

        for (const auto& e : new_extents) {
            p_state.extents.emplace(e);
            vlog(
              cd_log.debug,
              "Adding replacement extent of {} in {} [{}, {}]",
              tidp,
              e.oid,
              e.base_offset,
              e.last_offset);
        }

        for (const auto& extent : new_extents) {
            state.objects[extent.oid].total_data_size += extent.len;
        }
    }

    for (const auto& [tidp, _] : new_extents_by_tp) {
        remove_extents_below_start_offset_for_tp(
          state, tidp, "Added replacement below start offset");
    }

    for (const auto& [t, t_req] : compaction_updates) {
        for (const auto& [p, compaction_update] : t_req) {
            model::topic_id_partition tidp{t, p};
            auto& p_state = state.topic_to_state[tidp.topic_id]
                              .pid_to_state[tidp.partition];
            if (!p_state.compaction_state.has_value()) {
                p_state.compaction_state.emplace();
            }
            if (!compaction_update.new_cleaned_ranges.empty()) {
                const auto& req_cleaned_ranges
                  = compaction_update.new_cleaned_ranges;
                for (const auto& req_cleaned_range : req_cleaned_ranges) {
                    [[maybe_unused]] auto inserted
                      = p_state.compaction_state->cleaned_ranges.insert(
                        req_cleaned_range.base_offset,
                        req_cleaned_range.last_offset);
                    dassert(
                      inserted,
                      "Invalid interval [{}, {}]",
                      req_cleaned_range.base_offset,
                      req_cleaned_range.last_offset);
                    if (req_cleaned_range.has_tombstones) {
                        [[maybe_unused]] auto inserted
                          = p_state.compaction_state->add(
                            compaction_state::cleaned_range_with_tombstones{
                              .base_offset = req_cleaned_range.base_offset,
                              .last_offset = req_cleaned_range.last_offset,
                              .cleaned_with_tombstones_at
                              = compaction_update.cleaned_at,
                            });
                        dassert(
                          inserted,
                          "Failed to insert cleaned range with tombstones: "
                          "[{}, "
                          "{}]",
                          req_cleaned_range.base_offset,
                          req_cleaned_range.last_offset);
                    }
                }
            }

            auto& cstate = *p_state.compaction_state;
            auto req_range_removed_tombstones
              = compaction_update.removed_tombstones_ranges.make_stream();
            while (req_range_removed_tombstones.has_next()) {
                auto cleaned_range = req_range_removed_tombstones.next();
                [[maybe_unused]] auto erased
                  = cstate.erase_contiguous_range_with_tombstones(
                    cleaned_range.base_offset, cleaned_range.last_offset);
                dassert(
                  erased,
                  "Failed to remove range: [{}, {}]",
                  cleaned_range.base_offset,
                  cleaned_range.last_offset);
            }
            ++p_state.compaction_epoch;
        }
    }
    return std::monostate{};
}

std::expected<replace_objects_update, stm_update_error>
replace_objects_update::build(
  const state& state,
  chunked_vector<new_object> objects,
  chunked_hash_map<model::topic_id_partition, compaction_state_update>
    compaction_updates) {
    chunked_hash_map<
      model::topic_id,
      chunked_hash_map<model::partition_id, compaction_state_update>>
      cmp_state_updates;
    for (auto& [tp, update] : compaction_updates) {
        cmp_state_updates[tp.topic_id][tp.partition] = std::move(update);
    }
    replace_objects_update update{
      .new_objects = std::move(objects),
      .compaction_updates = std::move(cmp_state_updates),
    };
    auto allowed = update.can_apply(state);
    if (!allowed.has_value()) {
        return std::unexpected(allowed.error());
    }
    return update;
}

std::expected<set_start_offset_update, stm_update_error>
set_start_offset_update::build(
  const state& state,
  const model::topic_id_partition& tp,
  kafka::offset o,
  bool* is_no_op) {
    set_start_offset_update update{
      .tp = tp,
      .new_start_offset = o,
    };
    auto allowed = update.can_apply(state, is_no_op);
    if (!allowed.has_value()) {
        return std::unexpected(allowed.error());
    }
    return update;
}

std::expected<std::monostate, stm_update_error>
set_start_offset_update::can_apply(const state& state, bool* is_no_op) {
    auto prt_ref = state.partition_state(tp);
    if (!prt_ref.has_value()) {
        return std::unexpected(stm_update_error(
          fmt::format("Partition {} not tracked by state", tp)));
    }
    auto& prt = prt_ref->get();
    if (new_start_offset > prt.next_offset) {
        return std::unexpected(stm_update_error(
          fmt::format(
            "Requested start offset for {} is above the next offset: {} > {}",
            tp,
            new_start_offset,
            prt.next_offset)));
    }
    if (is_no_op) {
        *is_no_op = new_start_offset <= prt.start_offset;
    }
    return std::monostate{};
}

std::expected<std::monostate, stm_update_error>
set_start_offset_update::apply(state& state) {
    bool is_no_op = false;
    auto allowed = can_apply(state, &is_no_op);
    if (!allowed.has_value()) {
        return std::unexpected(allowed.error());
    }
    if (is_no_op) {
        return std::monostate{};
    }
    auto& p_state
      = state.topic_to_state[tp.topic_id].pid_to_state[tp.partition];
    if (p_state.start_offset == new_start_offset) {
        return std::monostate{};
    }
    p_state.start_offset = new_start_offset;
    remove_extents_below_start_offset_for_tp(state, tp, "New start offset");

    // Now remove terms. Note that the removal should always leave at least one
    // term start, enough to cover `next_offset`, even if the log is empty.
    while (p_state.term_starts.size() > 1) {
        if (p_state.term_starts.begin()->start_offset < new_start_offset) {
            p_state.term_starts.erase(p_state.term_starts.begin());
        }
    }
    // Finally, remove any compaction state that falls below the start offset.
    if (p_state.compaction_state.has_value()) {
        auto& c_state = *p_state.compaction_state;
        c_state.truncate_with_new_start_offset(new_start_offset);
    }
    return std::monostate{};
}

std::expected<remove_objects_update, stm_update_error>
remove_objects_update::build(
  const state& state, chunked_vector<object_id> objects) {
    remove_objects_update update{
      .objects = std::move(objects),
    };
    auto allowed = update.can_apply(state);
    if (!allowed.has_value()) {
        return std::unexpected(allowed.error());
    }
    return update;
}

std::expected<std::monostate, stm_update_error>
remove_objects_update::can_apply(const state& state) {
    for (const auto oid : objects) {
        auto o_iter = state.objects.find(oid);
        if (o_iter == state.objects.end()) {
            continue;
        }
        const auto& obj_entry = o_iter->second;
        if (obj_entry.total_data_size != obj_entry.removed_data_size) {
            return std::unexpected(
              stm_update_error{fmt::format(
                "Object {} is still referenced: removed_data_size: {}, "
                "total_data_size: {}",
                oid,
                obj_entry.removed_data_size,
                obj_entry.total_data_size)});
        }
    }
    return std::monostate{};
}

std::expected<std::monostate, stm_update_error>
remove_objects_update::apply(state& state) {
    auto allowed = can_apply(state);
    if (!allowed.has_value()) {
        return std::unexpected(allowed.error());
    }
    for (const auto oid : objects) {
        auto o_iter = state.objects.find(oid);
        if (o_iter == state.objects.end()) {
            continue;
        }
        state.objects.erase(o_iter);
    }
    return std::monostate{};
}

std::expected<remove_topics_update, stm_update_error>
remove_topics_update::build(
  const state& state, chunked_vector<model::topic_id> topics) {
    remove_topics_update update{
      .topics = std::move(topics),
    };
    auto allowed = update.can_apply(state);
    if (!allowed.has_value()) {
        return std::unexpected(allowed.error());
    }
    return update;
}

std::expected<std::monostate, stm_update_error>
remove_topics_update::can_apply(const state&) {
    return std::monostate{};
}

std::expected<std::monostate, stm_update_error>
remove_topics_update::apply(state& state) {
    auto allowed = can_apply(state);
    if (!allowed.has_value()) {
        return std::unexpected(allowed.error());
    }
    for (const auto tid : topics) {
        auto t_it = state.topic_to_state.find(tid);
        if (t_it == state.topic_to_state.end()) {
            continue;
        }
        const auto& t_state = t_it->second;
        for (const auto& [pid, p_state] : t_state.pid_to_state) {
            for (const auto& e : p_state.extents) {
                auto o_it = state.objects.find(e.oid);
                if (o_it == state.objects.end()) {
                    continue;
                }
                auto& [oid, obj_entry] = *o_it;
                obj_entry.removed_data_size += e.len;
            }
        }
        state.topic_to_state.erase(t_it);
    }
    return std::monostate{};
}

std::expected<std::monostate, stm_update_error>
preregister_objects_update::can_apply(const state& s) {
    for (const auto& oid : object_ids) {
        if (s.objects.contains(oid)) {
            return std::unexpected(
              stm_update_error{fmt::format("object {} already exists", oid)});
        }
    }
    return std::monostate{};
}

std::expected<std::monostate, stm_update_error>
preregister_objects_update::apply(state& s) {
    auto allowed = can_apply(s);
    if (!allowed.has_value()) {
        return std::unexpected(allowed.error());
    }
    for (const auto& oid : object_ids) {
        s.objects[oid] = object_entry{
          .last_updated = registered_at,
          .is_preregistration = true,
        };
    }
    return std::monostate{};
}

std::expected<std::monostate, stm_update_error>
expire_preregistered_objects_update::can_apply(const state&) {
    return std::monostate{};
}

std::expected<std::monostate, stm_update_error>
expire_preregistered_objects_update::apply(state& s) {
    for (const auto& oid : object_ids) {
        auto it = s.objects.find(oid);
        if (it != s.objects.end()) {
            it->second.is_preregistration = false;
        }
    }
    return std::monostate{};
}

} // namespace cloud_topics::l1
