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

#include "model/fundamental.h"

namespace experimental::cloud_topics::l1 {

namespace {

using extent_iter_t = partition_state::extent_set_t::const_iterator;
struct extent_range {
    extent_iter_t base_it;
    extent_iter_t last_it;
};
std::optional<extent_range> get_range(
  const partition_state::extent_set_t& extents,
  kafka::offset base,
  kafka::offset last) {
    auto base_it = std::ranges::lower_bound(
      extents, base, std::less<>{}, &extent::base_offset);
    if (base_it == extents.end() || base_it->base_offset != base) {
        return std::nullopt;
    }
    // Check that the range's last offset aligns with an existing extent.
    auto last_it = std::ranges::lower_bound(
      extents, last, std::less<>{}, &extent::last_offset);
    if (last_it == extents.end() || last_it->last_offset != last) {
        return std::nullopt;
    }
    return extent_range{base_it, last_it};
}

} // namespace

void new_object::collect_extents_by_tidp(sorted_extents_by_tidp_t* ret) const {
    for (const auto& [tid, p_extents] : extent_metas) {
        for (const auto& [p, extent_meta] : p_extents) {
            auto& ret_extents = (*ret)[model::topic_id_partition(tid, p)];
            ret_extents.insert(extent{
              .base_offset = extent_meta.base_offset,
              .last_offset = extent_meta.last_offset,
              .max_timestamp = extent_meta.max_timestamp,
              .filepos = extent_meta.filepos,
              .len = extent_meta.len,
              .oid = oid,
            });
        }
    }
}

std::expected<add_objects_update, stm_update_error> add_objects_update::build(
  const state& state,
  chunked_vector<new_object> objects,
  chunked_hash_map<model::topic_id_partition, kafka::offset>* corrections) {
    add_objects_update update{
      .new_objects = std::move(objects),
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
    new_object::sorted_extents_by_tidp_t new_extents;
    for (const auto& o : new_objects) {
        if (state.objects.contains(o.oid)) {
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
            if (extent.base_offset != expected_next) {
                return std::unexpected(stm_update_error(fmt::format(
                  "Input object breaks partition {} offset ordering: "
                  "expected next: {}, actual: {}",
                  tidp,
                  expected_next,
                  extent.base_offset())));
            }
            expected_next = kafka::next_offset(extent.last_offset);
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
    new_object::sorted_extents_by_tidp_t extents_by_tp;
    for (const auto& o : new_objects) {
        o.collect_extents_by_tidp(&extents_by_tp);
        state.objects.emplace(
          o.oid,
          object_entry{
            .total_data_size = 0,
            .removed_data_size = 0,
            .footer_pos = o.footer_pos,
          });
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
            }
        } else {
            // The incoming extents don't align with the next position. "Drop"
            // them all.
            for (const auto& extent : extents) {
                auto& obj_entry = state.objects[extent.oid];
                obj_entry.removed_data_size += extent.len;
                obj_entry.total_data_size += extent.len;
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
    new_object::sorted_extents_by_tidp_t new_extents_by_tp;
    for (const auto& o : new_objects) {
        if (state.objects.contains(o.oid)) {
            return std::unexpected(
              stm_update_error{fmt::format("Object {} already exists", o.oid)});
        }
        o.collect_extents_by_tidp(&new_extents_by_tp);
    }

    for (const auto& [tidp, new_prt_extents] : new_extents_by_tp) {
        auto req_base = new_prt_extents.begin()->base_offset;
        auto req_last = std::prev(new_prt_extents.end())->last_offset;

        auto p_state = state.partition_state(tidp);
        if (!p_state) {
            return std::unexpected(stm_update_error(
              fmt::format("Partition {} not tracked by state", tidp)));
        }

        // Check that the new range's offset aligns with existing extents.
        const auto& prt = p_state->get();
        auto iters = get_range(prt.extents, req_base, req_last);
        if (!iters.has_value()) {
            return std::unexpected(stm_update_error(fmt::format(
              "Partition {} doesn't contain extents that span exactly [{}, "
              "{}]",
              tidp,
              req_base,
              req_last)));
        }

        // Check that the new range of extents is contiguous, which in turn
        // ensures the resulting total set of extents will be contiguous.
        const auto& [base_it, last_it] = *iters;
        auto expected_next = base_it->base_offset;
        for (const auto& new_extent : new_prt_extents) {
            if (new_extent.base_offset != expected_next) {
                return std::unexpected(stm_update_error(fmt::format(
                  "Input object breaks partition {} offset ordering: "
                  "expected next: {}, actual: {}",
                  tidp,
                  expected_next,
                  new_extent.base_offset)));
            }
            expected_next = kafka::next_offset(new_extent.last_offset);
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
            // Validate that any cleaned ranges actually correspond to the new
            // extents.
            auto new_extent_iter = new_extents_by_tp.find(tidp);
            if (new_extent_iter == new_extents_by_tp.end()) {
                return std::unexpected(stm_update_error(fmt::format(
                  "New cleaned range does not refer to partition with extent: "
                  "{}",
                  tidp)));
            }
            if (compaction_update.new_cleaned_range.has_value()) {
                const auto& req_cleaned_range
                  = *compaction_update.new_cleaned_range;

                // Check that the new extents span the start of the log to the
                // end of the new clean range.
                auto& [_, new_extents] = *new_extent_iter;
                auto req_last = new_extents.rbegin()->last_offset;
                if (req_cleaned_range.last_offset > req_last) {
                    return std::unexpected(stm_update_error(fmt::format(
                      "Cleaned range for {} does not match requested new "
                      "extents' last_offset {} > {}",
                      tidp,
                      req_cleaned_range.last_offset,
                      req_last)));
                }
                auto req_extents_base = new_extents.begin()->base_offset;
                if (req_extents_base > req_cleaned_range.base_offset) {
                    return std::unexpected(stm_update_error(fmt::format(
                      "Cleaned range start_offset for {} is not covered by "
                      "extents: {} > {}",
                      tidp,
                      req_extents_base,
                      req_cleaned_range.base_offset)));
                }

                // Check that the extents replace down to the start of the log.
                // By definition, this is a requirement of cleaning the log.
                if (req_extents_base > p_state.start_offset) {
                    return std::unexpected(stm_update_error(fmt::format(
                      "Cleaned range for {} does not replace to the beginning "
                      "of the log: {} > {}",
                      tidp,
                      req_extents_base,
                      p_state.start_offset)));
                }

                // Check that ranges with tombstones don't overlap with existing
                // ranges with tombstones.
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
                    return std::unexpected(stm_update_error(fmt::format(
                      "Cleaned range for {} has tombstones and overlaps with "
                      "an existing cleaned range with tombstones: [{}, {}]",
                      tidp,
                      req_cleaned_range.base_offset,
                      req_cleaned_range.last_offset)));
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
                    return std::unexpected(stm_update_error(fmt::format(
                      "Tombstone-removed range [{}, {}] for {} is not tracked "
                      "as having tombstones",
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
    new_object::sorted_extents_by_tidp_t new_extents_by_tp;
    for (const auto& o : new_objects) {
        o.collect_extents_by_tidp(&new_extents_by_tp);
        state.objects.emplace(
          o.oid,
          object_entry{
            .total_data_size = 0,
            .removed_data_size = 0,
            .footer_pos = o.footer_pos,
          });
    }
    for (const auto& [tidp, new_extents] : new_extents_by_tp) {
        auto& p_state
          = state.topic_to_state[tidp.topic_id].pid_to_state[tidp.partition];
        auto requested_base = new_extents.begin()->base_offset;
        auto requested_last = new_extents.rbegin()->last_offset;
        auto iters = get_range(p_state.extents, requested_base, requested_last);
        auto [base_it, last_it] = *iters;
        auto end_it = std::next(last_it);
        for (auto iter = base_it; iter != end_it; ++iter) {
            auto& old_extent = *iter;
            state.objects[old_extent.oid].removed_data_size += old_extent.len;
        }

        p_state.extents.erase(base_it, end_it);
        for (const auto& e : new_extents) {
            p_state.extents.emplace(e);
        }
        // NOTE: we don't need to update the start or next offsets since we've
        // validated that the new extents replace exact ranges.

        for (const auto& extent : new_extents) {
            state.objects[extent.oid].total_data_size += extent.len;
        }
    }
    for (const auto& [t, t_req] : compaction_updates) {
        for (const auto& [p, compaction_update] : t_req) {
            model::topic_id_partition tidp{t, p};
            auto& p_state = state.topic_to_state[tidp.topic_id]
                              .pid_to_state[tidp.partition];
            if (!p_state.compaction_state.has_value()) {
                p_state.compaction_state.emplace();
            }
            if (compaction_update.new_cleaned_range.has_value()) {
                const auto& req_cleaned_range
                  = *compaction_update.new_cleaned_range;
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
                      "Failed to insert cleaned range with tombstones: [{}, "
                      "{}]",
                      req_cleaned_range.base_offset,
                      req_cleaned_range.last_offset);
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

} // namespace experimental::cloud_topics::l1
