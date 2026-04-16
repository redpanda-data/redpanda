/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/metastore/lsm/state_update.h"

#include "cloud_topics/level_one/metastore/lsm/keys.h"
#include "cloud_topics/level_one/metastore/lsm/values.h"
#include "cloud_topics/level_one/metastore/state_update.h"
#include "cloud_topics/level_one/metastore/state_update_utils.h"

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/as_future.hh>

namespace cloud_topics::l1 {

namespace {

using enum db_update_errc;

template<typename... T>
db_update_error wrap_read_err(
  state_reader::error e, fmt::format_string<T...> msg, T&&... args) {
    switch (e.e) {
    case state_reader::errc::io_error:
        return std::move(e).wrap(
          io_error, std::move(msg), std::forward<T>(args)...);
    case state_reader::errc::corruption:
        return std::move(e).wrap(
          corruption, std::move(msg), std::forward<T>(args)...);
    case state_reader::errc::shutting_down:
        return std::move(e).wrap(
          shutting_down, std::move(msg), std::forward<T>(args)...);
    }
}

// Checks that each new object is pre-registered (exists with
// is_preregistration=true), then collects extents and committed object
// entries into the output maps.
ss::future<std::expected<void, db_update_error>>
validate_preregistered_and_collect(
  const chunked_vector<new_object>& new_objects,
  state_reader& state,
  sorted_extents_by_tidp_t& out_extents,
  chunked_hash_map<object_id, object_entry>& out_objects) {
    for (const auto& o : new_objects) {
        auto object_res = co_await state.get_object(o.oid);
        if (!object_res.has_value()) {
            co_return std::unexpected(db_update_error(
              invalid_update, fmt::format("Error getting object {}", o.oid)));
        }
        if (
          !object_res->has_value() || !object_res->value().is_preregistration) {
            co_return std::unexpected(db_update_error(
              invalid_update,
              fmt::format("object {} not pre-registered", o.oid)));
        }
        auto data_size = o.collect_extents_by_tidp(&out_extents);
        out_objects.emplace(
          o.oid,
          object_entry{
            .total_data_size = data_size,
            .removed_data_size = 0,
            .footer_pos = o.footer_pos,
            .object_size = o.object_size,
            .last_updated = model::timestamp::now(),
            .is_preregistration = false,
          });
    }
    co_return std::expected<void, db_update_error>{};
}

// Returns the extents of the given partition that align exactly with the input
// intervals. If there any of the intervals don't align with exact intervals,
// returns an error.
//
// Intervals that start below start_offset are adjusted: we find the first
// extent at or above start_offset and use that as the range start. This allows
// replacement extents referencing offsets that have been truncated.
ss::future<std::expected<void, db_update_error>> collect_exact_intervals(
  const model::topic_id_partition& tidp,
  const chunked_vector<offset_interval_set::interval>& intervals,
  kafka::offset start_offset,
  state_reader& state,
  chunked_vector<ss::sstring>& out_extent_keys,
  chunked_hash_map<object_id, size_t>& out_sizes) {
    for (const auto& interval : intervals) {
        auto base = interval.base_offset;
        auto last = interval.last_offset;
        if (last < start_offset) {
            // This whole interval is below the start offset. We don't need to
            // find extents for it, as it will be dropped.
            continue;
        }

        // If base < start_offset, we are considering replacing an extent that
        // starts below the log start offset. Adjust the base offset to point
        // at the first extent; we'll collect all extents below the start
        // offset for removal.
        if (base < start_offset) {
            auto first_extent_res = co_await state.get_extent_ge(
              tidp, kafka::offset(0));
            if (!first_extent_res.has_value()) {
                co_return std::unexpected(wrap_read_err(
                  std::move(first_extent_res.error()),
                  "Error getting first extent >= {} for {}",
                  start_offset,
                  tidp));
            }
            if (!first_extent_res->has_value()) {
                // There are no extents at all, and therefore we cannot collect
                // extents to replace.
                co_return std::unexpected(db_update_error(
                  invalid_update,
                  fmt::format(
                    "Partition {} doesn't contain extents that span exactly "
                    "[{}, {}]",
                    tidp,
                    base,
                    last)));
            }
            base = first_extent_res->value().base_offset;
        }

        auto range_res = co_await state.get_extent_range(tidp, base, last);

        if (!range_res.has_value()) {
            co_return std::unexpected(wrap_read_err(
              std::move(range_res.error()),
              "Error getting extent range for {} [{}, {}]",
              tidp,
              base,
              last));
        }
        if (!range_res.value().has_value()) {
            co_return std::unexpected(db_update_error(
              invalid_update,
              fmt::format(
                "Partition {} doesn't contain extents that span exactly "
                "[{}, {}]",
                tidp,
                base,
                last)));
        }

        auto extent_gen = range_res.value()->get_rows();
        while (auto extent_res = co_await extent_gen()) {
            if (!extent_res->get().has_value()) {
                co_return std::unexpected(wrap_read_err(
                  std::move(extent_res->get().error()),
                  "Error iterating through {} extents in range [{}, {}]",
                  tidp,
                  base,
                  last));
            }
            auto& row = extent_res->get().value();
            out_sizes[row.val.oid] += row.val.len;
            out_extent_keys.push_back(std::move(row.key));
        }
    }
    co_return std::expected<void, db_update_error>{};
}

ss::future<std::expected<void, db_update_error>> merge_compaction_state(
  const model::topic_id_partition& tidp,
  const compaction_state_update& comp_update,
  state_reader& state,
  metadata_row_value& inout_meta,
  compaction_state& out_state) {
    if (inout_meta.compaction_epoch != comp_update.expected_compaction_epoch) {
        co_return std::unexpected(db_update_error(
          invalid_update,
          fmt::format(
            "Compaction epoch mismatch for {}: expected {}, got {}",
            tidp,
            comp_update.expected_compaction_epoch,
            inout_meta.compaction_epoch)));
    }
    inout_meta.compaction_epoch = partition_state::compaction_epoch_t{
      inout_meta.compaction_epoch() + 1};

    auto comp_res = co_await state.get_compaction_metadata(tidp);
    if (!comp_res.has_value()) {
        co_return std::unexpected(wrap_read_err(
          std::move(comp_res.error()),
          "Error getting compaction metadata for {}",
          tidp));
    }

    // Validate and apply new cleaned ranges.
    auto merged_state = comp_res.value().value_or(compaction_state{});
    if (!comp_update.new_cleaned_ranges.empty()) {
        const auto& req_cleaned_ranges = comp_update.new_cleaned_ranges;

        // Check that ranges with tombstones don't overlap.
        for (const auto& req_cleaned_range : req_cleaned_ranges) {
            if (
              req_cleaned_range.has_tombstones
              && !merged_state.may_add(
                compaction_state::cleaned_range_with_tombstones{
                  .base_offset = req_cleaned_range.base_offset,
                  .last_offset = req_cleaned_range.last_offset,
                  .cleaned_with_tombstones_at = comp_update.cleaned_at,
                })) {
                co_return std::unexpected(db_update_error(
                  invalid_update,
                  fmt::format(
                    "Cleaned range for {} has tombstones and overlaps "
                    "with an existing cleaned range with tombstones: "
                    "[{}, {}]",
                    tidp,
                    req_cleaned_range.base_offset,
                    req_cleaned_range.last_offset)));
            }
        }

        // Apply new cleaned ranges.
        for (const auto& req_cleaned_range : req_cleaned_ranges) {
            auto inserted = merged_state.cleaned_ranges.insert(
              req_cleaned_range.base_offset, req_cleaned_range.last_offset);
            vassert(
              inserted,
              "Invalid interval [{}, {}]",
              req_cleaned_range.base_offset,
              req_cleaned_range.last_offset);

            if (req_cleaned_range.has_tombstones) {
                auto inserted_tomb = merged_state.add(
                  compaction_state::cleaned_range_with_tombstones{
                    .base_offset = req_cleaned_range.base_offset,
                    .last_offset = req_cleaned_range.last_offset,
                    .cleaned_with_tombstones_at = comp_update.cleaned_at,
                  });
                vassert(
                  inserted_tomb,
                  "Failed to insert cleaned range with tombstones: "
                  "[{}, {}]",
                  req_cleaned_range.base_offset,
                  req_cleaned_range.last_offset);
            }
        }
    }
    // Validate and remove tombstone ranges.
    auto req_range_removed_tombstones
      = comp_update.removed_tombstones_ranges.make_stream();
    while (req_range_removed_tombstones.has_next()) {
        auto req_range = req_range_removed_tombstones.next();
        if (!merged_state.has_contiguous_range_with_tombstones(
              req_range.base_offset, req_range.last_offset)) {
            co_return std::unexpected(db_update_error(
              invalid_update,
              fmt::format(
                "Tombstone-removed range [{}, {}] for {} is not "
                "tracked as having tombstones",
                req_range.base_offset,
                req_range.last_offset,
                tidp)));
        }
        auto erased = merged_state.erase_contiguous_range_with_tombstones(
          req_range.base_offset, req_range.last_offset);
        vassert(
          erased,
          "Failed to remove range: [{}, {}]",
          req_range.base_offset,
          req_range.last_offset);
    }

    out_state = std::move(merged_state);
    co_return std::expected<void, db_update_error>{};
}

/*
 * Helper function which will immediately return a pointer to metadata row if it
 * exists in the index for the given tidp. Otherwise, the tidp metadata is
 * fetched first and then the pointer is returned.
 *
 * Important: the returned pointer is invalidated by index mutations.
 */
ss::future<std::expected<metadata_row_value*, db_update_error>>
get_metadata_for_update(
  state_reader& state,
  chunked_hash_map<model::topic_id_partition, metadata_row_value>& index,
  const model::topic_id_partition& tidp,
  std::string_view context) {
    if (auto it = index.find(tidp); it != index.end()) {
        co_return &it->second;
    }

    auto meta_res = co_await state.get_metadata(tidp);
    if (!meta_res.has_value()) {
        co_return std::unexpected(wrap_read_err(
          std::move(meta_res.error()),
          "Error getting metadata for {} during {}",
          tidp,
          context));
    }

    if (!meta_res.value().has_value()) {
        co_return std::unexpected(db_update_error(
          invalid_update,
          fmt::format(
            "Partition {} not tracked by state during {}", tidp, context)));
    }

    auto res = index.insert_or_assign(tidp, meta_res.value().value());
    co_return &res.first->second;
}

// Goes through the given removed objects and builds a map of object_entries of
// corresponding objects with the provided sizes removed.
ss::future<
  std::expected<chunked_hash_map<object_id, object_entry>, db_update_error>>
build_object_removal_entries(
  state_reader& state,
  const chunked_hash_map<object_id, size_t>& removed_sizes_by_oid) {
    chunked_hash_map<object_id, object_entry> updated_old_objects;
    for (const auto& [oid, removed_size] : removed_sizes_by_oid) {
        auto obj_res = co_await state.get_object(oid);
        if (!obj_res.has_value()) {
            co_return std::unexpected(wrap_read_err(
              std::move(obj_res.error()), "Error getting object {}", oid));
        }
        if (obj_res.value().has_value()) {
            auto obj_entry = obj_res.value().value();
            if (obj_entry.is_preregistration) {
                co_return std::unexpected(wrap_read_err(
                  state_reader::error(
                    state_reader::errc::corruption,
                    "Unexpected preregistered object: {}",
                    oid),
                  "Error getting object for removal"));
            }
            obj_entry.removed_data_size += removed_size;
            obj_entry.last_updated = model::timestamp::now();
            updated_old_objects[oid] = obj_entry;
        }
        // If object doesn't exist, skip it (benign).
    }
    co_return updated_old_objects;
}

} // namespace

ss::future<std::expected<void, db_update_error>>
add_objects_db_update::build_rows(
  state_reader& state,
  chunked_vector<write_batch_row>& out,
  chunked_hash_map<model::topic_id_partition, kafka::offset>* corrections)
  const {
    auto validate_res = validate_inputs();
    if (!validate_res.has_value()) {
        co_return std::unexpected(std::move(validate_res.error()));
    }
    sorted_extents_by_tidp_t new_extents_by_tp;
    chunked_hash_map<object_id, object_entry> new_objects_by_oid;
    auto new_extents_res = co_await validate_preregistered_and_collect(
      new_objects, state, new_extents_by_tp, new_objects_by_oid);
    if (!new_extents_res.has_value()) {
        co_return std::unexpected(std::move(new_extents_res.error()));
    }

    chunked_hash_map<model::topic_id_partition, kafka::offset>
      corrected_next_offsets;
    chunked_hash_map<model::topic_id_partition, chunked_vector<extent>>
      verified_extents;
    chunked_hash_map<model::topic_id_partition, metadata_row_value>
      verified_meta_vals;
    for (const auto& [tidp, extents] : new_extents_by_tp) {
        // TODO: maybe we need some mount operation that adopts a partition log
        // and allows it to start a specific offset.
        auto meta_res = co_await state.get_metadata(tidp);
        if (!meta_res.has_value()) {
            co_return std::unexpected(wrap_read_err(
              std::move(meta_res.error()),
              "Error getting metadata for {}",
              tidp));
        }
        auto opt = meta_res.value();
        auto expected_next = opt ? opt->next_offset : kafka::offset{0};

        if (extents.begin()->base_offset != expected_next) {
            // If the start of the new extents for this partition aren't
            // aligned, allow the operation to succeed, but the expectation is
            // when applying, we'll "drop" these extents. Account for them as
            // removed data.
            for (const auto& extent : extents) {
                new_objects_by_oid[extent.oid].removed_data_size += extent.len;
            }
            corrected_next_offsets[tidp] = expected_next;
            continue;
        }
        size_t extent_size_sum = 0;
        for (const auto& extent : extents) {
            verified_extents[tidp].push_back(extent);
            extent_size_sum += extent.len;
        }
        verified_meta_vals[tidp] = metadata_row_value{
          .start_offset = opt ? opt->start_offset : kafka::offset{0},
          .next_offset = kafka::next_offset(extents.rbegin()->last_offset),
          .compaction_epoch = opt ? opt->compaction_epoch
                                  : partition_state::compaction_epoch_t{0},
          .size = (opt ? opt->size : 0) + extent_size_sum,
          .num_extents = (opt ? opt->num_extents : 0) + extents.size(),
        };
    }
    // Now that we've validated the offsets of our extents, validate the terms
    // for the accepted extents.
    chunked_hash_map<model::topic_id_partition, absl::btree_set<term_start>>
      verified_terms;
    for (const auto& [tp, req_entries] : new_terms) {
        if (corrected_next_offsets.contains(tp)) {
            continue;
        }
        // First do a basic check that the incoming term entries can be
        // appended to our state without violating ordering requirements.
        auto term_res = co_await state.get_max_term(tp);
        if (!term_res.has_value()) {
            co_return std::unexpected(wrap_read_err(
              std::move(term_res.error()),
              "Error getting max term for {}",
              tp));
        }
        auto term_opt = term_res.value();
        if (term_opt.has_value()) {
            auto req_first_entry = req_entries.begin();
            // NOTE: it's valid for the first requested term to be equal to the
            // last term (e.g. if leadership has not changed). The same cannot
            // be said about offsets, hence the difference in comparator.
            if (req_first_entry->term_id < term_opt->term_id) {
                co_return std::unexpected(db_update_error(
                  invalid_update,
                  fmt::format(
                    "New term for {} must be >= last term: {} < {}",
                    tp,
                    req_first_entry->term_id,
                    term_opt->term_id)));
            }
            if (req_first_entry->start_offset <= term_opt->start_offset) {
                co_return std::unexpected(db_update_error(
                  invalid_update,
                  fmt::format(
                    "New term for {} must start after last term: {} <= {}",
                    tp,
                    req_first_entry->start_offset,
                    term_opt->start_offset)));
            }
        }
        auto new_term_it = req_entries.begin();
        if (term_opt.has_value() && new_term_it->term_id == term_opt->term_id) {
            // If the first added term matches the back of the latest
            // tracked term, it isn't a new term.
            ++new_term_it;
        }
        verified_terms[tp].insert(new_term_it, req_entries.end());
    }
    if (corrections) {
        *corrections = std::move(corrected_next_offsets);
    }
    // Generate the rows.
    for (const auto& [oid, entry] : new_objects_by_oid) {
        out.emplace_back(
          write_batch_row{
            .key = object_row_key::encode(oid),
            .value = serde::to_iobuf(
              object_row_value{
                .object = entry,
              }),
          });
    }
    for (const auto& [tidp, entries] : verified_terms) {
        for (const auto& entry : entries) {
            out.emplace_back(
              write_batch_row{
                .key = term_row_key::encode(tidp, entry.term_id),
                .value = serde::to_iobuf(
                  term_row_value{
                    .term_start_offset = entry.start_offset,
                  }),
              });
        }
    }
    for (const auto& [tidp, entries] : verified_extents) {
        for (const auto& entry : entries) {
            out.emplace_back(
              write_batch_row{
                .key = extent_row_key::encode(tidp, entry.base_offset),
                .value = serde::to_iobuf(
                  extent_row_value{
                    .last_offset = entry.last_offset,
                    .max_timestamp = entry.max_timestamp,
                    .filepos = entry.filepos,
                    .len = entry.len,
                    .oid = entry.oid,
                  }),
              });
        }
    }
    for (const auto& [tidp, entry] : verified_meta_vals) {
        out.emplace_back(
          write_batch_row{
            .key = metadata_row_key::encode(tidp),
            .value = serde::to_iobuf(entry),
          });
    }
    co_return std::expected<void, db_update_error>{};
}

std::expected<void, db_update_error>
add_objects_db_update::validate_inputs() const {
    if (new_objects.empty()) {
        return std::unexpected(
          db_update_error(invalid_input, "No objects requested"));
    }
    if (new_terms.empty()) {
        return std::unexpected(
          db_update_error(invalid_input, "Missing term info in request"));
    }
    sorted_extents_by_tidp_t new_extents;
    for (const auto& o : new_objects) {
        o.collect_extents_by_tidp(&new_extents);
    }
    for (const auto& [tidp, extents] : new_extents) {
        if (!new_terms.contains(tidp)) {
            return std::unexpected(db_update_error(
              invalid_input, fmt::format("Missing term info for {}", tidp)));
        }
        auto expected_next = extents.begin()->base_offset;
        for (const auto& extent : extents) {
            if (extent.base_offset > extent.last_offset) {
                return std::unexpected(db_update_error(
                  invalid_input,
                  fmt::format(
                    "Input object has inverted extent for partition {}: "
                    "base_offset {} > last_offset {}",
                    tidp,
                    extent.base_offset,
                    extent.last_offset)));
            }
            if (extent.base_offset != expected_next) {
                return std::unexpected(db_update_error(
                  invalid_input,
                  fmt::format(
                    "Input object breaks partition {} offset ordering: "
                    "expected next: {}, actual: {}",
                    tidp,
                    expected_next,
                    extent.base_offset())));
            }
            expected_next = kafka::next_offset(extent.last_offset);
        }
    }
    for (const auto& [tidp, terms] : new_terms) {
        if (terms.empty()) {
            return std::unexpected(db_update_error(
              invalid_input,
              fmt::format("Empty terms requested for {}", tidp)));
        }
        auto extents_it = new_extents.find(tidp);
        if (extents_it == new_extents.end() || extents_it->second.empty()) {
            return std::unexpected(db_update_error(
              invalid_input,
              fmt::format(
                "Terms provided for a partition that has no extents: {}",
                tidp)));
        }
        auto new_extents_start_offset
          = extents_it->second.begin()->base_offset();
        auto new_terms_start_offset = terms.begin()->start_offset;
        if (new_extents_start_offset != new_terms_start_offset) {
            return std::unexpected(db_update_error(
              invalid_input,
              fmt::format(
                "Extent start and term start do not match for {}: {} != {}",
                tidp,
                new_extents_start_offset,
                new_terms_start_offset)));
        }
        auto new_extents_last_offset = extents_it->second.rbegin()->last_offset;
        auto new_terms_last_start_offset = terms.back().start_offset;
        if (new_extents_last_offset < new_terms_last_start_offset) {
            return std::unexpected(db_update_error(
              invalid_input,
              fmt::format(
                "Extents end below a requested new term for {}: {} < {}",
                tidp,
                new_extents_last_offset,
                new_terms_last_start_offset)));
        }
        // Now check that the the term entries themselves (both terms and
        // offsets) are in increasing order.
        auto max_term_so_far = model::term_id{-1};
        auto max_offset_so_far = kafka::offset{-1};
        for (const auto& entry : terms) {
            if (
              entry.term_id <= max_term_so_far
              || entry.start_offset <= max_offset_so_far) {
                return std::unexpected(db_update_error(
                  invalid_input,
                  fmt::format(
                    "Invalid term for {}: term={}, offset={}, "
                    "max_term_so_far={}, max_offset_so_far={}",
                    tidp,
                    entry.term_id,
                    entry.start_offset,
                    max_term_so_far,
                    max_offset_so_far)));
            }
            max_term_so_far = entry.term_id;
            max_offset_so_far = entry.start_offset;
        }
    }
    return std::expected<void, db_update_error>{};
}

ss::future<std::expected<void, db_update_error>>
replace_objects_db_update::build_rows(
  state_reader& state, chunked_vector<write_batch_row>& out) const {
    auto validate_res = validate_inputs();
    if (!validate_res.has_value()) {
        co_return std::unexpected(std::move(validate_res.error()));
    }
    sorted_extents_by_tidp_t new_extents_by_tp;
    chunked_hash_map<object_id, object_entry> new_objects_map;
    auto new_extents_res = co_await validate_preregistered_and_collect(
      new_objects, state, new_extents_by_tp, new_objects_map);
    if (!new_extents_res.has_value()) {
        co_return std::unexpected(std::move(new_extents_res.error()));
    }

    // Calculate contiguous intervals and validate that they align with
    // appropriate extents.
    auto contiguous_intervals_res = contiguous_intervals_for_extents(
      new_extents_by_tp);
    if (!contiguous_intervals_res.has_value()) {
        co_return std::unexpected(db_update_error(
          invalid_input, std::move(contiguous_intervals_res.error())));
    }
    const auto& contiguous_intervals_by_tp = contiguous_intervals_res.value();
    chunked_hash_map<object_id, size_t> old_extent_sizes_by_oid;
    chunked_hash_map<model::topic_id_partition, chunked_vector<ss::sstring>>
      extent_keys_to_delete;
    chunked_hash_map<model::topic_id_partition, kafka::offset>
      start_offsets_by_tp;

    /*
     * The `updated_metadata` map tracks metadata that needs to be updated. Use
     * the `get_metadata_for_update` helper to index into `updated_metadata`
     * which will fetch the current metadata state if necessary.
     */
    chunked_hash_map<model::topic_id_partition, metadata_row_value>
      updated_metadata;

    for (const auto& [tidp, intervals] : contiguous_intervals_by_tp) {
        auto meta_res = co_await get_metadata_for_update(
          state, updated_metadata, tidp, "contiguous interval processing");
        if (!meta_res.has_value()) {
            co_return std::unexpected(std::move(meta_res.error()));
        }
        start_offsets_by_tp[tidp] = meta_res.value()->start_offset;
        // Track removed sizes for this partition specifically.
        chunked_hash_map<object_id, size_t> tidp_removed_sizes;
        auto exact_intervals_res = co_await collect_exact_intervals(
          tidp,
          intervals,
          meta_res.value()->start_offset,
          state,
          extent_keys_to_delete[tidp],
          tidp_removed_sizes);
        if (!exact_intervals_res.has_value()) {
            co_return std::unexpected(std::move(exact_intervals_res.error()));
        }
        // Accumulate total removed size for this partition, and then fold the
        // updates for this partition back into old_extent_sizes_by_oid which
        // tracks per object instead of per partition. The total is then
        // subtracted off of the metadata which will later be updated in the db.
        ssize_t removed_for_tidp = 0;
        for (const auto& [oid, sz] : tidp_removed_sizes) {
            removed_for_tidp += sz;
            old_extent_sizes_by_oid[oid] += sz;
        }
        auto prev_size = static_cast<ssize_t>(meta_res.value()->size);
        meta_res.value()->size = std::max(
          ssize_t(0), prev_size - removed_for_tidp);
        meta_res.value()->num_extents -= std::min(
          meta_res.value()->num_extents, extent_keys_to_delete[tidp].size());
    }

    // Update existing object entries to indicate the removal of data from
    // replaced extents.
    auto updated_old_objects_res = co_await build_object_removal_entries(
      state, old_extent_sizes_by_oid);
    if (!updated_old_objects_res.has_value()) {
        co_return std::unexpected(updated_old_objects_res.error());
    }
    auto& updated_old_objects = updated_old_objects_res.value();

    chunked_hash_map<model::topic_id_partition, compaction_state>
      merged_compaction_states;

    for (const auto& [t, p_updates] : compaction_updates) {
        for (const auto& [p, comp_update] : p_updates) {
            model::topic_id_partition tidp{t, p};
            auto meta = co_await get_metadata_for_update(
              state, updated_metadata, tidp, "compaction");
            if (!meta.has_value()) {
                co_return std::unexpected(std::move(meta.error()));
            }
            auto merge_res = co_await merge_compaction_state(
              tidp,
              comp_update,
              state,
              *meta.value(),
              merged_compaction_states[tidp]);
            if (!merge_res.has_value()) {
                co_return std::unexpected(std::move(merge_res.error()));
            }

            // If there are new cleaned ranges, check that the extents replace
            // down to the start of the log. By definition, this is a
            // requirement of cleaning the log.
            if (!comp_update.new_cleaned_ranges.empty()) {
                auto new_extent_iter = new_extents_by_tp.find(tidp);
                if (new_extent_iter != new_extents_by_tp.end()) {
                    auto req_extents_base
                      = new_extent_iter->second.begin()->base_offset;
                    auto start_offset = updated_metadata[tidp].start_offset;
                    if (req_extents_base > start_offset) {
                        co_return std::unexpected(db_update_error(
                          invalid_update,
                          fmt::format(
                            "Cleaned range for {} does not replace to the "
                            "beginning of the log: {} > {}",
                            tidp,
                            req_extents_base,
                            start_offset)));
                    }
                }
            }
        }
    }

    // Generate the rows.
    chunked_hash_set<ss::sstring> added_extent_keys;
    for (const auto& [tidp, extents] : new_extents_by_tp) {
        auto start_it = start_offsets_by_tp.find(tidp);
        auto start_offset = start_it != start_offsets_by_tp.end()
                              ? start_it->second
                              : kafka::offset{0};
        size_t added_for_tidp = 0;
        size_t added_extents_for_tidp = 0;
        for (const auto& extent : extents) {
            // Skip extents fully below start_offset. These are stale
            // replacements for extents that have been truncated.
            if (extent.last_offset < start_offset) {
                new_objects_map[extent.oid].removed_data_size += extent.len;
                continue;
            }
            auto key = extent_row_key::encode(tidp, extent.base_offset);
            added_extent_keys.emplace(key);
            added_for_tidp += extent.len;
            ++added_extents_for_tidp;
            out.emplace_back(
              write_batch_row{
                .key = extent_row_key::encode(tidp, extent.base_offset),
                .value = serde::to_iobuf(
                  extent_row_value{
                    .last_offset = extent.last_offset,
                    .max_timestamp = extent.max_timestamp,
                    .filepos = extent.filepos,
                    .len = extent.len,
                    .oid = extent.oid,
                  }),
              });
        }

        auto meta_res = co_await get_metadata_for_update(
          state, updated_metadata, tidp, "new extents");
        if (!meta_res.has_value()) {
            co_return std::unexpected(std::move(meta_res.error()));
        }
        meta_res.value()->size += added_for_tidp;
        meta_res.value()->num_extents += added_extents_for_tidp;
    }
    if (added_extent_keys.empty()) {
        // No extents, e.g. because all replacements are below the current
        // start offsets.
        co_return std::unexpected(db_update_error(
          invalid_update, "Replacement extents all filtered out"));
    }
    for (const auto& [tidp, keys] : extent_keys_to_delete) {
        for (const auto& key : keys) {
            if (added_extent_keys.contains(key)) {
                // Don't delete keys that are also being added.
                continue;
            }
            out.emplace_back(
              write_batch_row{
                .key = key,
                .value = iobuf{},
              });
        }
    }
    for (const auto& [oid, entry] : updated_old_objects) {
        out.emplace_back(
          write_batch_row{
            .key = object_row_key::encode(oid),
            .value = serde::to_iobuf(object_row_value{.object = entry}),
          });
    }
    for (const auto& [oid, entry] : new_objects_map) {
        out.emplace_back(
          write_batch_row{
            .key = object_row_key::encode(oid),
            .value = serde::to_iobuf(object_row_value{.object = entry}),
          });
    }
    for (const auto& [tidp, comp_state] : merged_compaction_states) {
        out.emplace_back(
          write_batch_row{
            .key = compaction_row_key::encode(tidp),
            .value = serde::to_iobuf(compaction_row_value{.state = comp_state}),
          });
    }

    for (auto& [tidp, meta] : updated_metadata) {
        out.emplace_back(
          write_batch_row{
            .key = metadata_row_key::encode(tidp),
            .value = serde::to_iobuf(meta),
          });
    }

    co_return std::expected<void, db_update_error>{};
}

ss::future<std::expected<absl::btree_set<object_id>, db_update_error>>
replace_objects_db_update::discover_replaced_object_ids(
  state_reader& state) const {
    auto validate_res = validate_inputs();
    if (!validate_res.has_value()) {
        co_return std::unexpected(std::move(validate_res.error()));
    }
    sorted_extents_by_tidp_t new_extents_by_tp;
    for (const auto& o : new_objects) {
        o.collect_extents_by_tidp(&new_extents_by_tp);
    }

    auto contiguous_intervals_res = contiguous_intervals_for_extents(
      new_extents_by_tp);
    if (!contiguous_intervals_res.has_value()) {
        co_return std::unexpected(db_update_error(
          invalid_input, std::move(contiguous_intervals_res.error())));
    }

    // For each partition's replacement intervals, scan existing extents
    // in that range and collect their object IDs. We use get_inclusive_extents
    // rather than get_extent_range — discovery only needs OIDs, not exact
    // alignment validation (build_rows handles that).
    absl::btree_set<object_id> discovered_oids;
    for (const auto& [tidp, intervals] : contiguous_intervals_res.value()) {
        for (const auto& interval : intervals) {
            auto extents_res = co_await state.get_inclusive_extents(
              tidp, interval.base_offset, interval.last_offset);
            if (!extents_res.has_value()) {
                co_return std::unexpected(wrap_read_err(
                  std::move(extents_res.error()),
                  "Error getting {} extents for discovery in [{}, {}]",
                  tidp,
                  interval.base_offset,
                  interval.last_offset));
            }
            if (!extents_res->has_value()) {
                continue;
            }
            auto gen = (*extents_res)->get_rows();
            while (auto row_res = co_await gen()) {
                const auto& row = row_res->get();
                if (!row.has_value()) {
                    co_return std::unexpected(wrap_read_err(
                      row.error(),
                      "Error iterating {} extents during discovery",
                      tidp));
                }
                discovered_oids.insert(row->val.oid);
            }
        }
    }
    co_return discovered_oids;
}

std::expected<void, db_update_error>
replace_objects_db_update::validate_inputs() const {
    if (new_objects.empty()) {
        return std::unexpected(
          db_update_error(invalid_input, "No objects requested"));
    }

    sorted_extents_by_tidp_t new_extents_by_tp;
    for (const auto& o : new_objects) {
        o.collect_extents_by_tidp(&new_extents_by_tp);
    }
    for (const auto& [tidp, extents] : new_extents_by_tp) {
        for (const auto& extent : extents) {
            if (extent.base_offset > extent.last_offset) {
                return std::unexpected(db_update_error(
                  invalid_input,
                  fmt::format(
                    "Input object has inverted extent for partition {}: "
                    "base_offset {} > last_offset {}",
                    tidp,
                    extent.base_offset,
                    extent.last_offset)));
            }
        }
    }

    auto contiguous_intervals = contiguous_intervals_for_extents(
      new_extents_by_tp);
    if (!contiguous_intervals.has_value()) {
        return std::unexpected(
          db_update_error(invalid_input, contiguous_intervals.error()));
    }

    // Validate compaction updates align with extents
    if (compaction_updates.empty()) {
        return std::expected<void, db_update_error>{};
    }
    for (const auto& [t, t_req] : compaction_updates) {
        for (const auto& [p, compaction_update] : t_req) {
            model::topic_id_partition tidp{t, p};

            // Verify partition has new extents
            auto new_extent_iter = new_extents_by_tp.find(tidp);
            if (new_extent_iter == new_extents_by_tp.end()) {
                return std::unexpected(db_update_error(
                  invalid_input,
                  fmt::format(
                    "New cleaned range does not refer to partition with "
                    "extent: {}",
                    tidp)));
            }

            if (!compaction_update.new_cleaned_ranges.empty()) {
                const auto& req_cleaned_ranges
                  = compaction_update.new_cleaned_ranges;
                const auto& new_extents = new_extent_iter->second;

                // Check that the new extents span the cleaned ranges
                auto req_last = new_extents.rbegin()->last_offset;
                auto clean_last = req_cleaned_ranges.back().last_offset;
                if (clean_last > req_last) {
                    return std::unexpected(db_update_error(
                      invalid_input,
                      fmt::format(
                        "Cleaned range for {} does not match requested "
                        "new extents' last_offset {} > {}",
                        tidp,
                        clean_last,
                        req_last)));
                }

                auto req_extents_base = new_extents.begin()->base_offset;
                auto clean_base = req_cleaned_ranges.front().base_offset;
                if (req_extents_base > clean_base) {
                    return std::unexpected(db_update_error(
                      invalid_input,
                      fmt::format(
                        "Cleaned range start_offset for {} is not "
                        "covered by extents: {} > {}",
                        tidp,
                        req_extents_base,
                        clean_base)));
                }
            }
        }
    }

    return std::expected<void, db_update_error>{};
}

ss::future<std::expected<void, db_update_error>>
set_start_offset_db_update::build_rows(
  state_reader& reader,
  chunked_vector<write_batch_row>& out,
  bool* is_no_op) const {
    auto meta_res = co_await reader.get_metadata(tp);
    if (!meta_res.has_value()) {
        co_return std::unexpected(wrap_read_err(
          std::move(meta_res.error()), "Error reading metadata for {}", tp));
    }
    if (!meta_res->has_value()) {
        co_return std::unexpected(db_update_error(
          invalid_update, fmt::format("Partition {} not found", tp)));
    }
    const auto& metadata = meta_res.value().value();

    // Validate the current offset range.
    if (new_start_offset > metadata.next_offset) {
        co_return std::unexpected(db_update_error(
          invalid_update,
          fmt::format(
            "Requested start offset for {} is above the next offset: {} > {}",
            new_start_offset,
            tp,
            metadata.next_offset)));
    }
    auto no_op = new_start_offset <= metadata.start_offset;
    if (is_no_op) {
        *is_no_op = no_op;
    }
    if (no_op) {
        co_return std::expected<void, db_update_error>{};
    }

    // Find extents below new_start_offset and collect for deletion.
    chunked_hash_map<object_id, size_t> removed_size_by_oid;
    chunked_vector<ss::sstring> extent_keys_to_delete;
    size_t total_removed_size = 0;

    auto max_to_remove = kafka::prev_offset(new_start_offset);
    auto extents_res = co_await reader.get_inclusive_extents(
      tp, metadata.start_offset, max_to_remove);
    if (!extents_res.has_value()) {
        co_return std::unexpected(wrap_read_err(
          std::move(extents_res.error()),
          "Error getting {} extents for removal in range [{}, {}]",
          tp,
          metadata.start_offset,
          max_to_remove));
    }
    if (extents_res.value().has_value()) {
        auto extent_gen = (*extents_res)->get_rows();
        while (auto row_res = co_await extent_gen()) {
            const auto& row = row_res->get();
            if (!row.has_value()) {
                co_return std::unexpected(wrap_read_err(
                  row.error(),
                  "Error iterating through {} extents in range [{}, {}]",
                  tp,
                  metadata.start_offset,
                  max_to_remove));
            }
            // Only delete if extent is fully below new_start_offset.
            const auto& extent = *row;
            if (extent.val.last_offset < new_start_offset) {
                removed_size_by_oid[extent.val.oid] += extent.val.len;
                extent_keys_to_delete.push_back(extent.key);
                total_removed_size += extent.val.len;
            }
        }
    }

    // Write tombstones for deleted extents.
    for (const auto& key : extent_keys_to_delete) {
        out.emplace_back(write_batch_row{.key = key, .value = iobuf{}});
    }

    // Update object entries with removed_data_size.
    auto updated_old_objects_res = co_await build_object_removal_entries(
      reader, removed_size_by_oid);
    if (!updated_old_objects_res.has_value()) {
        co_return std::unexpected(updated_old_objects_res.error());
    }
    auto& updated_old_objects = updated_old_objects_res.value();
    for (const auto& [oid, obj_entry] : updated_old_objects) {
        out.emplace_back(
          write_batch_row{
            .key = object_row_key::encode(oid),
            .value = serde::to_iobuf(object_row_value{.object = obj_entry}),
          });
    }

    // Collect term starts at or below new_start_offset.
    auto term_keys_res = co_await reader.get_term_keys(tp, new_start_offset);
    if (!term_keys_res.has_value()) {
        co_return std::unexpected(wrap_read_err(
          std::move(term_keys_res.error()),
          "Error getting term keys for {}",
          tp));
    }
    auto& term_keys = *term_keys_res;
    if (!term_keys.empty()) {
        // Keep the last one to ensure we still have the term for the start
        // offset.
        term_keys.pop_back();
    }
    for (const auto& term_key : term_keys) {
        out.emplace_back(write_batch_row{.key = term_key, .value = iobuf{}});
    }

    // Truncate compaction state if it exists.
    if (metadata.compaction_epoch > partition_state::compaction_epoch_t(0)) {
        auto comp_res = co_await reader.get_compaction_metadata(tp);
        if (!comp_res.has_value()) {
            co_return std::unexpected(wrap_read_err(
              std::move(comp_res.error()),
              "Error getting compaction metadata for {}",
              tp));
        }
        if (comp_res->has_value()) {
            auto comp_state = std::move(comp_res.value().value());
            comp_state.truncate_with_new_start_offset(new_start_offset);
            out.emplace_back(
              write_batch_row{
                .key = compaction_row_key::encode(tp),
                .value = serde::to_iobuf(
                  compaction_row_value{.state = comp_state}),
              });
        }
    }

    // Finally, write updated partition metadata with reduced size.
    out.emplace_back(
      write_batch_row{
        .key = metadata_row_key::encode(tp),
        .value = serde::to_iobuf(
          metadata_row_value{
            .start_offset = new_start_offset,
            .next_offset = metadata.next_offset,
            .compaction_epoch = metadata.compaction_epoch,
            .size = metadata.size - std::min(metadata.size, total_removed_size),
            .num_extents
            = metadata.num_extents
              - std::min(metadata.num_extents, extent_keys_to_delete.size()),
          }),
      });

    // TODO: if the resulting set of rows is too large, we should consider
    // doing some incremental prefix truncation.
    co_return std::expected<void, db_update_error>{};
}

ss::future<std::expected<absl::btree_set<object_id>, db_update_error>>
set_start_offset_db_update::discover_truncated_object_ids(
  state_reader& reader) const {
    auto meta_res = co_await reader.get_metadata(tp);
    if (!meta_res.has_value()) {
        co_return std::unexpected(wrap_read_err(
          std::move(meta_res.error()),
          "Error reading metadata for {} during discovery",
          tp));
    }
    if (!meta_res->has_value()) {
        co_return std::unexpected(db_update_error(
          invalid_update,
          fmt::format("Partition {} not found during discovery", tp)));
    }
    const auto& metadata = meta_res.value().value();

    if (new_start_offset <= metadata.start_offset) {
        co_return absl::btree_set<object_id>{};
    }

    absl::btree_set<object_id> discovered_oids;
    auto max_to_remove = kafka::prev_offset(new_start_offset);
    auto extents_res = co_await reader.get_inclusive_extents(
      tp, metadata.start_offset, max_to_remove);
    if (!extents_res.has_value()) {
        co_return std::unexpected(wrap_read_err(
          std::move(extents_res.error()),
          "Error getting {} extents for discovery in range [{}, {}]",
          tp,
          metadata.start_offset,
          max_to_remove));
    }
    if (extents_res.value().has_value()) {
        auto extent_gen = (*extents_res)->get_rows();
        while (auto row_res = co_await extent_gen()) {
            const auto& row = row_res->get();
            if (!row.has_value()) {
                co_return std::unexpected(wrap_read_err(
                  row.error(),
                  "Error iterating {} extents during discovery",
                  tp));
            }
            if (row->val.last_offset < new_start_offset) {
                discovered_oids.insert(row->val.oid);
            }
        }
    }
    co_return discovered_oids;
}

ss::future<std::expected<void, db_update_error>>
remove_topics_db_update::build_rows(
  state_reader& reader, chunked_vector<write_batch_row>& out) const {
    if (topics.empty()) {
        co_return std::expected<void, db_update_error>{};
    }

    // Track removed data sizes by object across all topics/partitions.
    chunked_hash_map<object_id, size_t> removed_size_by_oid;

    // TODO: this is embarassingly parallel.
    for (const auto& tid : topics) {
        // Get all partitions for this topic.
        auto partitions_res = co_await reader.get_partitions_for_topic(tid);
        if (!partitions_res.has_value()) {
            co_return std::unexpected(wrap_read_err(
              std::move(partitions_res.error()),
              "Error getting partitions for topic {}",
              tid));
        }

        for (const auto& pid : partitions_res.value()) {
            model::topic_id_partition tidp(tid, pid);

            // Collect all extents to be removed and track the removed sizes by
            // object.
            auto extents_res = co_await reader.get_inclusive_extents(
              tidp, std::nullopt, std::nullopt);
            if (extents_res.has_value() && extents_res->has_value()) {
                auto extent_gen = (*extents_res)->get_rows();
                while (auto row_res = co_await extent_gen()) {
                    const auto& row = row_res->get();
                    if (!row.has_value()) {
                        break;
                    }
                    const auto& extent = *row;
                    removed_size_by_oid[extent.val.oid] += extent.val.len;

                    // Write tombstone for each extent row.
                    out.emplace_back(
                      write_batch_row{.key = extent.key, .value = iobuf{}});
                }
            }

            // Get all term keys and write tombstones.
            auto terms_res = co_await reader.get_term_keys(tidp, std::nullopt);
            if (terms_res.has_value()) {
                for (const auto& term_key : *terms_res) {
                    out.emplace_back(
                      write_batch_row{.key = term_key, .value = iobuf{}});
                }
            }

            // Write tombstone for the partition's compaction row.
            out.emplace_back(
              write_batch_row{
                .key = compaction_row_key::encode(tidp), .value = iobuf{}});

            // Write tombstone for the partition's metadata row.
            out.emplace_back(
              write_batch_row{
                .key = metadata_row_key::encode(tidp), .value = iobuf{}});
        }
    }

    // Update object entries with removed_data_size.
    auto updated_old_objects_res = co_await build_object_removal_entries(
      reader, removed_size_by_oid);
    if (!updated_old_objects_res.has_value()) {
        co_return std::unexpected(updated_old_objects_res.error());
    }
    auto& updated_old_objects = updated_old_objects_res.value();
    for (const auto& [oid, obj_entry] : updated_old_objects) {
        out.emplace_back(
          write_batch_row{
            .key = object_row_key::encode(oid),
            .value = serde::to_iobuf(object_row_value{.object = obj_entry}),
          });
    }

    co_return std::expected<void, db_update_error>{};
}

ss::future<std::expected<absl::btree_set<object_id>, db_update_error>>
remove_topics_db_update::discover_object_ids(state_reader& reader) const {
    absl::btree_set<object_id> discovered_oids;
    for (const auto& tid : topics) {
        auto partitions_res = co_await reader.get_partitions_for_topic(tid);
        if (!partitions_res.has_value()) {
            co_return std::unexpected(wrap_read_err(
              std::move(partitions_res.error()),
              "Error getting partitions for {} during discovery",
              tid));
        }
        for (const auto& pid : partitions_res.value()) {
            model::topic_id_partition tidp(tid, pid);
            auto extents_res = co_await reader.get_inclusive_extents(
              tidp, std::nullopt, std::nullopt);
            if (!extents_res.has_value()) {
                co_return std::unexpected(wrap_read_err(
                  std::move(extents_res.error()),
                  "Error getting {} extents during discovery",
                  tidp));
            }
            if (!extents_res->has_value()) {
                continue;
            }
            auto gen = (*extents_res)->get_rows();
            while (auto row_res = co_await gen()) {
                const auto& row = row_res->get();
                if (!row.has_value()) {
                    co_return std::unexpected(wrap_read_err(
                      row.error(),
                      "Error iterating {} extents during discovery",
                      tidp));
                }
                discovered_oids.insert(row->val.oid);
            }
        }
    }
    co_return discovered_oids;
}

ss::future<std::expected<void, db_update_error>>
remove_objects_db_update::build_rows(
  chunked_vector<write_batch_row>& out) const {
    for (const auto& oid : objects) {
        out.emplace_back(
          write_batch_row{
            .key = object_row_key::encode(oid), .value = iobuf{}});
    }

    co_return std::expected<void, db_update_error>{};
}

ss::future<std::expected<void, db_update_error>>
preregister_objects_db_update::can_apply(state_reader& reader) const {
    for (const auto& oid : object_ids) {
        auto obj_res = co_await reader.get_object(oid);
        if (!obj_res.has_value()) {
            co_return std::unexpected(wrap_read_err(
              std::move(obj_res.error()), "Error getting object {}", oid));
        }
        if (obj_res->has_value()) {
            co_return std::unexpected(db_update_error(
              invalid_update, fmt::format("object {} already exists", oid)));
        }
    }
    co_return std::expected<void, db_update_error>{};
}

ss::future<std::expected<void, db_update_error>>
preregister_objects_db_update::build_rows(
  state_reader& reader, chunked_vector<write_batch_row>& out) const {
    auto allowed = co_await can_apply(reader);
    if (!allowed.has_value()) {
        co_return std::unexpected(std::move(allowed.error()));
    }
    for (const auto& oid : object_ids) {
        out.emplace_back(
          write_batch_row{
            .key = object_row_key::encode(oid),
            .value = serde::to_iobuf(
              object_row_value{
                .object = object_entry{
                  .last_updated = registered_at,
                  .is_preregistration = true,
                },
              }),
          });
    }
    co_return std::expected<void, db_update_error>{};
}

ss::future<std::expected<void, db_update_error>>
expire_preregistered_objects_db_update::build_rows(
  state_reader& reader, chunked_vector<write_batch_row>& out) const {
    for (const auto& oid : object_ids) {
        auto obj_res = co_await reader.get_object(oid);
        if (!obj_res.has_value()) {
            co_return std::unexpected(wrap_read_err(
              std::move(obj_res.error()), "Error getting object {}", oid));
        }
        if (!obj_res->has_value() || !obj_res->value().is_preregistration) {
            continue;
        }
        auto entry = obj_res->value();
        entry.is_preregistration = false;
        entry.last_updated = model::timestamp::now();
        out.emplace_back(
          write_batch_row{
            .key = object_row_key::encode(oid),
            .value = serde::to_iobuf(object_row_value{.object = entry}),
          });
    }
    co_return std::expected<void, db_update_error>{};
}

} // namespace cloud_topics::l1
