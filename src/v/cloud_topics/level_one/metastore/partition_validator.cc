/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/metastore/partition_validator.h"

#include "cloud_io/remote.h"
#include "cloud_topics/level_one/common/object_utils.h"
#include "cloud_topics/level_one/metastore/lsm/keys.h"
#include "model/fundamental.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/coroutine.hh>

#include <chrono>

using namespace std::chrono_literals;

namespace cloud_topics::l1 {

std::string_view to_string_view(anomaly_type t) {
    switch (t) {
    case anomaly_type::extent_overlap:
        return "extent_overlap";
    case anomaly_type::extent_gap:
        return "extent_gap";
    case anomaly_type::next_offset_mismatch:
        return "next_offset_mismatch";
    case anomaly_type::start_offset_mismatch:
        return "start_offset_mismatch";
    case anomaly_type::object_not_found:
        return "object_not_found";
    case anomaly_type::object_preregistered:
        return "object_preregistered";
    case anomaly_type::object_not_in_storage:
        return "object_not_in_storage";
    case anomaly_type::compaction_range_below_start:
        return "compaction_range_below_start";
    case anomaly_type::compaction_range_above_next:
        return "compaction_range_above_next";
    case anomaly_type::compaction_tombstone_overlap:
        return "compaction_tombstone_overlap";
    case anomaly_type::compaction_tombstone_outside_cleaned:
        return "compaction_tombstone_outside_cleaned";
    case anomaly_type::term_ordering:
        return "term_ordering";
    case anomaly_type::compaction_state_unexpected:
        return "compaction_state_unexpected";
    }
    return "unknown_anomaly";
}

std::string_view to_string_view(partition_validator::errc e) {
    switch (e) {
    case partition_validator::errc::io_error:
        return "partition_validator::errc::io_error";
    case partition_validator::errc::shutting_down:
        return "partition_validator::errc::shutting_down";
    }
    return "partition_validator::errc::unknown";
}

namespace {

partition_validator::error
wrap_read_err(state_reader::error err, std::string_view context) {
    return std::move(err).wrap(
      partition_validator::errc::io_error, "{}", context);
}

struct decoded_extent {
    kafka::offset base_offset;
    kafka::offset last_offset;
    object_id oid;
};

using error = partition_validator::error;
using errc = partition_validator::errc;

std::expected<decoded_extent, error>
decode_extent(const state_reader::extent_row& row) {
    auto key = extent_row_key::decode(row.key);
    if (!key.has_value()) {
        return std::unexpected(error(errc::io_error, "failed to decode key"));
    }
    return decoded_extent{
      .base_offset = key.value().base_offset,
      .last_offset = row.val.last_offset,
      .oid = row.val.oid,
    };
}

/// Returns last_offset for contiguity, or nullopt if no extents remain.
std::optional<kafka::offset> validate_first_extent(
  const validate_partition_options& opts,
  kafka::offset start_offset,
  kafka::offset next_offset,
  std::optional<decoded_extent> first_extent_opt,
  chunked_hash_set<object_id>& seen_objects,
  partition_validation_result& result) {
    // If we are resuming validation from a previous call, we need to make sure
    // the resumption boundary looks okay.
    if (opts.resume_at_offset.has_value()) {
        if (!first_extent_opt.has_value()) {
            result.record(
              anomaly_type::extent_gap,
              "no extents found at or after resume_at_offset={}",
              *opts.resume_at_offset);
            return std::nullopt;
        }
        const auto& first_extent = first_extent_opt.value();
        auto resume_at = opts.resume_at_offset.value();
        if (
          first_extent.base_offset > resume_at
          || first_extent.last_offset < resume_at) {
            result.record(
              anomaly_type::extent_gap,
              "continuation: expected extent containing offset {}, "
              "got [{}, {}]",
              resume_at,
              first_extent.base_offset,
              first_extent.last_offset);
        }
        if (first_extent.base_offset != resume_at) {
            // Extent bounds changed (e.g. compaction merged it), so
            // count it as new.
            seen_objects.insert(first_extent_opt.value().oid);
            result.extents_validated++;
        }
        return first_extent_opt.value().last_offset;
    }
    // Otherwise, this is the first call for validation of this partition.

    if (!first_extent_opt.has_value()) {
        // Only expected to not have an extent if the partition is empty.
        if (start_offset != next_offset) {
            result.record(
              anomaly_type::next_offset_mismatch,
              "no extents but start_offset={} != next_offset={}",
              start_offset,
              next_offset);
        }
        return std::nullopt;
    }

    // No extent should be fully below start_offset. An extent
    // straddling start_offset (base < start <= last) is valid after
    // set_start_offset.
    const auto& first_extent = first_extent_opt.value();
    if (start_offset > first_extent.last_offset) {
        result.record(
          anomaly_type::start_offset_mismatch,
          "start_offset={} > first extent last_offset={} "
          "(entire extent is below start)",
          start_offset,
          first_extent.last_offset);
    }
    if (first_extent.base_offset > start_offset) {
        result.record(
          anomaly_type::extent_gap,
          "gap between start_offset={} and first extent base_offset={}",
          start_offset,
          first_extent.base_offset);
    }

    seen_objects.insert(first_extent.oid);
    result.extents_validated++;
    if (opts.max_extents > 0 && result.extents_validated >= opts.max_extents) {
        result.resume_at_offset = first_extent.base_offset;
    }
    return first_extent.last_offset;
}

} // namespace

partition_validator::partition_validator(state_reader& reader)
  : reader_(reader) {}

ss::future<
  std::expected<partition_validation_result, partition_validator::error>>
partition_validator::validate(validate_partition_options opts) {
    partition_validation_result result;

    auto metadata_res = co_await reader_.get_metadata(opts.tidp);
    if (!metadata_res.has_value()) {
        co_return std::unexpected(
          wrap_read_err(std::move(metadata_res.error()), "reading metadata"));
    }
    if (!metadata_res.value().has_value()) {
        co_return result;
    }
    const auto& metadata = metadata_res.value().value();
    if (
      opts.resume_at_offset.has_value()
      && *opts.resume_at_offset < metadata.start_offset) {
        // A prefix truncation may have advanced start_offset past the
        // resume point between paginated calls. Clamp to start_offset
        // so pagination skips past the truncated region.
        opts.resume_at_offset = metadata.start_offset;
    }

    auto seen_objects_res = co_await validate_extents(opts, metadata, result);
    if (!seen_objects_res.has_value()) {
        co_return std::unexpected(std::move(seen_objects_res.error()));
    }

    // Only verify terms and compaction metadata on if this is the first
    // validation call for this partition (i.e. we aren't resuming).
    if (!opts.resume_at_offset.has_value()) {
        auto terms_res = co_await validate_terms(opts, metadata, result);
        if (!terms_res.has_value()) {
            co_return std::unexpected(std::move(terms_res.error()));
        }

        auto comp_res = co_await validate_compaction(opts, metadata, result);
        if (!comp_res.has_value()) {
            co_return std::unexpected(std::move(comp_res.error()));
        }
    }

    auto obj_res = co_await validate_objects(
      opts, std::move(seen_objects_res.value()), result);
    if (!obj_res.has_value()) {
        co_return std::unexpected(std::move(obj_res.error()));
    }

    co_return result;
}

ss::future<
  std::expected<chunked_hash_set<object_id>, partition_validator::error>>
partition_validator::validate_extents(
  const validate_partition_options& opts,
  const metadata_row_value& metadata,
  partition_validation_result& result) {
    const auto start_offset = metadata.start_offset;
    const auto next_offset = metadata.next_offset;
    chunked_hash_set<object_id> seen_objects;

    auto extents_res = co_await reader_.get_inclusive_extents(
      opts.tidp, opts.resume_at_offset, std::nullopt);
    if (!extents_res.has_value()) {
        co_return std::unexpected(
          wrap_read_err(std::move(extents_res.error()), "reading extents"));
    }
    if (!extents_res.value().has_value()) {
        // There are no extents. Validate that this is expected.
        validate_first_extent(
          opts,
          start_offset,
          next_offset,
          /*first_extent=*/std::nullopt,
          seen_objects,
          result);
        co_return seen_objects;
    }

    auto gen = extents_res.value().value().get_rows();

    auto consume_next = [&gen](this auto)
      -> ss::future<std::optional<std::expected<decoded_extent, error>>> {
        auto row_opt = co_await gen();
        if (!row_opt) {
            co_return std::nullopt;
        }
        const auto& row = row_opt.value().get();
        if (!row.has_value()) {
            auto err = row.error();
            co_return std::unexpected(
              wrap_read_err(std::move(err), "iterating extents"));
        }
        co_return decode_extent(row.value());
    };

    std::optional<decoded_extent> first_ext;
    auto first_opt = co_await consume_next();
    if (first_opt.has_value()) {
        if (!first_opt.value().has_value()) {
            co_return std::unexpected(std::move(first_opt.value().error()));
        }
        first_ext = first_opt.value().value();
    }

    auto first_ext_last = validate_first_extent(
      opts, start_offset, next_offset, first_ext, seen_objects, result);
    if (!first_ext_last.has_value() || result.resume_at_offset.has_value()) {
        co_return seen_objects;
    }
    auto last = *first_ext_last;

    while (auto ext_opt = co_await consume_next()) {
        if (!ext_opt.value().has_value()) {
            co_return std::unexpected(std::move(ext_opt.value().error()));
        }
        auto ext = ext_opt.value().value();
        auto expected_base = kafka::next_offset(last);
        if (ext.base_offset > expected_base) {
            result.record(
              anomaly_type::extent_gap,
              "gap between extents: prev last_offset={}, next "
              "base_offset={} (expected {})",
              last,
              ext.base_offset,
              expected_base);
        }
        if (ext.base_offset < expected_base) {
            result.record(
              anomaly_type::extent_overlap,
              "overlap between extents: prev last_offset={}, next "
              "base_offset={} (expected {})",
              last,
              ext.base_offset,
              expected_base);
        }

        last = ext.last_offset;
        seen_objects.insert(ext.oid);
        result.extents_validated++;

        if (
          opts.max_extents > 0
          && result.extents_validated >= opts.max_extents) {
            result.resume_at_offset = ext.base_offset;
            break;
        }
    }

    if (!result.resume_at_offset.has_value()) {
        // We've iterated through all extents. Make sure the last one we got
        // aligned with next_offset.
        auto expected_next = kafka::next_offset(last);
        if (next_offset != expected_next) {
            result.record(
              anomaly_type::next_offset_mismatch,
              "next_offset={} but last extent last_offset={} (expected "
              "next_offset={})",
              next_offset,
              last,
              expected_next);
        }
    }

    co_return seen_objects;
}

ss::future<std::expected<std::monostate, partition_validator::error>>
partition_validator::validate_terms(
  const validate_partition_options& opts,
  const metadata_row_value& metadata,
  partition_validation_result& result) {
    const auto start_offset = metadata.start_offset;
    const auto next_offset = metadata.next_offset;

    auto terms_res = co_await reader_.get_all_terms(opts.tidp);
    if (!terms_res.has_value()) {
        co_return std::unexpected(
          wrap_read_err(std::move(terms_res.error()), "reading terms"));
    }
    const auto& terms = terms_res.value();

    if (terms.empty()) {
        result.record(
          anomaly_type::term_ordering,
          "no terms for partition with metadata "
          "(start_offset={}, next_offset={})",
          start_offset,
          next_offset);
        co_return std::monostate{};
    }

    if (terms.front().start_offset > start_offset) {
        result.record(
          anomaly_type::term_ordering,
          "first term (id={}, start_offset={}) does not cover "
          "partition start_offset={}",
          terms.front().term_id,
          terms.front().start_offset,
          start_offset);
    }

    if (terms.back().start_offset >= next_offset) {
        result.record(
          anomaly_type::term_ordering,
          "last term (id={}, start_offset={}) starts at or above "
          "next_offset={}",
          terms.back().term_id,
          terms.back().start_offset,
          next_offset);
    }

    std::optional<model::term_id> prev_term;
    std::optional<kafka::offset> prev_offset;
    for (const auto& ts : terms) {
        if (prev_term && ts.term_id <= *prev_term) {
            result.record(
              anomaly_type::term_ordering,
              "term_id {} is not strictly greater than previous term_id {}",
              ts.term_id,
              *prev_term);
        }
        if (prev_offset && ts.start_offset <= *prev_offset) {
            result.record(
              anomaly_type::term_ordering,
              "term {} start_offset={} is not strictly greater than "
              "previous term {}, start_offset={}",
              ts.term_id,
              ts.start_offset,
              *prev_term,
              *prev_offset);
        }
        prev_term = ts.term_id;
        prev_offset = ts.start_offset;
    }

    co_return std::monostate{};
}

ss::future<std::expected<std::monostate, partition_validator::error>>
partition_validator::validate_compaction(
  const validate_partition_options& opts,
  const metadata_row_value& metadata,
  partition_validation_result& result) {
    auto comp_res = co_await reader_.get_compaction_metadata(opts.tidp);
    if (!comp_res.has_value()) {
        co_return std::unexpected(wrap_read_err(
          std::move(comp_res.error()), "reading compaction metadata"));
    }
    bool has_compaction_meta = comp_res.value().has_value();
    bool has_compaction_epoch = metadata.compaction_epoch
                                > partition_state::compaction_epoch_t{0};

    if (!has_compaction_meta && has_compaction_epoch) {
        result.record(
          anomaly_type::compaction_state_unexpected,
          "compaction_epoch={} but no compaction state exists",
          metadata.compaction_epoch);
        co_return std::monostate{};
    }
    if (has_compaction_meta && !has_compaction_epoch) {
        result.record(
          anomaly_type::compaction_state_unexpected,
          "compaction state exists but compaction_epoch={}",
          metadata.compaction_epoch);
        co_return std::monostate{};
    }
    if (!has_compaction_meta) {
        co_return std::monostate{};
    }

    const auto& comp = comp_res.value().value();
    const auto start_offset = metadata.start_offset;
    const auto next_offset = metadata.next_offset;

    auto stream = comp.cleaned_ranges.make_stream();
    while (stream.has_next()) {
        auto iv = stream.next();
        if (iv.base_offset < start_offset) {
            result.record(
              anomaly_type::compaction_range_below_start,
              "cleaned range {} starts below start_offset={}",
              iv,
              start_offset);
        }
        if (iv.last_offset >= next_offset) {
            result.record(
              anomaly_type::compaction_range_above_next,
              "cleaned range {} extends at or above next_offset={}",
              iv,
              next_offset);
        }
    }

    std::optional<kafka::offset> prev_tombstone_last;
    for (const auto& tr : comp.cleaned_ranges_with_tombstones) {
        if (prev_tombstone_last && tr.base_offset <= *prev_tombstone_last) {
            result.record(
              anomaly_type::compaction_tombstone_overlap,
              "tombstone range [{}, {}] overlaps with previous "
              "range ending at {}",
              tr.base_offset,
              tr.last_offset,
              *prev_tombstone_last);
        }
        if (!comp.cleaned_ranges.covers(tr.base_offset, tr.last_offset)) {
            result.record(
              anomaly_type::compaction_tombstone_outside_cleaned,
              "tombstone range [{}, {}] not fully covered by "
              "cleaned ranges",
              tr.base_offset,
              tr.last_offset);
        }
        prev_tombstone_last = tr.last_offset;
    }

    co_return std::monostate{};
}

ss::future<std::expected<std::monostate, partition_validator::error>>
partition_validator::validate_objects(
  const validate_partition_options& opts,
  chunked_hash_set<object_id> seen_objects,
  partition_validation_result& result) {
    if (!opts.check_object_metadata && !opts.check_object_storage) {
        co_return std::monostate{};
    }
    for (const auto& oid : seen_objects) {
        if (opts.check_object_metadata) {
            auto obj_res = co_await reader_.get_object(oid);
            if (!obj_res.has_value()) {
                co_return std::unexpected(wrap_read_err(
                  std::move(obj_res.error()), "reading object metadata"));
            }
            if (!obj_res.value().has_value()) {
                result.record(
                  anomaly_type::object_not_found,
                  "extent references object {} which does not exist "
                  "in metastore",
                  oid);
                continue;
            }
            if (obj_res.value().value().is_preregistration) {
                result.record(
                  anomaly_type::object_preregistered,
                  "extent references object {} which is still a "
                  "preregistration",
                  oid);
            }
        }

        if (!opts.check_object_storage) {
            continue;
        }
        if (!opts.remote || !opts.bucket || !opts.as) {
            continue;
        }
        auto path = object_path_factory::level_one_path(oid);
        retry_chain_node rtc(*opts.as, 30s, 1s);
        auto exists_fut = co_await ss::coroutine::as_future(
          opts.remote->object_exists(
            *opts.bucket, path, rtc, "partition_validator"));
        if (exists_fut.failed()) {
            auto ex = exists_fut.get_exception();
            auto ec = ssx::is_shutdown_exception(ex) ? errc::shutting_down
                                                     : errc::io_error;
            co_return std::unexpected(error(
              ec,
              fmt::format(
                "object_exists failed for {}: {}",
                oid,
                exists_fut.get_exception())));
        }
        auto dl_res = exists_fut.get();
        switch (dl_res) {
        case cloud_io::download_result::success:
            continue;
        case cloud_io::download_result::notfound:
            result.record(
              anomaly_type::object_not_in_storage,
              "object {} not found in cloud storage at {}",
              oid,
              path());
            continue;
        case cloud_io::download_result::timedout:
        case cloud_io::download_result::failed:
            co_return std::unexpected(error(
              errc::io_error,
              fmt::format("object_exists failed for {}: {}", oid, dl_res)));
        }
    }

    co_return std::monostate{};
}

} // namespace cloud_topics::l1
