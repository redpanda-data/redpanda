/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/partition_manifest.h"

#include "base/vlog.h"
#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "bytes/streambuf.h"
#include "cloud_storage/base_manifest.h"
#include "cloud_storage/logger.h"
#include "cloud_storage/partition_path_utils.h"
#include "cloud_storage/remote_path_provider.h"
#include "cloud_storage/segment_meta_cstore.h"
#include "cloud_storage/types.h"
#include "hashing/xx.h"
#include "json/document.h"
#include "json/istreamwrapper.h"
#include "json/ostreamwrapper.h"
#include "json/writer.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "reflection/to_tuple.h"
#include "reflection/type_traits.h"
#include "serde/rw/envelope.h"
#include "serde/rw/iobuf.h"
#include "serde/rw/optional.h"
#include "serde/rw/rw.h"
#include "serde/rw/scalar.h"
#include "serde/rw/vector.h"
#include "ssx/sformat.h"
#include "storage/fs_utils.h"
#include "utils/to_string.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/later.hh>

#include <fmt/ostream.h>
#include <rapidjson/error/en.h>

#include <algorithm>
#include <charconv>
#include <exception>
#include <iterator>
#include <memory>
#include <numeric>
#include <optional>
#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <utility>

namespace fmt {
template<>
struct fmt::formatter<cloud_storage::partition_manifest::segment_meta> {
    using segment_meta = cloud_storage::partition_manifest::segment_meta;

    template<typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template<typename FormatContext>
    auto format(const segment_meta& m, FormatContext& ctx) {
        return fmt::format_to(
          ctx.out(),
          "{{o={}-{} t={}-{}}}",
          m.base_offset,
          m.committed_offset,
          m.base_timestamp,
          m.max_timestamp);
    }
};
} // namespace fmt

namespace cloud_storage {
std::ostream&
operator<<(std::ostream& s, const partition_manifest_path_components& c) {
    fmt::print(
      s, "{{{}: {}-{}-{}-{}}}", c._origin, c._ns, c._topic, c._part, c._rev);
    return s;
}

std::optional<segment_name_components>
parse_segment_name(const segment_name& name) {
    auto parsed = storage::segment_path::parse_segment_filename(name);
    if (!parsed) {
        return std::nullopt;
    }
    return segment_name_components{
      .base_offset = parsed->base_offset,
      .term = parsed->term,
    };
}

segment_name generate_local_segment_name(model::offset o, model::term_id t) {
    vassert(t != model::term_id{}, "Invalid term id");
    return segment_name(ssx::sformat("{}-{}-v1.log", o(), t()));
}

partition_manifest::lw_segment_meta
partition_manifest::lw_segment_meta::convert(const segment_meta& m) {
    return lw_segment_meta{
      .ntp_revision = m.ntp_revision,
      .base_offset = m.base_offset,
      .committed_offset = m.committed_offset,
      .archiver_term = m.archiver_term,
      .segment_term = m.segment_term,
      .size_bytes = m.sname_format == segment_name_format::v1 ? 0
                                                              : m.size_bytes,
      .sname_format = m.sname_format};
}

segment_meta partition_manifest::lw_segment_meta::convert(
  const partition_manifest::lw_segment_meta& lw) {
    // default the name format to v1
    auto sname_format = segment_name_format::v1;

    // if we have an explicit format other than the default then use it,
    // otherwise derive from size_bytes
    if (lw.sname_format != segment_name_format::v1) {
        sname_format = lw.sname_format;
    } else if (lw.size_bytes != 0) {
        sname_format = segment_name_format::v2;
    }
    return segment_meta{
      .size_bytes = lw.size_bytes,
      .base_offset = lw.base_offset,
      .committed_offset = lw.committed_offset,
      .ntp_revision = lw.ntp_revision,
      .archiver_term = lw.archiver_term,
      .segment_term = lw.segment_term,
      .sname_format = sname_format,
    };
}

partition_manifest::partition_manifest()
  : _ntp()
  , _rev()
  , _mem_tracker(ss::make_shared<util::mem_tracker>(""))
  , _segments()
  , _last_offset(0) {}

partition_manifest::partition_manifest(
  model::ntp ntp,
  model::initial_revision_id rev,
  ss::shared_ptr<util::mem_tracker> partition_mem_tracker)
  : _ntp(std::move(ntp))
  , _rev(rev)
  , _mem_tracker(
      partition_mem_tracker ? partition_mem_tracker->create_child("manifest")
                            : ss::make_shared<util::mem_tracker>(""))
  , _segments()
  , _last_offset(0) {}

remote_manifest_path partition_manifest::get_manifest_path(
  const remote_path_provider& path_provider) const {
    return remote_manifest_path{path_provider.partition_manifest_path(*this)};
}

ss::sstring partition_manifest::display_name() const {
    return fmt::format("{}_{}", get_ntp().path(), _rev());
}

const model::ntp& partition_manifest::get_ntp() const { return _ntp; }

model::offset partition_manifest::get_last_offset() const {
    return _last_offset;
}

std::optional<kafka::offset> partition_manifest::get_last_kafka_offset() const {
    const auto next_kafka_offset = get_next_kafka_offset();
    if (!next_kafka_offset || *next_kafka_offset == kafka::offset{0}) {
        return std::nullopt;
    }

    return *next_kafka_offset - kafka::offset{1};
}

std::optional<kafka::offset> partition_manifest::get_next_kafka_offset() const {
    auto last_seg = last_segment();
    if (!last_seg.has_value()) {
        return std::nullopt;
    }
    return last_seg->next_kafka_offset();
}

model::offset partition_manifest::get_insync_offset() const {
    return _insync_offset;
}

void partition_manifest::advance_insync_offset(model::offset o) {
    _insync_offset = std::max(o, _insync_offset);
}

std::optional<model::offset> partition_manifest::get_start_offset() const {
    if (_start_offset == model::offset{}) {
        return std::nullopt;
    }
    return _start_offset;
}

kafka::offset partition_manifest::get_start_kafka_offset_override() const {
    return _start_kafka_offset_override;
}

const partition_manifest::spillover_manifest_map&
partition_manifest::get_spillover_map() const {
    return _spillover_manifests;
}

std::optional<kafka::offset>
partition_manifest::full_log_start_kafka_offset() const {
    if (_archive_start_offset != model::offset{}) {
        // The archive start offset is guaranteed to be smaller than
        // the manifest start offset.
        vassert(
          _archive_start_offset <= _start_offset,
          "Archive start offset {} is greater than the start offset {}",
          _archive_start_offset,
          _start_offset);
        return _archive_start_offset - _archive_start_offset_delta;
    }

    return get_start_kafka_offset();
}

std::optional<model::offset> partition_manifest::full_log_start_offset() const {
    if (_archive_start_offset != model::offset{}) {
        return _archive_start_offset;
    }
    if (_start_offset == model::offset{}) {
        return std::nullopt;
    }
    return _start_offset;
}

std::optional<kafka::offset>
partition_manifest::get_start_kafka_offset() const {
    if (_start_offset == model::offset{}) {
        return std::nullopt;
    }

    if (unlikely(!_cached_start_kafka_offset_local)) {
        _cached_start_kafka_offset_local = compute_start_kafka_offset_local();
    }

    return _cached_start_kafka_offset_local;
}

void partition_manifest::set_start_offset(model::offset start_offset) {
    _start_offset = start_offset;
    // `_cached_start_kafka_offset_local` needs to be invalidated every
    // time the start offset changes.
    _cached_start_kafka_offset_local = std::nullopt;
}

std::optional<kafka::offset>
partition_manifest::compute_start_kafka_offset_local() const {
    std::optional<kafka::offset> local_start_offset;
    auto iter = _segments.find(_start_offset);
    if (iter != _segments.end()) {
        auto delta = iter->delta_offset;
        local_start_offset = _start_offset - delta;
    } else {
        // If start offset points outside a segment, then we cannot
        // translate it.  If there are any segments ahead of it, then
        // those may be considered the start of the remote log.
        if (auto front_it = _segments.begin();
            front_it != _segments.end()
            && front_it->base_offset >= _start_offset) {
            local_start_offset = front_it->base_offset - front_it->delta_offset;
        }
    }
    return local_start_offset;
}

partition_manifest::const_iterator
partition_manifest::segment_containing(kafka::offset o) const {
    vlog(cst_log.debug, "Metadata lookup using kafka offset {}", o);
    if (_segments.empty()) {
        return end();
    }

    // Kafka offset is always <= log offset.
    // To find a segment by its kafka offset we can simply query
    // manifest by log offset and then traverse forward until we
    // find a matching segment.
    auto it = _segments.lower_bound(kafka::offset_cast(o));
    if (it == _segments.begin()) {
        if (it->base_kafka_offset() > o) {
            // The beginning of the manifest already has a base offset that
            // doesn't satisfy the query.
            return end();
        } else {
            // move it to the second element, so we can safely call
            // _segments.prev(it)
            ++it;
        }
    }

    // We need to find first element which has greater kafka offset than
    // the target and step back. It is possible to have a segment that
    // doesn't have data batches. This scan has to skip segments like that.
    auto end_it = end();
    for (; it != end_it; ++it) {
        if (it->base_kafka_offset() > o) {
            // On the first segment we see with a base kafka offset higher than
            // 'o', return its previous segment.
            return _segments.prev(it);
        }
    }

    // All segments had base kafka offsets lower than 'o'.
    auto back = _segments.prev(end_it);
    if (back->delta_offset_end != model::offset_delta{}) {
        // If 'prev' points to the last segment, it's not guaranteed that
        // the segment contains the required kafka offset. We need an extra
        // check using delta_offset_end. If the field is not set then we
        // will return the last segment. This is OK since delta_offset_end
        // will always be set for new segments.
        if (back->next_kafka_offset() <= o) {
            return end_it;
        }
    }
    return back;
}

model::offset partition_manifest::get_last_uploaded_compacted_offset() const {
    return _last_uploaded_compacted_offset;
}

model::initial_revision_id partition_manifest::get_revision_id() const {
    return _rev;
}

remote_segment_path partition_manifest::generate_segment_path(
  const segment_meta& meta, const remote_path_provider& path_provider) const {
    return remote_segment_path{path_provider.segment_path(*this, meta)};
}

remote_segment_path partition_manifest::generate_segment_path(
  const lw_segment_meta& meta,
  const remote_path_provider& path_provider) const {
    return generate_segment_path(lw_segment_meta::convert(meta), path_provider);
}

segment_name partition_manifest::generate_remote_segment_name(
  const partition_manifest::value& val) {
    switch (val.sname_format) {
    case segment_name_format::v1:
        return segment_name(
          ssx::sformat("{}-{}-v1.log", val.base_offset(), val.segment_term()));
    case segment_name_format::v2:
        [[fallthrough]];
    case segment_name_format::v3:
        // Use new style format ".../base-committed-term-size-v1.log"
        return segment_name(ssx::sformat(
          "{}-{}-{}-{}-v1.log",
          val.base_offset(),
          val.committed_offset(),
          val.size_bytes,
          val.segment_term()));
    }
    __builtin_unreachable();
}

partition_manifest::const_iterator
partition_manifest::first_addressable_segment() const {
    if (_start_offset == model::offset{}) {
        return end();
    }

    return _segments.find(_start_offset);
}

partition_manifest::const_iterator partition_manifest::begin() const {
    return _segments.begin();
}

partition_manifest::const_iterator partition_manifest::end() const {
    return _segments.end();
}

std::optional<segment_meta> partition_manifest::last_segment() const {
    if (_segments.empty()) {
        return std::nullopt;
    }
    return _segments.last_segment();
}

bool partition_manifest::empty() const { return _segments.size() == 0; }

size_t partition_manifest::size() const { return _segments.size(); }

size_t partition_manifest::segments_metadata_bytes() const {
    return _segments.inflated_actual_size().second;
}

size_t partition_manifest::estimate_serialized_size() const {
    constexpr auto bytes_per_segment = 10;
    return _segments.size() * bytes_per_segment;
}

void partition_manifest::flush_write_buffer() {
    _segments.flush_write_buffer();
}

uint64_t partition_manifest::compute_cloud_log_size() const {
    const auto& bo_col = _segments.get_base_offset_column();
    const auto& sz_col = _segments.get_size_bytes_column();
    auto it = bo_col.find(_start_offset);
    if (it.is_end()) {
        return 0;
    }
    return std::accumulate(sz_col.at_index(it.index()), sz_col.end(), 0);
}

uint64_t partition_manifest::cloud_log_size() const {
    return _cloud_log_size_bytes + _archive_size_bytes;
}

uint64_t partition_manifest::stm_region_size_bytes() const {
    return _cloud_log_size_bytes;
}

uint64_t partition_manifest::archive_size_bytes() const {
    return _archive_size_bytes;
}

void partition_manifest::disable_permanently() {
    _last_offset = model::offset::max();
    _last_uploaded_compacted_offset = model::offset::max();
}

void partition_manifest::subtract_from_cloud_log_size(size_t to_subtract) {
    if (to_subtract > _cloud_log_size_bytes) {
        vlog(
          cst_log.warn,
          "Invalid attempt to subtract from cloud log size of {}. Setting it "
          "to 0 to prevent underflow",
          _ntp);
        _cloud_log_size_bytes = 0;
    } else {
        _cloud_log_size_bytes -= to_subtract;
    }
}

bool partition_manifest::contains(const partition_manifest::key& key) const {
    return _segments.contains(key);
}

bool partition_manifest::contains(const segment_name& name) const {
    auto maybe_key = parse_segment_name(name);
    if (!maybe_key) {
        throw std::runtime_error(
          fmt_with_ctx(fmt::format, "can't parse segment name \"{}\"", name));
    }
    return _segments.contains(maybe_key->base_offset);
}

bool partition_manifest::segment_with_offset_range_exists(
  model::offset base, model::offset committed) const {
    if (auto iter = find(base); iter != end()) {
        const auto expected_committed
          = _segments.get_committed_offset_column().at_index(iter.index());

        // false when committed offset doesn't match
        return committed == *expected_committed;
    } else {
        // base offset doesn't match any segment
        return false;
    }
}

void partition_manifest::delete_replaced_segments() { _replaced.clear(); }

model::offset partition_manifest::get_archive_start_offset() const {
    return _archive_start_offset;
}

model::offset_delta partition_manifest::get_archive_start_offset_delta() const {
    return _archive_start_offset_delta;
}

kafka::offset partition_manifest::get_archive_start_kafka_offset() const {
    if (_archive_start_offset == model::offset{}) {
        return kafka::offset{};
    }
    return _archive_start_offset - _archive_start_offset_delta;
}

model::offset partition_manifest::get_archive_clean_offset() const {
    return _archive_clean_offset;
}

void partition_manifest::set_archive_start_offset(
  model::offset start_rp_offset, model::offset_delta start_delta) {
    if (_archive_start_offset < start_rp_offset) {
        _archive_start_offset = start_rp_offset;
        _archive_start_offset_delta = start_delta;
        auto new_so = _archive_start_offset - _archive_start_offset_delta;
        if (new_so > _start_kafka_offset_override) {
            // The new archive start has moved past the user-requested start
            // offset, so there's no point in tracking it further.
            _start_kafka_offset_override = kafka::offset{};
        }
    } else {
        vlog(
          cst_log.warn,
          "{} Can't advance archive_start_offset to {} because it's smaller "
          "than the "
          "current value {}",
          _ntp,
          start_rp_offset,
          _archive_start_offset);
    }
    vlog(
      cst_log.info,
      "{} archive start offset moved to {} archive start delta set to {}",
      _ntp,
      _archive_start_offset,
      _archive_start_offset_delta);
}

void partition_manifest::set_archive_clean_offset(
  model::offset start_rp_offset, uint64_t size_bytes) {
    if (_archive_start_offset < start_rp_offset) {
        vlog(
          cst_log.error,
          "{} Requested to advance archive_clean_offset to {} which is greater "
          "than the current archive_start_offset {}. The offset won't be "
          "changed. Archive size won't be changed by {} bytes.",
          display_name(),
          start_rp_offset,
          _archive_start_offset,
          size_bytes);
        return;
    }
    if (_archive_clean_offset < start_rp_offset) {
        if (start_rp_offset == _start_offset) {
            // If we've truncated up to the start offset of the STM manifest,
            // the archive is completely removed.
            _archive_clean_offset = model::offset{};
            _archive_start_offset = model::offset{};
            _archive_start_offset_delta = model::offset_delta{};
        } else {
            _archive_clean_offset = start_rp_offset;
        }
        if (_archive_size_bytes >= size_bytes) {
            _archive_size_bytes -= size_bytes;
        } else {
            vlog(
              cst_log.error,
              "{} archive clean offset moved to {} but the archive size can't "
              "be updated because current size {} is smaller than the update "
              "{}. This needs to be reported and investigated.",
              display_name(),
              _archive_clean_offset,
              _archive_size_bytes,
              size_bytes);
            _archive_size_bytes = 0;
        }
    } else {
        vlog(
          cst_log.warn,
          "{} Can't advance archive_clean_offset to {} because it's smaller "
          "than the "
          "current value {}",
          _ntp,
          start_rp_offset,
          _archive_clean_offset);
    }

    // Prefix truncate to get rid of the spillover manifests
    // that have fallen below the clean offset.
    const auto previous_spillover_manifests_size = _spillover_manifests.size();
    if (_archive_clean_offset == model::offset{}) {
        // Handle the case where the entire archive was removed.
        _spillover_manifests = {};
    } else {
        std::optional<model::offset> truncation_point;
        for (const auto& spill : _spillover_manifests) {
            if (spill.base_offset >= _archive_clean_offset) {
                break;
            }
            truncation_point = spill.base_offset;
        }

        if (truncation_point) {
            vassert(
              _archive_clean_offset >= *truncation_point,
              "Attempt to prefix truncate the spillover manifest list above "
              "the "
              "archive clean offest: {} > {}",
              *truncation_point,
              _archive_clean_offset);
            _spillover_manifests.prefix_truncate(*truncation_point);
        }
    }

    vlog(
      cst_log.info,
      "{} archive clean offset moved to {} archive size set to {}; count of "
      "spillover manifests {} -> {}",
      _ntp,
      _archive_clean_offset,
      _archive_size_bytes,
      previous_spillover_manifests_size,
      _spillover_manifests.size());
}

bool partition_manifest::advance_start_kafka_offset(
  kafka::offset new_start_offset) {
    if (_start_kafka_offset_override > new_start_offset) {
        return false;
    }
    _start_kafka_offset_override = new_start_offset;
    vlog(
      cst_log.info,
      "{} start kafka offset override set to {}",
      _ntp,
      _start_kafka_offset_override);
    return true;
}

void partition_manifest::unsafe_reset() {
    *this = partition_manifest{_ntp, _rev, _mem_tracker};
}

bool partition_manifest::advance_highest_producer_id(model::producer_id pid) {
    if (_highest_producer_id >= pid) {
        return false;
    }
    _highest_producer_id = pid;
    return true;
}

bool partition_manifest::advance_start_offset(model::offset new_start_offset) {
    const auto previous_start_offset = _start_offset;

    if (new_start_offset > _start_offset && !_segments.empty()) {
        auto it = _segments.upper_bound(new_start_offset);
        if (it == _segments.begin()) {
            return false;
        }

        auto new_head_segment = [&] {
            auto prev = _segments.prev(it);
            if (prev->committed_offset >= new_start_offset) {
                return prev;
            }
            return std::move(it);
        }();

        model::offset advanced_start_offset = new_head_segment == end()
                                                ? new_start_offset
                                                : new_head_segment->base_offset;

        if (previous_start_offset > advanced_start_offset) {
            vlog(
              cst_log.error,
              "Previous start offset is greater than the new one: "
              "previous_start_offset={}, computed_new_start_offset={}, "
              "requested_new_start_offset={}",
              previous_start_offset,
              advanced_start_offset,
              new_start_offset);
            return false;
        }

        set_start_offset(advanced_start_offset);

        auto previous_head_segment = segment_containing(previous_start_offset);
        if (previous_head_segment == end()) {
            // This branch should never be taken. It indicates that the
            // in-memory manifest may be is in some sort of inconsistent state.
            vlog(
              cst_log.error,
              "Previous start offset is not within segment in "
              "manifest for {}: previous_start_offset={}",
              display_name(),
              previous_start_offset);
            previous_head_segment = _segments.begin();
        }

        // Note that we start subtracting from the cloud log size from the
        // previous head segments. This is required in order to account for
        // the case when two `truncate` commands are applied sequentially,
        // without a `cleanup_metadata` command in between to trim the list of
        // segments.
        kafka::offset highest_removed_offset{};
        for (auto it = std::move(previous_head_segment); it != new_head_segment;
             ++it) {
            highest_removed_offset = std::max(
              highest_removed_offset, it->last_kafka_offset());
            subtract_from_cloud_log_size(it->size_bytes);
        }

        // The new start offset has moved past the user-requested start offset,
        // so there's no point in tracking it further.
        if (
          highest_removed_offset != kafka::offset{}
          && highest_removed_offset >= _start_kafka_offset_override) {
            _start_kafka_offset_override = kafka::offset{};
        }
        return true;
    }
    return false;
}

fragmented_vector<partition_manifest::lw_segment_meta>
partition_manifest::lw_replaced_segments() const {
    return _replaced.copy();
}

fragmented_vector<segment_meta> partition_manifest::replaced_segments() const {
    fragmented_vector<segment_meta> res;
    for (const auto& s : _replaced) {
        res.push_back(lw_segment_meta::convert(s));
    }
    return res;
}

size_t partition_manifest::replaced_segments_count() const {
    return _replaced.size();
}

model::timestamp partition_manifest::last_partition_scrub() const {
    return _last_partition_scrub;
}

std::optional<model::offset> partition_manifest::last_scrubbed_offset() const {
    return _last_scrubbed_offset;
}

const anomalies& partition_manifest::detected_anomalies() const {
    return _detected_anomalies;
}

void partition_manifest::reset_scrubbing_metadata() {
    _detected_anomalies = {};
    _last_partition_scrub = model::timestamp::missing();
    _last_scrubbed_offset = std::nullopt;
}

std::optional<size_t> partition_manifest::move_aligned_offset_range(
  const segment_meta& replacing_segment) {
    size_t total_replaced_size = 0;
    auto replacing_path = generate_remote_segment_name(replacing_segment);
    for (auto it = _segments.lower_bound(replacing_segment.base_offset),
              end_it = _segments.end();
         it != end_it
         // The segment is considered replaced only if all its
         // offsets are covered by new segment's offset range
         && it->base_offset >= replacing_segment.base_offset
         && it->committed_offset <= replacing_segment.committed_offset;
         ++it) {
        if (generate_remote_segment_name(*it) == replacing_path) {
            // The replacing segment shouldn't be exactly the same as the
            // one that we already have in the manifest. Attempt to re-add
            // same segment twice leads to data loss.
            vlog(
              cst_log.warn,
              "{} segment is already added {}",
              _ntp,
              replacing_segment);
            return std::nullopt;
        }
        _replaced.push_back(lw_segment_meta::convert(*it));
        total_replaced_size += it->size_bytes;
    }

    return total_replaced_size;
}

std::optional<partition_manifest::add_segment_meta_result>
partition_manifest::add(segment_meta meta) {
    if (_start_offset == model::offset{} && _segments.empty()) {
        // This can happen if this is the first time we add something
        // to the manifest or if all data was removed previously.
        set_start_offset(meta.base_offset);
    }
    const auto total_replaced_size = move_aligned_offset_range(meta);

    if (!total_replaced_size) {
        return std::nullopt;
    }

    if (meta.ntp_revision == model::initial_revision_id{}) {
        meta.ntp_revision = _rev;
    }
    _segments.insert(meta);

    _last_offset = std::max(meta.committed_offset, _last_offset);
    if (meta.is_compacted) {
        _last_uploaded_compacted_offset = std::max(
          meta.committed_offset, _last_uploaded_compacted_offset);
    }

    subtract_from_cloud_log_size(total_replaced_size.value());
    _cloud_log_size_bytes += meta.size_bytes;
    return add_segment_meta_result{
      .bytes_replaced_range = total_replaced_size.value(),
      .bytes_new_range = meta.size_bytes};
}

std::optional<partition_manifest::add_segment_meta_result>
partition_manifest::add(const segment_name& name, const segment_meta& meta) {
    if (meta.segment_term != model::term_id{}) {
        return add(meta);
    }
    auto maybe_key = parse_segment_name(name);
    if (!maybe_key) {
        throw std::runtime_error(
          fmt_with_ctx(fmt::format, "can't parse segment name \"{}\"", name));
    }
    auto m = meta;
    m.segment_term = maybe_key->term;
    return add(m);
}

size_t partition_manifest::safe_segment_meta_to_add(
  std::vector<segment_meta> meta_list) const {
    struct manifest_substitute {
        model::offset last_offset;
        std::optional<segment_meta> last_segment;
        size_t num_accepted{0};
    };

    manifest_substitute subst{
      .last_offset = _last_offset,
      .last_segment = last_segment(),
    };

    for (const auto& m : meta_list) {
        if (_segments.empty() && !subst.last_segment.has_value()) {
            // The empty manifest can be started from any offset. If we deleted
            // all segments due to retention we should start from last uploaded
            // offset. The reuploads are not possible if the manifest is empty.
            const bool is_safe = subst.last_offset == model::offset{0}
                                 || model::next_offset(_last_offset)
                                      == m.base_offset;

            if (!is_safe) {
                vlog(
                  cst_log.error,
                  "[{}] New segment does not line up with last offset of empty "
                  "log: "
                  "last_offset: {}, new_segment: {}",
                  display_name(),
                  subst.last_offset,
                  m);
                break;
            }
            subst.last_offset = m.committed_offset;
            subst.last_segment = m;
            subst.num_accepted++;
        } else {
            // We have segments to check
            auto format_seg_meta_anomalies =
              [](const segment_meta_anomalies& smas) {
                  if (smas.empty()) {
                      return ss::sstring{};
                  }

                  std::vector<anomaly_type> types;
                  for (const auto& a : smas) {
                      types.push_back(a.type);
                  }

                  return ssx::sformat(
                    "{{anomaly_types: {}, new_segment: {}, previous_segment: "
                    "{}}}",
                    types,
                    smas.begin()->at,
                    smas.begin()->previous);
              };

            auto it = _segments.find(m.base_offset);
            if (it == _segments.end()) {
                // Segment added to tip of the log
                const auto last_seg = subst.last_segment;
                vassert(
                  last_seg.has_value(),
                  "Empty manifest, base_offset: {}",
                  m.base_offset);

                segment_meta_anomalies anomalies;
                scrub_segment_meta(m, last_seg, anomalies);
                if (!anomalies.empty()) {
                    vlog(
                      cst_log.error,
                      "[{}] New segment does not line up with previous "
                      "segment: {}",
                      display_name(),
                      format_seg_meta_anomalies(anomalies));
                    break;
                }
                // Only update if we extending the log
                subst.last_offset = m.committed_offset;
                subst.last_segment = m;
                subst.num_accepted++;
                continue;
            } else {
                // Segment reupload case:
                // The segment should be aligned with existing segments in the
                // manifest but there should be at least one segment covered by
                // 'm'.
                if (it != _segments.begin()) {
                    // Firstly, check that the replacement lines up with the
                    // segment preceeding it if one exists.
                    const auto prev_segment_it = _segments.prev(it);
                    segment_meta_anomalies anomalies;
                    scrub_segment_meta(m, *prev_segment_it, anomalies);
                    if (!anomalies.empty()) {
                        vlog(
                          cst_log.error,
                          "[{}] New replacement segment does not line up with "
                          "previous "
                          "segment: {}",
                          display_name(),
                          format_seg_meta_anomalies(anomalies));
                        break;
                    }
                }

                if (it->committed_offset == m.committed_offset) {
                    // 'm' is a reupload of an individual segment, the segment
                    // should have different size
                    if (it->size_bytes == m.size_bytes) {
                        vlog(
                          cst_log.error,
                          "[{}] New replacement segment has the same size as "
                          "replaced "
                          "segment: new_segment: {}, replaced_segment: {}",
                          display_name(),
                          m,
                          *it);
                        break;
                    }
                    subst.num_accepted++;
                    continue;
                }

                // 'm' is a reupload which merges multiple segments. The
                // committed offset of 'm' should match an existing segment.
                // Otherwise, the re-upload changes segment boundaries and is
                // *not valid*.
                ++it;
                bool boundary_found = false;
                while (it != _segments.end()) {
                    if (it->committed_offset == m.committed_offset) {
                        boundary_found = true;
                        break;
                    }
                    ++it;
                }

                if (!boundary_found) {
                    vlog(
                      cst_log.error,
                      "[{}] New replacement segment does not match the "
                      "committed "
                      "offset of "
                      "any previous segment: new_segment: {}",
                      display_name(),
                      m);
                    break;
                }
                subst.num_accepted++;
            }
        }
    }
    return subst.num_accepted;
}

bool partition_manifest::safe_segment_meta_to_add(const segment_meta& m) const {
    return safe_segment_meta_to_add(std::vector<segment_meta>{m}) == 1;
}

partition_manifest
partition_manifest::truncate(model::offset starting_rp_offset) {
    if (!advance_start_offset(starting_rp_offset)) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "can't truncate manifest up to {} due to misalignment",
          starting_rp_offset));
    }
    return truncate();
}

partition_manifest partition_manifest::truncate() {
    partition_manifest removed(_ntp, _rev);
    // copy segments that will be removed by the truncate op
    for (auto it = _segments.begin(),
              end_it = _segments.lower_bound(_start_offset);
         it != end_it && it->committed_offset < _start_offset;
         ++it) {
        // it->committed_offset < _start_offset should be true for the whole
        // [it, end_it) range
        removed.add(*it);
    }

    _segments.prefix_truncate(_start_offset);

    if (_segments.empty()) {
        // start offset only makes sense if we have segments
        set_start_offset({});
        // NOTE: _last_offset should not be reset
    }
    return removed;
}

partition_manifest partition_manifest::clone() const {
    struct segment_name_meta {
        segment_name name;
        segment_meta meta;
    };
    fragmented_vector<segment_name_meta> segments;
    fragmented_vector<segment_name_meta> replaced;
    fragmented_vector<segment_name_meta> spillover;
    for (const auto& m : _segments) {
        segments.push_back(
          {.name = generate_local_segment_name(m.base_offset, m.segment_term),
           .meta = m});
    }
    for (const auto& m : _replaced) {
        replaced.push_back(
          {.name = generate_local_segment_name(m.base_offset, m.segment_term),
           .meta = lw_segment_meta::convert(m)});
    }
    for (const auto& m : _spillover_manifests) {
        spillover.push_back(
          {.name = generate_local_segment_name(m.base_offset, m.segment_term),
           .meta = m});
    }
    partition_manifest tmp(
      _ntp,
      _rev,
      _mem_tracker,
      _start_offset,
      _last_offset,
      _last_uploaded_compacted_offset,
      _insync_offset,
      segments,
      replaced,
      _start_kafka_offset_override,
      _archive_start_offset,
      _archive_start_offset_delta,
      _archive_clean_offset,
      _archive_size_bytes,
      spillover,
      _last_partition_scrub,
      _last_scrubbed_offset,
      _detected_anomalies,
      _highest_producer_id,
      _applied_offset);
    return tmp;
}

void partition_manifest::spillover(const segment_meta& spillover_meta) {
    auto start_offset = model::next_offset(spillover_meta.committed_offset);
    auto append_tx = _spillover_manifests.append(spillover_meta);
    partition_manifest removed;
    auto num_segments = _segments.size();
    try {
        removed = truncate(start_offset);
    } catch (...) {
        if (_segments.size() < num_segments) {
            // If the segments list was actually truncated we
            // need to commit the changes to the _spillover_manifests
            // collection. Otherwise it will be inconsistent with the _segments.
            _spillover_manifests.flush_write_buffer();
        }
        throw;
    }
    // Commit changes to the spillover manifest list so they won't
    // be rolled back if any code below throws an exception.
    _spillover_manifests.flush_write_buffer();

    auto expected_meta = removed.make_manifest_metadata();

    if (expected_meta != spillover_meta) {
        vlog(
          cst_log.error,
          "[{}] Expected spillover metadata {} doesn't match actual spillover "
          "metadata {}",
          display_name(),
          expected_meta,
          spillover_meta);
    } else {
        vlog(
          cst_log.debug,
          "{} Applying spillover metadata {}",
          _ntp,
          spillover_meta);
    }
    // Update size of the archived part of the log.
    // It doesn't include segments which are remaining in the
    // manifest.
    _archive_size_bytes += removed.cloud_log_size();
}

segment_meta partition_manifest::make_manifest_metadata() const {
    return segment_meta{
      .size_bytes = cloud_log_size(),
      .base_offset = get_start_offset().value(),
      .committed_offset = get_last_offset(),
      .base_timestamp = begin()->base_timestamp,
      .max_timestamp = last_segment()->max_timestamp,
      .delta_offset = begin()->delta_offset,
      .ntp_revision = get_revision_id(),
      .archiver_term = begin()->segment_term,
      .segment_term = last_segment()->segment_term,
      .delta_offset_end = last_segment()->delta_offset_end,
      .sname_format = segment_name_format::v3,
      .metadata_size_hint = segments_metadata_bytes(),
    };
}

bool partition_manifest::safe_spillover_manifest(const segment_meta& meta) {
    // New spillover manifest should connect with previous spillover manifests
    // and *this manifest. The manifest is not truncated yet so meta.base_offset
    // should be equal to start offset and the meta.committed_offset + 1 should
    // be aligned with one of the segments.
    auto so = get_start_offset();
    if (!so.has_value()) {
        vlog(
          cst_log.warn,
          "{} Can't apply spillover manifest because the manifest is empty, {}",
          _ntp,
          meta);
        return false;
    }
    // Invariant: 'meta' contains the tail of the log stored in this manifest
    if (so.value() != meta.base_offset) {
        vlog(
          cst_log.warn,
          "{} Can't apply spillover manifest because the start offsets are not "
          "aligned: {} vs {}, {}",
          _ntp,
          so.value(),
          meta.base_offset,
          meta);
        return false;
    }
    auto next_so = model::next_offset(meta.committed_offset);
    auto it = find(next_so);
    if (it == end()) {
        // The end of the spillover manifest is not aligned with the segment
        // correctly.
        vlog(
          cst_log.warn,
          "{} Can't apply spillover manifest because the end of the manifest "
          "is not aligned, {}",
          _ntp,
          meta);
        return false;
    }
    // Invariant: 'meta' should be aligned perfectly with the previous spillover
    // manifest.
    if (_spillover_manifests.empty()) {
        return true;
    }
    if (
      model::next_offset(_spillover_manifests.last_segment()->committed_offset)
      == meta.base_offset) {
        return true;
    }
    vlog(
      cst_log.warn,
      "{} Can't apply spillover manifest because the end of the previous "
      "manifest {} "
      "is not aligned with the new one {}",
      _ntp,
      _spillover_manifests.last_segment(),
      meta);
    return false;
}

std::optional<partition_manifest::segment_meta>
partition_manifest::get(const partition_manifest::key& key) const {
    auto it = _segments.find(key);
    if (it == _segments.end()) {
        return std::nullopt;
    }
    return *it;
}

std::optional<partition_manifest::segment_meta>
partition_manifest::get(const segment_name& name) const {
    auto maybe_key = parse_segment_name(name);
    if (!maybe_key) {
        throw std::runtime_error(
          fmt_with_ctx(fmt::format, "can't parse segment name \"{}\"", name));
    }
    return get(maybe_key->base_offset);
}

partition_manifest::const_iterator
partition_manifest::find(model::offset o) const {
    auto it = _segments.lower_bound(o);
    if (it == _segments.end() || it->base_offset != o) {
        return end();
    }
    return it;
}

//@formatter:off
// clang-format off
/**
     ┌──────────────────────┐
     │ expect_manifest_start│
     └──────────┬───────────┘
                │
                │
                │
                │                                                                            EndObject()
                │  ┌────────────────────────────────────────────────────────────────────────────────────┐
                │  │                                                     Key("replaced")                |
                │  │  ┌───────────────────────────────────────────────────────────────────┐             |
                |  |  |                                                                   ▼             |
                │  │  │                       ┌───────────────┐                    ┌───────────────┐    |
                │  │  │      Key("segments")  │expect_segments│                    │expect_replaced│    |
   StartObject()│  │  │  ┌───────────────────►│    start      │                    │    start      │    |
                │  │  │  │                    └──────┬────────┘                    └──────┬────────┘    |
                │  │  │  │                           │StartObject()          StartObject()│             |
                ▼  ▼  ▼  │                           ▼                                    ▼             |
        ┌────────────────┴──┐                  ┌──────────────┐                    ┌───────────────┐    |
┌───────┤expect_manifest_key│◄─────────────────┤expect_segment│◄───┐               |expect_replaced│◄───┘
│       └──┬────────────────┘   EndObject()    │   path       │    │               │   path        │◄───┐
│          │          ▲                        └────┬─────────┘    │               └─────┬─────────┘    │
│     Key()│          │String()                     │              │                     │              │
│          │          │Uint()                  Key()│              │                Key()│              │
│          │          │Null()                       ▼              │EndObject()          ▼              │
│          ▼          │                       ┌──────────────┐     │              ┌───────────────┐     │
│      ┌──────────────┴──────┐                │expect_segment│     │              │expect_replaced│     │
│      │expect_manifest_value│                │ meta_start   │     │              │ meta_start    │     │
│      └─────────────────────┘                └─────┬────────┘     │              └──────┬────────┘     │
│                                                   │              │                     │              │
│                                      StartObject()│              │        StartObject()│              │
│EndObject()                                        ▼              │                     ▼              │
│                                             ┌───────────────┐    │              ┌────────────────┐    │
│                                             │ expect_segment├────┘              │ expect_replaced├────┘
│                                             │  meta_key     │                   │  meta_key      │
│            ┌────────┐                       └───┬───────────┘                   └────┬───────────┘
│            │terminal│                           │      ▲String()                     │     ▲String()
└───────────►│  state │                           │      │Uint()                       │     │Uint()
             └────────┘                      Key()│      │Bool()                  Key()│     │Bool()
                                                  ▼      │Null()                       ▼     │Null()
                                               ┌─────────┴─────┐                  ┌──────────┴─────┐
                                               │ expect_segment│                  │ expect_replaced│
                                               │  meta_value   │                  │  meta_value    │
                                               └───────────────┘                  └────────────────┘
**/
// clang-format on
//@formatter:on

struct partition_manifest_handler
  : public rapidjson::
      BaseReaderHandler<rapidjson::UTF8<>, partition_manifest_handler> {
    using key_string = ss::basic_sstring<char, uint32_t, 31>;

    explicit partition_manifest_handler(ss::shared_ptr<util::mem_tracker> mt)
      : _manifest_mem_tracker(std::move(mt)) {}

    bool StartObject() {
        switch (_state) {
        case state::expect_manifest_start:
            _state = state::expect_manifest_key;
            return true;
        case state::expect_segments_start:
            _state = state::expect_segment_path;
            return true;
        case state::expect_replaced_start:
            _state = state::expect_replaced_path;
            return true;
        case state::expect_segment_meta_start:
            _state = state::expect_segment_meta_key;
            return true;
        case state::expect_replaced_meta_start:
            _state = state::expect_replaced_meta_key;
            return true;
        case state::expect_spillover_meta_start:
            _state = state::expect_spillover_meta_key;
            return true;
        case state::expect_manifest_key:
        case state::expect_manifest_value:
        case state::expect_segment_path:
        case state::expect_segment_meta_key:
        case state::expect_segment_meta_value:
        case state::expect_replaced_path:
        case state::expect_replaced_meta_key:
        case state::expect_replaced_meta_value:
        case state::expect_spillover_meta_key:
        case state::expect_spillover_meta_value:
        case state::expect_spillover_start:
        case state::terminal_state:
            return false;
        }
    }

    bool Key(const char* str, rapidjson::SizeType length, bool /*copy*/) {
        switch (_state) {
        case state::expect_manifest_key:
            _manifest_key = key_string(str, length);
            if (_manifest_key == "segments") {
                _state = state::expect_segments_start;
            } else if (_manifest_key == "replaced") {
                _state = state::expect_replaced_start;
            } else if (_manifest_key == "spillover") {
                _state = state::expect_spillover_start;
            } else {
                _state = state::expect_manifest_value;
            }
            return true;
        case state::expect_segment_path:
        case state::expect_replaced_path:
            _segment_name = segment_name{str};
            _parsed_segment_key = parse_segment_name(_segment_name);
            if (!_parsed_segment_key) {
                throw std::runtime_error(fmt_with_ctx(
                  fmt::format,
                  "can't parse segment name \"{}\"",
                  _segment_name));
            }
            _segment_key = {
              .base_offset = _parsed_segment_key->base_offset,
              .term = _parsed_segment_key->term};
            if (_state == state::expect_segment_path) {
                _state = state::expect_segment_meta_start;
            } else if (_state == state::expect_replaced_path) {
                _state = state::expect_replaced_meta_start;
            }
            return true;
        case state::expect_segment_meta_key:
            _segment_meta_key = key_string(str, length);
            _state = state::expect_segment_meta_value;
            return true;
        case state::expect_replaced_meta_key:
            _segment_meta_key = key_string(str, length);
            _state = state::expect_replaced_meta_value;
            return true;
        case state::expect_spillover_meta_key:
            _segment_meta_key = key_string(str, length);
            _state = state::expect_spillover_meta_value;
            return true;
        case state::expect_manifest_start:
        case state::expect_manifest_value:
        case state::expect_segments_start:
        case state::expect_replaced_start:
        case state::expect_segment_meta_start:
        case state::expect_segment_meta_value:
        case state::expect_replaced_meta_start:
        case state::expect_replaced_meta_value:
        case state::expect_spillover_start:
        case state::expect_spillover_meta_start:
        case state::expect_spillover_meta_value:
        case state::terminal_state:
            return false;
        }
    }

    bool String(const char* str, rapidjson::SizeType length, bool /*copy*/) {
        std::string_view sv(str, length);
        switch (_state) {
        case state::expect_manifest_value:
            if (_manifest_key == "namespace") {
                _namespace = model::ns(ss::sstring(sv));
            } else if (_manifest_key == "topic") {
                _topic = model::topic(ss::sstring(sv));
            } else {
                return false;
            }
            _state = state::expect_manifest_key;
            return true;
        case state::expect_manifest_start:
        case state::expect_manifest_key:
        case state::expect_segments_start:
        case state::expect_replaced_start:
        case state::expect_segment_path:
        case state::expect_segment_meta_start:
        case state::expect_segment_meta_key:
        case state::expect_segment_meta_value:
        case state::expect_replaced_path:
        case state::expect_replaced_meta_start:
        case state::expect_replaced_meta_key:
        case state::expect_replaced_meta_value:
        case state::terminal_state:
        case state::expect_spillover_start:
        case state::expect_spillover_meta_key:
        case state::expect_spillover_meta_start:
        case state::expect_spillover_meta_value:
            return false;
        }
    }

    bool Uint(unsigned u) { return Uint64(u); }

    bool Uint64(uint64_t u) {
        switch (_state) {
        case state::expect_manifest_value:
            if (_manifest_key == "version") {
                _version = u;
            } else if (_manifest_key == "partition") {
                _partition_id = model::partition_id(u);
            } else if (_manifest_key == "revision") {
                _revision_id = model::initial_revision_id(u);
            } else if (_manifest_key == "last_offset") {
                _last_offset = model::offset(u);
            } else if (_manifest_key == "start_offset") {
                _start_offset = model::offset(u);
            } else if (_manifest_key == "last_uploaded_compacted_offset") {
                _last_uploaded_compacted_offset = model::offset(u);
            } else if (_manifest_key == "insync_offset") {
                _insync_offset = model::offset(u);
            } else if (_manifest_key == "cloud_log_size_bytes") {
                _cloud_log_size_bytes = u;
            } else if (_manifest_key == "archive_start_offset") {
                _archive_start_offset = model::offset(u);
            } else if (_manifest_key == "archive_start_offset_delta") {
                _archive_start_offset_delta = model::offset_delta(u);
            } else if (_manifest_key == "archive_clean_offset") {
                _archive_clean_offset = model::offset(u);
            } else if (_manifest_key == "start_kafka_offset") {
                _start_kafka_offset = kafka::offset(u);
            } else if (_manifest_key == "archive_size_bytes") {
                _archive_size_bytes = u;
            } else if (_manifest_key == "last_partition_scrub") {
                _last_partition_scrub = model::timestamp(u);
            } else if (_manifest_key == "last_scrubbed_offset") {
                _last_scrubbed_offset = model::offset(u);
            } else {
                return false;
            }
            _state = state::expect_manifest_key;
            return true;
        case state::expect_replaced_meta_value:
        case state::expect_segment_meta_value:
        case state::expect_spillover_meta_value:
            if (_segment_meta_key == "size_bytes") {
                _size_bytes = static_cast<size_t>(u);
            } else if (_segment_meta_key == "base_offset") {
                _base_offset = model::offset(u);
            } else if (_segment_meta_key == "committed_offset") {
                _committed_offset = model::offset(u);
            } else if (_segment_meta_key == "base_timestamp") {
                _base_timestamp = model::timestamp(u);
            } else if (_segment_meta_key == "max_timestamp") {
                _max_timestamp = model::timestamp(u);
            } else if (_segment_meta_key == "delta_offset") {
                _delta_offset = model::offset_delta(u);
            } else if (_segment_meta_key == "ntp_revision") {
                _ntp_revision = model::initial_revision_id(u);
            } else if (_segment_meta_key == "archiver_term") {
                _archiver_term = model::term_id(u);
            } else if (_segment_meta_key == "segment_term") {
                _segment_term = model::term_id(u);
            } else if (_segment_meta_key == "delta_offset_end") {
                _delta_offset_end = model::offset_delta(u);
            } else if (_segment_meta_key == "sname_format") {
                _meta_sname_format = segment_name_format(u);
            } else if (_segment_meta_key == "metadata_size_hint") {
                _metadata_size_hint = static_cast<size_t>(u);
            }
            if (_state == state::expect_segment_meta_value) {
                _state = state::expect_segment_meta_key;
            } else if (_state == state::expect_replaced_meta_value) {
                _state = state::expect_replaced_meta_key;
            } else if (_state == state::expect_spillover_meta_value) {
                _state = state::expect_spillover_meta_key;
            }
            return true;
        case state::expect_manifest_start:
        case state::expect_manifest_key:
        case state::expect_segments_start:
        case state::expect_replaced_start:
        case state::expect_segment_path:
        case state::expect_segment_meta_start:
        case state::expect_segment_meta_key:
        case state::expect_replaced_path:
        case state::expect_replaced_meta_start:
        case state::expect_replaced_meta_key:
        case state::expect_spillover_start:
        case state::expect_spillover_meta_key:
        case state::expect_spillover_meta_start:
        case state::terminal_state:
            return false;
        }
    }

    bool EndObject(rapidjson::SizeType /*size*/) {
        switch (_state) {
        case state::expect_manifest_key:
            check_manifest_fields_are_present();
            _state = state::terminal_state;
            return true;
        case state::expect_segment_path:
        case state::expect_replaced_path:
            _state = state::expect_manifest_key;
            return true;
        case state::expect_segment_meta_key:
        case state::expect_replaced_meta_key:
        case state::expect_spillover_meta_key:
            check_that_required_meta_fields_are_present();
            _meta = {
              .is_compacted = _is_compacted.value_or(false),
              .size_bytes = _size_bytes.value(),
              .base_offset = _base_offset.value(),
              .committed_offset = _committed_offset.value(),
              .base_timestamp = _base_timestamp.value_or(
                model::timestamp::missing()),
              .max_timestamp = _max_timestamp.value_or(
                model::timestamp::missing()),
              .delta_offset = _delta_offset.value_or(
                model::offset_delta::min()),
              .ntp_revision = _ntp_revision.value_or(
                _revision_id.value_or(model::initial_revision_id())),
              .archiver_term = _archiver_term.value_or(model::term_id{}),
              .segment_term = _segment_term.value_or(_segment_key.term),
              .delta_offset_end = _delta_offset_end.value_or(
                model::offset_delta::min()),
              .sname_format = _meta_sname_format.value_or(
                segment_name_format::v1),
              .metadata_size_hint = _metadata_size_hint.value_or(0)};
            if (_state == state::expect_segment_meta_key) {
                if (!_segments) {
                    _segments = std::make_unique<segment_map>();
                }
                _segments->insert(_meta);
                _state = state::expect_segment_path;
            } else if (_state == state::expect_replaced_meta_key) {
                if (!_replaced) {
                    _replaced = std::make_unique<replaced_segments_list>();
                }

                // Set version to v2 if not set explicitly and size_bytes is non
                // zero.
                if (
                  _meta.sname_format == segment_name_format::v1
                  && _meta.size_bytes != 0) {
                    _meta.sname_format = segment_name_format::v2;
                }

                _replaced->push_back(
                  partition_manifest::lw_segment_meta::convert(_meta));
                _state = state::expect_replaced_path;
            } else if (_state == state::expect_spillover_meta_key) {
                _meta.is_compacted = false;
                _meta.sname_format = segment_name_format::v3;
                if (!_spillover) {
                    _spillover = std::make_unique<segment_meta_cstore>();
                }
                _spillover->insert(_meta);
                _state = state::expect_spillover_meta_start;
            }
            clear_meta_fields();
            return true;
        case state::expect_manifest_start:
        case state::expect_manifest_value:
        case state::expect_segments_start:
        case state::expect_replaced_start:
        case state::expect_segment_meta_start:
        case state::expect_segment_meta_value:
        case state::expect_replaced_meta_start:
        case state::expect_replaced_meta_value:
        case state::expect_spillover_start:
        case state::expect_spillover_meta_start:
        case state::expect_spillover_meta_value:
        case state::terminal_state:
            return false;
        }
    }

    bool Bool(bool b) {
        switch (_state) {
        case state::expect_segment_meta_value:
            if (_segment_meta_key == "is_compacted") {
                _is_compacted = b;
                _state = state::expect_segment_meta_key;
                return true;
            }
            return false;
        case state::expect_replaced_meta_value:
            if (_segment_meta_key == "is_compacted") {
                _is_compacted = b;
                _state = state::expect_replaced_meta_key;
                return true;
            }
            return false;
        case state::expect_spillover_meta_value:
            /**
             * Spillover manifest is never compacted, simply skip if the field
             * is present
             */
            if (_segment_meta_key == "is_compacted") {
                _state = state::expect_spillover_meta_key;
                return true;
            }
            return false;
        case state::expect_spillover_start:
        case state::expect_spillover_meta_start:
        case state::expect_spillover_meta_key:
        case state::expect_manifest_start:
        case state::expect_manifest_key:
        case state::expect_manifest_value:
        case state::expect_segments_start:
        case state::expect_segment_path:
        case state::expect_segment_meta_start:
        case state::expect_segment_meta_key:
        case state::expect_replaced_start:
        case state::expect_replaced_path:
        case state::expect_replaced_meta_start:
        case state::expect_replaced_meta_key:
        case state::terminal_state:

            return false;
        }
    }

    bool Null() {
        switch (_state) {
        case state::expect_manifest_value:
            _state = state::expect_manifest_key;
            return true;
        case state::expect_segment_meta_value:
            _state = state::expect_segment_meta_key;
            return true;
        case state::expect_manifest_start:
        case state::expect_manifest_key:
        case state::expect_segments_start:
        case state::expect_segment_path:
        case state::expect_segment_meta_start:
        case state::expect_segment_meta_key:
        case state::expect_replaced_start:
        case state::expect_replaced_path:
        case state::expect_replaced_meta_start:
        case state::expect_replaced_meta_key:
        case state::expect_replaced_meta_value:
            _state = state::expect_replaced_meta_key;
            return true;
        case state::expect_spillover_start:
        case state::expect_spillover_meta_start:
        case state::expect_spillover_meta_key:
        case state::expect_spillover_meta_value:
            _state = state::expect_spillover_meta_key;
            return true;
        case state::terminal_state:
            return false;
        }
    }
    bool StartArray() {
        switch (_state) {
        case state::expect_spillover_start:
            _state = state::expect_spillover_meta_start;
            return true;
        case state::expect_manifest_value:
        case state::expect_segment_meta_value:
        case state::expect_manifest_start:
        case state::expect_manifest_key:
        case state::expect_segments_start:
        case state::expect_segment_path:
        case state::expect_segment_meta_start:
        case state::expect_segment_meta_key:
        case state::expect_replaced_start:
        case state::expect_replaced_path:
        case state::expect_replaced_meta_start:
        case state::expect_replaced_meta_key:
        case state::expect_replaced_meta_value:
        case state::expect_spillover_meta_start:
        case state::expect_spillover_meta_key:
        case state::expect_spillover_meta_value:
        case state::terminal_state:
            return false;
        };
    }
    bool EndArray(rapidjson::SizeType /*size*/) {
        switch (_state) {
        case state::expect_spillover_meta_start:
            if (!_spillover) {
                _spillover = std::make_unique<segment_meta_cstore>();
            }
            _spillover->insert(_meta);
            _state = state::expect_manifest_key;

            clear_meta_fields();
            return true;
        case state::expect_spillover_start:
        case state::expect_manifest_value:
        case state::expect_segment_meta_value:
        case state::expect_manifest_start:
        case state::expect_manifest_key:
        case state::expect_segments_start:
        case state::expect_segment_path:
        case state::expect_segment_meta_start:
        case state::expect_segment_meta_key:
        case state::expect_replaced_start:
        case state::expect_replaced_path:
        case state::expect_replaced_meta_start:
        case state::expect_replaced_meta_key:
        case state::expect_replaced_meta_value:
        case state::expect_spillover_meta_key:
        case state::expect_spillover_meta_value:
        case state::terminal_state:
            return false;
        };
    }
    bool Default() { return false; }

    enum class state {
        expect_manifest_start,
        expect_manifest_key,
        expect_manifest_value,
        expect_segments_start,
        expect_segment_path,
        expect_segment_meta_start,
        expect_segment_meta_key,
        expect_segment_meta_value,
        expect_replaced_start,
        expect_replaced_path,
        expect_replaced_meta_start,
        expect_replaced_meta_key,
        expect_replaced_meta_value,
        expect_spillover_start,
        expect_spillover_meta_start,
        expect_spillover_meta_key,
        expect_spillover_meta_value,
        terminal_state,
    } _state{state::expect_manifest_start};

    using segment_map = partition_manifest::segment_map;
    using replaced_segments_list = partition_manifest::replaced_segments_list;

    key_string _manifest_key;
    key_string _segment_meta_key;
    segment_name _segment_name;
    std::optional<segment_name_components> _parsed_segment_key;
    segment_name_components _segment_key;
    partition_manifest::segment_meta _meta;
    std::unique_ptr<segment_map> _segments;
    std::unique_ptr<replaced_segments_list> _replaced;
    std::unique_ptr<segment_meta_cstore> _spillover;

    // required manifest fields
    std::optional<int32_t> _version;
    std::optional<model::ns> _namespace;
    std::optional<model::topic> _topic;
    std::optional<int32_t> _partition_id;
    std::optional<model::initial_revision_id> _revision_id;
    std::optional<model::offset> _last_offset;

    // optional manifest fields
    std::optional<model::offset> _start_offset;
    std::optional<model::offset> _last_uploaded_compacted_offset;
    std::optional<model::offset> _insync_offset;
    std::optional<size_t> _cloud_log_size_bytes;
    std::optional<model::offset> _archive_start_offset;
    std::optional<model::offset_delta> _archive_start_offset_delta;
    std::optional<model::offset> _archive_clean_offset;
    std::optional<kafka::offset> _start_kafka_offset;
    std::optional<size_t> _archive_size_bytes;
    std::optional<model::timestamp> _last_partition_scrub;
    std::optional<model::offset> _last_scrubbed_offset;

    // required segment meta fields
    std::optional<bool> _is_compacted;
    std::optional<size_t> _size_bytes;
    std::optional<model::offset> _base_offset;
    std::optional<model::offset> _committed_offset;

    // optional segment meta fields
    std::optional<model::timestamp> _base_timestamp;
    std::optional<model::timestamp> _max_timestamp;
    std::optional<model::offset_delta> _delta_offset;
    std::optional<model::initial_revision_id> _ntp_revision;
    std::optional<model::term_id> _archiver_term;
    std::optional<model::term_id> _segment_term;
    std::optional<model::offset_delta> _delta_offset_end;
    std::optional<segment_name_format> _meta_sname_format;
    std::optional<size_t> _metadata_size_hint;

    ss::shared_ptr<util::mem_tracker> _manifest_mem_tracker;

    void check_that_required_meta_fields_are_present() {
        if (!_size_bytes) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "Missing size_bytes value in {} segment meta",
              _segment_name));
        }
        if (!_base_offset) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "Missing base_offset value in {} segment meta",
              _segment_name));
        }
        if (!_committed_offset) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "Missing committed_offset value in {} segment meta",
              _segment_name));
        }
    }

    void clear_meta_fields() {
        // required fields
        _size_bytes = std::nullopt;
        _base_offset = std::nullopt;
        _committed_offset = std::nullopt;

        // optional segment meta fields
        _is_compacted = std::nullopt;
        _base_timestamp = std::nullopt;
        _max_timestamp = std::nullopt;
        _delta_offset = std::nullopt;
        _ntp_revision = std::nullopt;
        _archiver_term = std::nullopt;
        _segment_term = std::nullopt;
        _delta_offset_end = std::nullopt;
        _meta_sname_format = std::nullopt;
        _metadata_size_hint = std::nullopt;
    }

    void check_manifest_fields_are_present() {
        if (!_version) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format, "Missing version value partition manifest"));
        }
        if (!_namespace) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format, "Missing namespace value in partition manifest"));
        }
        if (!_topic) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format, "Missing topic value in partition manifest"));
        }
        if (!_partition_id) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format, "Missing partition_id value in partition manifest"));
        }
        if (!_revision_id) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format, "Missing revision_id value in partition manifest"));
        }
        if (!_last_offset) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format, "Missing last_offset value in partition manifest"));
        }
    }
};

void partition_manifest::update_with_json(iobuf buf) {
    iobuf_istreambuf ibuf(buf);
    std::istream stream(&ibuf);
    json::IStreamWrapper wrapper(stream);
    rapidjson::Reader reader;
    partition_manifest_handler handler(_mem_tracker);

    if (reader.Parse(wrapper, handler)) {
        partition_manifest::do_update(std::move(handler));
    } else {
        rapidjson::ParseErrorCode e = reader.GetParseErrorCode();
        size_t o = reader.GetErrorOffset();

        // Hexdump 1kb region around the bad manifest
        buf.trim_front(o - std::min(size_t{512}, o));
        vlog(
          cst_log.warn,
          "Failed to parse manifest at 0x{:08x}: {}",
          o,
          buf.hexdump(1024));

        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Failed to parse partition manifest {}: {} at offset {}",
          prefixed_partition_manifest_json_path(get_ntp(), get_revision_id()),
          rapidjson::GetParseError_En(e),
          o));
    }
}

ss::future<> partition_manifest::update(
  manifest_format serialization_format, ss::input_stream<char> is) {
    iobuf result;
    std::exception_ptr e_ptr;
    try {
        auto os = make_iobuf_ref_output_stream(result);
        co_await ss::copy(is, os);
    } catch (...) {
        e_ptr = std::current_exception();
    }
    co_await is.close();
    if (e_ptr) {
        std::rethrow_exception(e_ptr);
    }

    switch (serialization_format) {
    case manifest_format::json:
        update_with_json(std::move(result));
        break;
    case manifest_format::serde:
        from_iobuf(std::move(result));
        break;
    }
}

template<typename Enum>
requires std::is_enum_v<Enum>
constexpr auto to_underlying(Enum e) {
    return static_cast<std::underlying_type_t<Enum>>(e);
}

void partition_manifest::do_update(partition_manifest_handler&& handler) {
    if (
      handler._version != to_underlying(manifest_version::v1)
      && handler._version != to_underlying(manifest_version::v2)
      && handler._version != to_underlying(manifest_version::v3)
      && handler._version != to_underlying(manifest_version::v4)) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "partition manifest version {} is not supported",
          handler._version));
    }
    _rev = handler._revision_id.value();
    _ntp = model::ntp(
      handler._namespace.value(),
      handler._topic.value(),
      handler._partition_id.value());
    _last_offset = handler._last_offset.value();

    if (handler._start_offset) {
        set_start_offset(handler._start_offset.value());
    } else {
        set_start_offset({});
    }

    _last_uploaded_compacted_offset
      = handler._last_uploaded_compacted_offset.value_or(model::offset{});

    if (handler._insync_offset) {
        _insync_offset = handler._insync_offset.value();
    }

    if (handler._archive_start_offset) {
        _archive_start_offset = handler._archive_start_offset.value();
    }

    if (handler._archive_start_offset_delta) {
        _archive_start_offset_delta
          = handler._archive_start_offset_delta.value();
    }

    if (handler._archive_clean_offset) {
        _archive_clean_offset = handler._archive_clean_offset.value();
    }

    if (handler._segments) {
        _segments = std::move(*handler._segments);
        if (handler._start_offset == std::nullopt && !_segments.empty()) {
            // Backward compatibility. Old manifest format doesn't have
            // start_offset field. In this case we need to set it implicitly.
            set_start_offset(_segments.begin()->base_offset);
        }
    }
    if (handler._replaced) {
        _replaced = std::move(*handler._replaced);
    }

    if (handler._archive_size_bytes) {
        _archive_size_bytes = *handler._archive_size_bytes;
    }

    if (handler._spillover) {
        _spillover_manifests = std::move(*handler._spillover);
    }

    if (handler._cloud_log_size_bytes) {
        _cloud_log_size_bytes = handler._cloud_log_size_bytes.value();
    } else {
        _cloud_log_size_bytes = compute_cloud_log_size();
    }

    _start_kafka_offset_override = handler._start_kafka_offset.value_or(
      kafka::offset{});

    _last_partition_scrub = handler._last_partition_scrub.value_or(
      model::timestamp::missing());

    _last_scrubbed_offset = handler._last_scrubbed_offset;
}

// This object is supposed to track state of the asynchronous
// serialization process. It stores information about the part
// which was serialized so far. Methods like serialize_begin,
// serialized_end, serialize_segment are using it to pause and
// resume operation. It's not supposed to be reused or used for
// any other purpose.
struct partition_manifest::serialization_cursor {
    serialization_cursor(std::ostream& out, size_t max_segments)
      : wrapper(out)
      , writer(wrapper)
      , max_segments_per_call(max_segments) {}

    json::OStreamWrapper wrapper;
    json::Writer<json::OStreamWrapper> writer;
    // Next serialized offset
    model::offset next_offset{};
    size_t segments_serialized{0};
    size_t max_segments_per_call{0};
    // Next serialized spillover manifest offset
    model::offset next_spill_offset{};
    size_t spills_serialized{0};

    // Flags that indicate serialization progress
    bool prologue_done{false};
    bool segments_done{false};
    bool replaced_done{false};
    bool spillover_done{false};
    bool epilogue_done{false};
};

ss::future<iobuf> partition_manifest::serialize_buf() const {
    return ss::make_ready_future<iobuf>(to_iobuf());
}

void partition_manifest::serialize_json(std::ostream& out) const {
    serialization_cursor_ptr c = make_cursor(out);
    serialize_begin(c);
    while (!c->segments_done) {
        serialize_segments(c);
    }
    serialize_replaced(c);
    while (!c->spillover_done) {
        serialize_spillover(c);
    }
    serialize_end(c);
}

ss::future<>
partition_manifest::serialize_json(ss::output_stream<char>& output) const {
    iobuf serialized;
    iobuf_ostreambuf obuf(serialized);
    std::ostream os(&obuf);
    serialization_cursor_ptr c = make_cursor(os);
    serialize_begin(c);
    while (!c->segments_done) {
        serialize_segments(c);

        co_await write_iobuf_to_output_stream(
          serialized.share(0, serialized.size_bytes()), output);
        serialized.clear();
    }
    serialize_replaced(c);
    while (!c->spillover_done) {
        serialize_spillover(c);

        co_await write_iobuf_to_output_stream(
          serialized.share(0, serialized.size_bytes()), output);
        serialized.clear();
    }
    serialize_end(c);

    co_await write_iobuf_to_output_stream(std::move(serialized), output);
}

partition_manifest::serialization_cursor_ptr
partition_manifest::make_cursor(std::ostream& out) const {
    return ss::make_lw_shared<serialization_cursor>(out, 500);
}

void partition_manifest::serialize_begin(
  partition_manifest::serialization_cursor_ptr cursor) const {
    vassert(cursor->prologue_done == false, "Prologue is already serialized");
    auto& w = cursor->writer;
    w.StartObject();
    w.Key("version");
    w.Int(static_cast<int>(manifest_version::v1));
    w.Key("namespace");
    w.String(_ntp.ns().c_str());
    w.Key("topic");
    w.String(_ntp.tp.topic().c_str());
    w.Key("partition");
    w.Int64(_ntp.tp.partition());
    w.Key("revision");
    w.Int64(_rev());
    w.Key("last_offset");
    w.Int64(_last_offset());
    if (_insync_offset != model::offset{}) {
        w.Key("insync_offset");
        w.Int64(_insync_offset());
    }
    if (_start_offset != model::offset{}) {
        w.Key("start_offset");
        w.Int64(_start_offset());
    }
    if (_last_uploaded_compacted_offset != model::offset{}) {
        w.Key("last_uploaded_compacted_offset");
        w.Int64(_last_uploaded_compacted_offset());
    }
    w.Key("cloud_log_size_bytes");
    w.Uint64(_cloud_log_size_bytes);
    if (_archive_start_offset != model::offset{}) {
        w.Key("archive_start_offset");
        w.Int64(_archive_start_offset());
    }
    if (_archive_start_offset_delta != model::offset_delta{}) {
        w.Key("archive_start_offset_delta");
        w.Int64(_archive_start_offset_delta());
    }
    if (_archive_clean_offset != model::offset{}) {
        w.Key("archive_clean_offset");
        w.Int64(_archive_clean_offset());
    }
    if (_start_kafka_offset_override != kafka::offset{}) {
        w.Key("start_kafka_offset");
        w.Int64(_start_kafka_offset_override());
    }
    if (_archive_size_bytes != 0) {
        w.Key("archive_size_bytes");
        w.Int64(static_cast<int64_t>(_archive_size_bytes));
    }
    if (_last_partition_scrub != model::timestamp::missing()) {
        w.Key("last_partition_scrub");
        w.Int64(static_cast<int64_t>(_last_partition_scrub()));
    }
    if (_last_scrubbed_offset != std::nullopt) {
        w.Key("last_scrubbed_offset");
        w.Int64(static_cast<int64_t>(*_last_scrubbed_offset));
    }
    if (_highest_producer_id != model::producer_id{}) {
        w.Key("highest_producer_id");
        w.Int64(_highest_producer_id());
    }
    cursor->prologue_done = true;
}

void partition_manifest::serialize_segment_meta(
  const segment_meta& meta, serialization_cursor_ptr cursor) const {
    vassert(
      meta.segment_term != model::term_id{},
      "Term id is not initialized, base offset {}",
      meta.base_offset);
    auto& w = cursor->writer;
    auto sn = generate_local_segment_name(meta.base_offset, meta.segment_term);
    w.Key(sn());
    w.StartObject();
    w.Key("is_compacted");
    w.Bool(meta.is_compacted);
    w.Key("size_bytes");
    w.Int64(meta.size_bytes);
    w.Key("committed_offset");
    w.Int64(meta.committed_offset());
    w.Key("base_offset");
    w.Int64(meta.base_offset());
    if (meta.base_timestamp != model::timestamp::missing()) {
        w.Key("base_timestamp");
        w.Int64(meta.base_timestamp.value());
    }
    if (meta.max_timestamp != model::timestamp::missing()) {
        w.Key("max_timestamp");
        w.Int64(meta.max_timestamp.value());
    }
    if (meta.delta_offset != model::offset_delta::min()) {
        w.Key("delta_offset");
        w.Int64(meta.delta_offset());
    }
    if (meta.ntp_revision != _rev) {
        vassert(
          meta.ntp_revision != model::initial_revision_id(),
          "ntp {}: missing ntp_revision for segment {} in the manifest",
          _ntp,
          sn);
        w.Key("ntp_revision");
        w.Int64(meta.ntp_revision());
    }
    if (meta.archiver_term != model::term_id::min()) {
        w.Key("archiver_term");
        w.Int64(meta.archiver_term());
    }
    w.Key("segment_term");
    w.Int64(meta.segment_term());
    if (
      meta.sname_format >= segment_name_format::v2
      && meta.delta_offset_end != model::offset_delta::min()) {
        w.Key("delta_offset_end");
        w.Int64(meta.delta_offset_end());
    }
    if (meta.sname_format != segment_name_format::v1) {
        w.Key("sname_format");
        w.Int64(static_cast<int16_t>(meta.sname_format));
    }
    if (meta.sname_format == segment_name_format::v3) {
        w.Key("metadata_size_hint");
        w.Int64(static_cast<int64_t>(meta.metadata_size_hint));
    }
    w.EndObject();
}

void partition_manifest::serialize_spillover_manifest_meta(
  const segment_meta& meta, serialization_cursor_ptr cursor) const {
    vassert(
      meta.segment_term != model::term_id{},
      "Term id is not initialized, base offset {}",
      meta.base_offset);
    auto& w = cursor->writer;
    w.StartObject();
    w.Key("size_bytes");
    w.Int64(meta.size_bytes);
    w.Key("committed_offset");
    w.Int64(meta.committed_offset());
    w.Key("base_offset");
    w.Int64(meta.base_offset());
    if (meta.base_timestamp != model::timestamp::missing()) {
        w.Key("base_timestamp");
        w.Int64(meta.base_timestamp.value());
    }
    if (meta.max_timestamp != model::timestamp::missing()) {
        w.Key("max_timestamp");
        w.Int64(meta.max_timestamp.value());
    }
    if (meta.delta_offset != model::offset_delta::min()) {
        w.Key("delta_offset");
        w.Int64(meta.delta_offset());
    }
    if (meta.ntp_revision != _rev) {
        w.Key("ntp_revision");
        w.Int64(meta.ntp_revision());
    }
    if (meta.archiver_term != model::term_id::min()) {
        w.Key("archiver_term");
        w.Int64(meta.archiver_term());
    }
    w.Key("segment_term");
    w.Int64(meta.segment_term());
    if (meta.delta_offset_end != model::offset_delta::min()) {
        w.Key("delta_offset_end");
        w.Int64(meta.delta_offset_end());
    }
    w.Key("metadata_size_hint");
    w.Int64(static_cast<int64_t>(meta.metadata_size_hint));
    w.EndObject();
}

void partition_manifest::serialize_removed_segment_meta(
  const lw_segment_meta& meta, serialization_cursor_ptr cursor) const {
    // Here we are serializing all fields stored in 'lw_segment_meta'.
    // The remaining fields are also added but they values are not
    // significant.
    vassert(
      meta.segment_term != model::term_id{},
      "Term id is not initialized, base offset {}",
      meta.base_offset);
    auto& w = cursor->writer;
    auto sn = generate_local_segment_name(meta.base_offset, meta.segment_term);
    w.Key(sn());
    w.StartObject();
    w.Key("size_bytes");
    w.Int64(static_cast<int64_t>(meta.size_bytes));
    w.Key("committed_offset");
    w.Int64(meta.committed_offset());
    w.Key("base_offset");
    w.Int64(meta.base_offset());
    if (meta.ntp_revision != _rev) {
        vassert(
          meta.ntp_revision != model::initial_revision_id(),
          "ntp {}: missing ntp_revision for segment {} in the manifest",
          _ntp,
          sn);
        w.Key("ntp_revision");
        w.Int64(meta.ntp_revision());
    }
    if (meta.archiver_term != model::term_id::min()) {
        w.Key("archiver_term");
        w.Int64(meta.archiver_term());
    }
    w.Key("segment_term");
    w.Int64(meta.segment_term());

    w.Key("sname_format");
    using name_format_type = std::underlying_type<segment_name_format>::type;
    w.Int64(static_cast<name_format_type>(meta.sname_format));

    w.EndObject();
}

void partition_manifest::serialize_segments(
  partition_manifest::serialization_cursor_ptr cursor) const {
    vassert(cursor->segments_done == false, "Segments are already serialized");
    if (_segments.empty()) {
        cursor->next_offset = model::offset{};
        cursor->segments_done = true;
        return;
    }
    auto& w = cursor->writer;
    if (cursor->next_offset == model::offset{} && !_segments.empty()) {
        // The method is called first time
        w.Key("segments");
        w.StartObject();
        cursor->next_offset = _segments.begin()->base_offset;
    }
    if (!_segments.empty()) {
        auto it = _segments.lower_bound(cursor->next_offset);
        for (; it != _segments.end(); ++it) {
            serialize_segment_meta(*it, cursor);
            cursor->segments_serialized++;
            if (cursor->segments_serialized >= cursor->max_segments_per_call) {
                cursor->segments_serialized = 0;
                ++it;
                break;
            }
        }
        if (it == _segments.end()) {
            cursor->next_offset = _last_offset;
        } else {
            // We hit the limit on number of serialized segment
            // metadata objects and 'it' points to the first segment
            // which is not serialized yet.
            cursor->next_offset = it->base_offset;
        }
    }
    if (cursor->next_offset == _last_offset && !_segments.empty()) {
        // next_offset will overshoot by one
        w.EndObject();
        cursor->segments_done = true;
    }
}

void partition_manifest::serialize_replaced(
  partition_manifest::serialization_cursor_ptr cursor) const {
    vassert(
      cursor->replaced_done == false,
      "Replaced segments are already serialized");
    auto& w = cursor->writer;
    if (!_replaced.empty()) {
        w.Key("replaced");
        w.StartObject();
        for (const auto& meta : _replaced) {
            serialize_removed_segment_meta(meta, cursor);
        }
        w.EndObject();
    }
    cursor->replaced_done = true;
}
void partition_manifest::serialize_spillover(
  partition_manifest::serialization_cursor_ptr cursor) const {
    auto& w = cursor->writer;
    if (_spillover_manifests.empty()) {
        cursor->next_spill_offset = model::offset{};
        cursor->spillover_done = true;
        return;
    }

    if (
      cursor->next_spill_offset == model::offset{}
      && !_spillover_manifests.empty()) {
        // The method is called first time
        w.Key("spillover");
        w.StartArray();
        cursor->next_spill_offset = _spillover_manifests.begin()->base_offset;
    }

    if (!_spillover_manifests.empty()) {
        auto it = _spillover_manifests.lower_bound(cursor->next_spill_offset);
        for (; it != _spillover_manifests.end(); ++it) {
            serialize_spillover_manifest_meta(*it, cursor);
            cursor->spills_serialized++;
            if (cursor->spills_serialized >= cursor->max_segments_per_call) {
                cursor->spills_serialized = 0;
                ++it;
                break;
            }
        }
        if (it == _spillover_manifests.end()) {
            cursor->next_spill_offset = _last_offset;
        } else {
            cursor->next_spill_offset = it->base_offset;
        }
    }
    if (
      cursor->next_spill_offset == _last_offset
      && !_spillover_manifests.empty()) {
        // next_offset will overshoot by one
        w.EndArray();
        cursor->spillover_done = true;
    }
}

void partition_manifest::serialize_end(serialization_cursor_ptr cursor) const {
    vassert(
      cursor->epilogue_done == false, "Manifest is already fully serialized");
    cursor->writer.EndObject();
    cursor->epilogue_done = true;
}

partition_manifest::const_iterator
partition_manifest::segment_containing(model::offset o) const {
    if (o > _last_offset || _segments.empty()) {
        return end();
    }

    auto it = _segments.upper_bound(o);
    if (it == _segments.begin()) {
        return end();
    }

    it = _segments.prev(it);
    if (it->base_offset <= o && it->committed_offset >= o) {
        return it;
    }

    // o lies in a gap in manifest
    return end();
}

/**
 * Look up the first segment containing timestamps >= the query timestamp.
 *
 * We do not keep a direct index of timestamp to segment, because
 * it is relatively rarely used, and would have a high memory cost.
 * In the absence of a random-access container of timestamps,
 * we may use offset as an indirect way of sampling the segments
 * for a bisection search, and use the total partition min+max
 * timestamps to establish a good initial guess: for a partition
 * with evenly spaced messages over time, this will result in very
 * fast lookup.
 * The procedure is:
 * 1. Make an initial guess by interpolating the timestamp between
 *    the partitions min+max timestamp and mapping that to an offset
 * 2. Convert offset guess into segment guess.  This is not a strictly
 *    correct offset lookup, we just want something close.
 * 3. Do linear search backward or forward from the location
 *    we probed, to find the right segment.
 */
std::optional<partition_manifest::segment_meta>
partition_manifest::timequery(model::timestamp t) const {
    if (_segments.empty()) {
        return std::nullopt;
    }

    auto first_segment = *_segments.begin();
    auto base_t = first_segment.base_timestamp;
    auto max_t = _segments.last_segment().value().max_timestamp;
    auto base_offset = first_segment.base_offset;
    auto max_offset = _segments.last_segment().value().committed_offset;

    // Fast handling of bounds/edge cases to simplify subsequent
    // arithmetic steps
    if (t < base_t) {
        return first_segment;
    } else if (t > max_t) {
        return std::nullopt;
    } else if (max_t == base_t) {
        // This is plausible in the case of a single segment with
        // a single batch using LogAppendTime.
        return first_segment;
    }

    // Single-offset case should have hit max_t==base_t above
    vassert(
      max_offset > base_offset,
      "Unexpected offsets {} {} (times {} {})",
      base_offset,
      max_offset,
      base_t,
      max_t);

    auto base_timestamp = _segments.get_base_timestamp_column().begin();
    auto max_timestamp = _segments.get_max_timestamp_column().begin();
    size_t target_ix = 0;
    while (!base_timestamp.is_end()) {
        if (*max_timestamp >= t.value() || *base_timestamp > t.value()) {
            target_ix = base_timestamp.index();
            break;
        }
        ++base_timestamp;
        ++max_timestamp;
    }
    return *_segments.at_index(target_ix);
}

// this class is a serde-enabled version of partition_manifest. it's needed
// because segment_meta_cstore is not copyable, and moving it would empty out
// partition_manifest
struct partition_manifest_serde
  : public serde::envelope<
      partition_manifest_serde,
      serde::version<to_underlying(manifest_version::v4)>,
      serde::compat_version<0>> {
    model::ntp _ntp;
    model::initial_revision_id _rev;

    // _segments_serialized is already serialized via serde into a iobuf
    // this is done because segment_meta_cstore is not a serde::envelope
    iobuf _segments_serialized;

    partition_manifest::replaced_segments_list _replaced;
    model::offset _last_offset;
    model::offset _start_offset;
    model::offset _last_uploaded_compacted_offset;
    model::offset _insync_offset;
    size_t _cloud_log_size_bytes;
    model::offset _archive_start_offset;
    model::offset_delta _archive_start_offset_delta;
    model::offset _archive_clean_offset;
    kafka::offset _start_kafka_offset;
    size_t archive_size_bytes;
    iobuf _spillover_manifests_serialized;
    model::timestamp _last_partition_scrub;
    std::optional<model::offset> _last_scrubbed_offset;
    model::producer_id _highest_producer_id;
    model::offset _applied_offset;

    auto serde_fields() {
        return std::tie(
          _ntp,
          _rev,
          _segments_serialized,
          _replaced,
          _last_offset,
          _start_offset,
          _last_uploaded_compacted_offset,
          _insync_offset,
          _cloud_log_size_bytes,
          _archive_start_offset,
          _archive_start_offset_delta,
          _archive_clean_offset,
          _start_kafka_offset,
          archive_size_bytes,
          _spillover_manifests_serialized,
          _last_partition_scrub,
          _last_scrubbed_offset,
          _highest_producer_id,
          _applied_offset);
    }
};

static_assert(
  std::tuple_size_v<
    decltype(std::declval<const partition_manifest&>().serde_fields())>
    == std::tuple_size_v<
      decltype(std::declval<partition_manifest&>().serde_fields())>,
  "ensure that serde_fields() and serde_fields() const capture the same "
  "fields");
static_assert(
  std::tuple_size_v<decltype(serde::envelope_to_tuple(
      std::declval<partition_manifest_serde&>()))>
    == std::tuple_size_v<
      decltype(std::declval<partition_manifest&>().serde_fields())>,
  "partition_manifest_serde and partition_manifest must have the same number "
  "of fields, for serialization purposes");

// construct partition_manifest_serde while keeping
// std::is_aggregate<partition_manifest_serde> true
static auto partition_manifest_serde_from_partition_manifest(
  const partition_manifest& m) -> partition_manifest_serde {
    partition_manifest_serde tmp{};
    // copy every field that is not segment_meta_cstore in
    // partition_manifest_serde, and uses to_iobuf for segment_meta_cstore

    []<typename DT, typename ST, size_t... Is>(
      DT dest_tuple, ST src_tuple, std::index_sequence<Is...>) {
        (([&]<typename Src>(auto& dest, const Src& src) {
             if constexpr (std::is_same_v<Src, segment_meta_cstore>) {
                 dest = src.to_iobuf();
             } else if constexpr (reflection::is_fragmented_vector<Src>) {
                 dest = src.copy();
             } else {
                 dest = src;
             }
         }(std::get<Is>(dest_tuple), std::get<Is>(src_tuple))),
         ...);
    }(serde::envelope_to_tuple(tmp),
      m.serde_fields(),
      std::make_index_sequence<
        std::tuple_size_v<decltype(m.serde_fields())>>());
    return tmp;
}

// almost the reverse of partition_manifest_serde_to_partition_manifest
static void partition_manifest_serde_to_partition_manifest(
  partition_manifest_serde src, partition_manifest& dest) {
    []<typename DT, typename ST, size_t... Is>(
      DT dest_tuple, ST src_tuple, std::index_sequence<Is...>) {
        (([&]<typename Dest>(Dest& dest, auto& src) {
             if constexpr (std::is_same_v<Dest, segment_meta_cstore>) {
                 dest.from_iobuf(std::move(src));
             } else {
                 dest = std::move(src);
             }
         }(std::get<Is>(dest_tuple), std::get<Is>(src_tuple))),
         ...);
    }(dest.serde_fields(),
      serde::envelope_to_tuple(src),
      std::make_index_sequence<
        std::tuple_size_v<decltype(dest.serde_fields())>>());
}

iobuf partition_manifest::to_iobuf() const {
    return serde::to_iobuf(
      partition_manifest_serde_from_partition_manifest(*this));
}

void partition_manifest::from_iobuf(iobuf in) {
    partition_manifest_serde_to_partition_manifest(
      serde::from_iobuf<partition_manifest_serde>(std::move(in)), *this);

    // `_start_offset` can be modified in the above so invalidate
    // the dependent cached value.
    _cached_start_kafka_offset_local = std::nullopt;
}

void partition_manifest::process_anomalies(
  model::timestamp scrub_timestamp,
  std::optional<model::offset> last_scrubbed_offset,
  scrub_status status,
  anomalies detected) {
    // Firstly, update the in memory list of anomalies.
    // If the entires log was scrubbed, overwrite the old anomalies,
    // otherwise append to them.
    if (status == scrub_status::full) {
        _detected_anomalies = std::move(detected);
    } else if (status == scrub_status::partial) {
        _detected_anomalies += std::move(detected);
    }

    // Secondly, remove any anomalies that are not present in manifest.
    // Such cases can occur when scrubbing races with manifest uploads.
    if (
      _detected_anomalies.missing_partition_manifest
      && _cloud_log_size_bytes == 0) {
        _detected_anomalies.missing_partition_manifest = false;
    }

    auto& missing_spills = _detected_anomalies.missing_spillover_manifests;

    const auto archive_start_offset = get_archive_start_offset();
    erase_if(
      missing_spills, [this, &archive_start_offset](const auto& spill_comp) {
          // Remove the missing spill if lies below the start offset of the
          // archive or if it doesn't match with any of the entries in the
          // manifest.
          return spill_comp.last < archive_start_offset
                 || _spillover_manifests.find(spill_comp.base)
                      == _spillover_manifests.end();
      });

    auto first_kafka_offset = full_log_start_kafka_offset();
    auto& missing_segs = _detected_anomalies.missing_segments;
    erase_if(missing_segs, [this, &first_kafka_offset](const auto& meta) {
        if (meta.next_kafka_offset() <= first_kafka_offset) {
            return true;
        }

        if (meta.committed_offset >= get_start_offset()) {
            // The segment might have been missing because it was merged with
            // something else. If the offset range doesn't match a segment
            // exactly, discard the anomaly. Only segments from the STM manifest
            // may be merged/reuploaded.
            return !segment_with_offset_range_exists(
              meta.base_offset, meta.committed_offset);
        } else {
            // Segment belongs to the archive. No reuploads are done here.
            return false;
        }
    });

    auto& segment_meta_anomalies
      = _detected_anomalies.segment_metadata_anomalies;
    erase_if(
      segment_meta_anomalies,
      [this, &first_kafka_offset](const auto& anomaly_meta) {
          if (anomaly_meta.at.next_kafka_offset() <= first_kafka_offset) {
              return true;
          }

          if (anomaly_meta.at.committed_offset >= get_start_offset()) {
              // Similarly to the missing segment case, if the boundaries of the
              // segment where the anomaly was detected changed, drop it.
              return !segment_with_offset_range_exists(
                anomaly_meta.at.base_offset, anomaly_meta.at.committed_offset);
          } else {
              return false;
          }
      });

    _last_partition_scrub = scrub_timestamp;
    _last_scrubbed_offset = last_scrubbed_offset;

    if (!_last_scrubbed_offset) {
        _detected_anomalies.last_complete_scrub = scrub_timestamp;
    }

    vlog(
      cst_log.debug,
      "[{}] Anomalies processed: {{ detected: {}, last_partition_scrub: {}, "
      "last_scrubbed_offset: {} }}",
      _ntp,
      _detected_anomalies,
      _last_partition_scrub,
      _last_scrubbed_offset);
}

} // namespace cloud_storage
