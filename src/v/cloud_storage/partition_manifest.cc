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

#include "bytes/iobuf_istreambuf.h"
#include "bytes/iobuf_ostreambuf.h"
#include "bytes/iostream.h"
#include "cloud_storage/logger.h"
#include "cloud_storage/types.h"
#include "hashing/xx.h"
#include "json/document.h"
#include "json/istreamwrapper.h"
#include "json/ostreamwrapper.h"
#include "json/writer.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "ssx/sformat.h"
#include "storage/fs_utils.h"
#include "utils/to_string.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/later.hh>

#include <fmt/ostream.h>
#include <rapidjson/error/en.h>

#include <algorithm>
#include <charconv>
#include <iterator>
#include <memory>
#include <optional>
#include <stdexcept>
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
    auto format(segment_meta const& m, FormatContext& ctx) {
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

static bool parse_partition_and_revision(
  std::string_view s, partition_manifest_path_components& comp) {
    auto pos = s.find('_');
    if (pos == std::string_view::npos) {
        // Invalid segment file name
        return false;
    }
    uint64_t res = 0;
    // parse first component
    auto sv = s.substr(0, pos);
    auto e = std::from_chars(sv.data(), sv.data() + sv.size(), res);
    if (e.ec != std::errc()) {
        return false;
    }
    comp._part = model::partition_id(res);
    // parse second component
    sv = s.substr(pos + 1);
    e = std::from_chars(sv.data(), sv.data() + sv.size(), res);
    if (e.ec != std::errc()) {
        return false;
    }
    comp._rev = model::initial_revision_id(res);
    return true;
}

std::optional<partition_manifest_path_components>
get_partition_manifest_path_components(const std::filesystem::path& path) {
    // example: b0000000/meta/kafka/redpanda-test/4_2/manifest.json
    enum {
        ix_prefix,
        ix_meta,
        ix_namespace,
        ix_topic,
        ix_part_rev,
        ix_file_name,
        total_components
    };
    partition_manifest_path_components res;
    res._origin = path;
    int ix = 0;
    for (const auto& c : path) {
        ss::sstring p = c.string();
        switch (ix++) {
        case ix_prefix:
            break;
        case ix_namespace:
            res._ns = model::ns(std::move(p));
            break;
        case ix_topic:
            res._topic = model::topic(std::move(p));
            break;
        case ix_part_rev:
            if (!parse_partition_and_revision(p, res)) {
                return std::nullopt;
            }
            break;
        case ix_file_name:
            if (p != "manifest.json") {
                return std::nullopt;
            }
            break;
        }
    }
    if (ix == total_components) {
        return res;
    }
    return std::nullopt;
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

remote_segment_path generate_remote_segment_path(
  const model::ntp& ntp,
  model::initial_revision_id rev_id,
  const segment_name& name,
  model::term_id archiver_term) {
    vassert(
      rev_id != model::initial_revision_id(),
      "ntp {}: ntp revision must be known for segment {}",
      ntp,
      name);

    auto path = ssx::sformat("{}_{}/{}", ntp.path(), rev_id(), name());
    uint32_t hash = xxhash_32(path.data(), path.size());
    if (archiver_term != model::term_id{}) {
        return remote_segment_path(
          fmt::format("{:08x}/{}.{}", hash, path, archiver_term()));
    } else {
        return remote_segment_path(fmt::format("{:08x}/{}", hash, path));
    }
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
  , _segments(util::mem_tracked::map<absl::btree_map, key, value>(_mem_tracker))
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
  , _segments(util::mem_tracked::map<absl::btree_map, key, value>(_mem_tracker))
  , _last_offset(0) {}

// NOTE: the methods that generate remote paths use the xxhash function
// to randomize the prefix. S3 groups the objects into chunks based on
// these prefixes. It also applies rate limit to chunks so if all segments
// and manifests will have the same prefix we will be able to do around
// 3000-5000 req/sec. AWS doc mentions that having only two prefix
// characters should be enough for most workloads
// (https://aws.amazon.com/blogs/aws/amazon-s3-performance-tips-tricks-seattle-hiring-event/)
// We're using eight because it's free and because AWS S3 is not the only
// backend and other S3 API implementations might benefit from that.

remote_manifest_path generate_partition_manifest_path(
  const model::ntp& ntp, model::initial_revision_id rev) {
    // NOTE: the idea here is to split all possible hash values into
    // 16 bins. Every bin should have lowest 28-bits set to 0.
    // As result, for segment names all prefixes are possible, but
    // for manifests, only 0x00000000, 0x10000000, ... 0xf0000000
    // are used. This will allow us to quickly find all manifests
    // that S3 bucket contains.
    constexpr uint32_t bitmask = 0xF0000000;
    auto path = ssx::sformat("{}_{}", ntp.path(), rev());
    uint32_t hash = bitmask & xxhash_32(path.data(), path.size());
    return remote_manifest_path(
      fmt::format("{:08x}/meta/{}_{}/manifest.json", hash, ntp.path(), rev()));
}

remote_manifest_path partition_manifest::get_manifest_path() const {
    return generate_partition_manifest_path(_ntp, _rev);
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
    return _start_kafka_offset;
}

std::optional<kafka::offset>
partition_manifest::get_start_kafka_offset() const {
    if (_start_kafka_offset != kafka::offset{}) {
        return _start_kafka_offset;
    }
    if (_start_offset != model::offset{}) {
        auto iter = _segments.find(_start_offset);
        if (iter != _segments.end()) {
            auto delta = iter->second.delta_offset;
            return _start_offset - delta;
        } else {
            // If start offset points outside a segment, then we cannot
            // translate it.  If there are any segments ahead of it, then
            // those may be considered the start of the remote log.
            if (
              !_segments.empty()
              && _segments.begin()->second.base_offset >= _start_offset) {
                const auto& seg = _segments.begin()->second;
                return seg.base_offset - seg.delta_offset;
            } else {
                return std::nullopt;
            }
        }
    }
    return std::nullopt;
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
    // We need to find first element which has greater kafka offset than
    // the target and step back. It is possible to have a segment that
    // doesn't have data batches. This scan has to skip segments like that.
    while (it != end()) {
        if (it->second.base_kafka_offset() > o) {
            // The beginning of the manifest already has a base offset that
            // doesn't satisfy the query.
            if (it == begin()) {
                return end();
            }
            // On the first segment we see with a base kafka offset higher than
            // 'o', return its previous segment.
            return std::prev(it);
        }
        it = std::next(it);
    }
    // All segments had base kafka offsets lower than 'o'.
    auto back = std::prev(it);
    if (back->second.delta_offset_end != model::offset_delta{}) {
        // If 'prev' points to the last segment, it's not guaranteed that
        // the segment contains the required kafka offset. We need an extra
        // check using delta_offset_end. If the field is not set then we
        // will return the last segment. This is OK since delta_offset_end
        // will always be set for new segments.
        if (back->second.next_kafka_offset() <= o) {
            return end();
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

remote_segment_path
partition_manifest::generate_segment_path(const segment_meta& meta) const {
    auto name = generate_remote_segment_name(meta);
    return cloud_storage::generate_remote_segment_path(
      _ntp, meta.ntp_revision, name, meta.archiver_term);
}

remote_segment_path
partition_manifest::generate_segment_path(const lw_segment_meta& meta) const {
    return generate_segment_path(lw_segment_meta::convert(meta));
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
        // Use new stlyle format ".../base-committed-term-size-v1.log"
        return segment_name(ssx::sformat(
          "{}-{}-{}-{}-v1.log",
          val.base_offset(),
          val.committed_offset(),
          val.size_bytes,
          val.segment_term()));
    }
    __builtin_unreachable();
}

remote_segment_path partition_manifest::generate_remote_segment_path(
  const model::ntp& ntp, const partition_manifest::value& val) {
    auto name = generate_remote_segment_name(val);
    return cloud_storage::generate_remote_segment_path(
      ntp, val.ntp_revision, name, val.archiver_term);
}

local_segment_path partition_manifest::generate_local_segment_path(
  const model::ntp& ntp, const partition_manifest::value& val) {
    auto name = cloud_storage::generate_local_segment_name(
      val.base_offset, val.segment_term);
    return local_segment_path(
      fmt::format("{}_{}/{}", ntp.path(), val.ntp_revision, name()));
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
    return _segments.rbegin()->second;
}

bool partition_manifest::empty() const { return _segments.size() == 0; }

size_t partition_manifest::size() const { return _segments.size(); }

size_t partition_manifest::segments_metadata_bytes() const {
    return _mem_tracker->consumption();
}

uint64_t partition_manifest::compute_cloud_log_size() const {
    auto start_iter = find(_start_offset);

    // No addresable segments
    if (start_iter == end()) {
        return 0;
    }

    return std::transform_reduce(
      start_iter, end(), uint64_t{0}, std::plus{}, [](const auto& seg) {
          return seg.second.size_bytes;
      });
}

uint64_t partition_manifest::cloud_log_size() const {
    return _cloud_log_size_bytes;
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
}

void partition_manifest::set_archive_clean_offset(
  model::offset start_rp_offset) {
    if (_archive_start_offset < start_rp_offset) {
        vlog(
          cst_log.error,
          "{} Requested to advance archive_clean_offset to {} which is greater "
          "than the current archive_start_offset {}. The offset won't be "
          "changed.",
          _ntp,
          start_rp_offset,
          _archive_start_offset);
        return;
    }
    if (_archive_clean_offset < start_rp_offset) {
        _archive_clean_offset = start_rp_offset;
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
}

bool partition_manifest::advance_start_kafka_offset(
  kafka::offset new_start_offset) {
    auto prev_kso = get_start_kafka_offset();
    if (
      _archive_start_offset != model::offset{} && new_start_offset < prev_kso) {
        // Special case. If the archive is enabled and contains some data
        // the offset could be placed anywhere in the archive. The archive
        // housekeeping should take this into account.
        if (new_start_offset < get_archive_start_kafka_offset()) {
            return false;
        }
        _start_kafka_offset = std::max(new_start_offset, _start_kafka_offset);
        return true;
    }
    auto it = segment_containing(new_start_offset);
    if (it == end()) {
        vlog(
          cst_log.debug,
          "{} start kafka offset not moved to {}, no such segment",
          _ntp,
          _start_kafka_offset);
        return false;
    } else if (it == begin()) {
        _start_kafka_offset = std::max(new_start_offset, _start_kafka_offset);
        vlog(
          cst_log.debug,
          "{} start kafka offset moved to {}, start offset stayed at {}",
          _ntp,
          _start_kafka_offset,
          _start_offset);
        return true;
    }
    auto moved = advance_start_offset(it->second.base_offset);
    // 'advance_start_offset' resets _start_kafka_offset value
    // so it's important to set it after this call.
    _start_kafka_offset = std::max(new_start_offset, _start_kafka_offset);
    vlog(
      cst_log.info,
      "{} start kafka offset moved to {}, start offset {} {}",
      _ntp,
      _start_kafka_offset,
      moved ? "moved to" : "stayed at",
      _start_offset);
    return true;
}

bool partition_manifest::advance_start_offset(model::offset new_start_offset) {
    const auto previous_start_offset = _start_offset;

    if (new_start_offset > _start_offset && !_segments.empty()) {
        auto it = _segments.upper_bound(new_start_offset);
        if (it == _segments.begin()) {
            return false;
        }

        auto new_head_segment = std::prev(it);
        if (new_head_segment->second.committed_offset < new_start_offset) {
            new_head_segment = std::next(new_head_segment);
        }

        model::offset advanced_start_offset
          = new_head_segment == end() ? new_start_offset
                                      : new_head_segment->second.base_offset;

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

        _start_offset = advanced_start_offset;

        auto previous_head_segment = segment_containing(previous_start_offset);
        if (previous_head_segment == end()) {
            // This branch should never be taken. It indicates that the
            // in-memory manifest may be is in some sort of inconsistent state.
            vlog(
              cst_log.error,
              "Previous start offset is not within segment in "
              "manifest for {}: previous_start_offset={}",
              _ntp,
              previous_start_offset);
            previous_head_segment = _segments.begin();
        }

        // Note that we start subtracting from the cloud log size from the
        // previous head segments. This is required in order to account for
        // the case when two `truncate` commands are applied sequentially,
        // without a `cleanup_metadata` command in between to trim the list of
        // segments.
        for (auto it = previous_head_segment; it != new_head_segment; ++it) {
            subtract_from_cloud_log_size(it->second.size_bytes);
        }

        // Reset start kafka offset so it will be aligned by segment boundary
        _start_kafka_offset = kafka::offset{};

        return true;
    }
    return false;
}

std::vector<partition_manifest::lw_segment_meta>
partition_manifest::lw_replaced_segments() const {
    return _replaced;
}

std::vector<segment_meta> partition_manifest::replaced_segments() const {
    std::vector<segment_meta> res;
    res.reserve(_replaced.size());
    for (const auto& s : _replaced) {
        res.push_back(lw_segment_meta::convert(s));
    }
    return res;
}

size_t partition_manifest::replaced_segments_count() const {
    return _replaced.size();
}

size_t partition_manifest::move_aligned_offset_range(
  model::offset begin_inclusive,
  model::offset end_inclusive,
  const segment_meta& replacing_segment) {
    size_t total_replaced_size = 0;
    auto replacing_path = generate_remote_segment_name(replacing_segment);
    auto it = _segments.lower_bound(begin_inclusive);
    while (it != _segments.end()
           // The segment is considered replaced only if all its
           // offsets are covered by new segment's offset range
           && it->second.base_offset >= begin_inclusive
           && it->second.committed_offset <= end_inclusive) {
        if (generate_remote_segment_name(it->second) == replacing_path) {
            // The replacing segment shouldn't be exactly the same as the
            // one that we already have in the manifest. Attempt to re-add
            // same segment twice leads to data loss.
            vlog(
              cst_log.warn,
              "{} segment is already added {}",
              _ntp,
              replacing_segment);
            break;
        }
        _replaced.push_back(lw_segment_meta::convert(it->second));
        total_replaced_size += it->second.size_bytes;
        it = _segments.erase(it);
    }

    return total_replaced_size;
}

bool partition_manifest::add(
  const partition_manifest::key& key, const segment_meta& meta) {
    if (_start_offset == model::offset{} && _segments.empty()) {
        // This can happen if this is the first time we add something
        // to the manifest or if all data was removed previously.
        _start_offset = meta.base_offset;
    }
    const auto total_replaced_size = move_aligned_offset_range(
      meta.base_offset, meta.committed_offset, meta);
    auto [it, ok] = _segments.insert(std::make_pair(key, meta));
    if (ok && it->second.ntp_revision == model::initial_revision_id{}) {
        it->second.ntp_revision = _rev;
    }
    _last_offset = std::max(meta.committed_offset, _last_offset);
    if (meta.is_compacted) {
        _last_uploaded_compacted_offset = std::max(
          meta.committed_offset, _last_uploaded_compacted_offset);
    }

    if (ok) {
        // If the segment does not replace the one that we have we will
        // fail to insert it into the map. In this case we shouldn't
        // modify the total cloud size.
        subtract_from_cloud_log_size(total_replaced_size);
        _cloud_log_size_bytes += meta.size_bytes;
    }

    return ok;
}

bool partition_manifest::add(
  const segment_name& name, const segment_meta& meta) {
    if (meta.segment_term != model::term_id{}) {
        return add(meta.base_offset, meta);
    }
    auto maybe_key = parse_segment_name(name);
    if (!maybe_key) {
        throw std::runtime_error(
          fmt_with_ctx(fmt::format, "can't parse segment name \"{}\"", name));
    }
    auto m = meta;
    m.segment_term = maybe_key->term;
    return add(meta.base_offset, m);
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
    for (auto it : _segments) {
        if (it.second.committed_offset < _start_offset) {
            removed.add(it.first, it.second);
        }
    }
    for (auto s : removed._segments) {
        _segments.erase(s.first);
    }
    if (_segments.empty()) {
        // start offset only makes sense if we have segments
        _start_offset = model::offset{};
        // NOTE: _last_offset should not be reset
    }
    return removed;
}

const partition_manifest::segment_meta*
partition_manifest::get(const partition_manifest::key& key) const {
    auto it = _segments.find(key);
    if (it == _segments.end()) {
        return nullptr;
    }
    return &it->second;
}

const partition_manifest::segment_meta*
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
    if (it == _segments.end() || it->second.base_offset != o) {
        return end();
    }
    return it;
}

std::insert_iterator<partition_manifest::segment_map>
partition_manifest::get_insert_iterator() {
    return std::inserter(_segments, _segments.begin());
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
        case state::expect_manifest_key:
        case state::expect_manifest_value:
        case state::expect_segment_path:
        case state::expect_segment_meta_key:
        case state::expect_segment_meta_value:
        case state::expect_replaced_path:
        case state::expect_replaced_meta_key:
        case state::expect_replaced_meta_value:
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
            } else {
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
        case state::expect_manifest_start:
        case state::expect_manifest_value:
        case state::expect_segments_start:
        case state::expect_replaced_start:
        case state::expect_segment_meta_start:
        case state::expect_segment_meta_value:
        case state::expect_replaced_meta_start:
        case state::expect_replaced_meta_value:
        case state::terminal_state:
            return false;
        }
    }

    bool String(const char* str, rapidjson::SizeType length, bool /*copy*/) {
        std::string_view sv(str, length);
        switch (_state) {
        case state::expect_manifest_value:
            if ("namespace" == _manifest_key) {
                _namespace = model::ns(ss::sstring(sv));
            } else if ("topic" == _manifest_key) {
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
            return false;
        }
    }

    bool Uint(unsigned u) { return Uint64(u); }

    bool Uint64(uint64_t u) {
        switch (_state) {
        case state::expect_manifest_value:
            if ("version" == _manifest_key) {
                _version = u;
            } else if ("partition" == _manifest_key) {
                _partition_id = model::partition_id(u);
            } else if ("revision" == _manifest_key) {
                _revision_id = model::initial_revision_id(u);
            } else if ("last_offset" == _manifest_key) {
                _last_offset = model::offset(u);
            } else if ("start_offset" == _manifest_key) {
                _start_offset = model::offset(u);
            } else if ("last_uploaded_compacted_offset" == _manifest_key) {
                _last_uploaded_compacted_offset = model::offset(u);
            } else if ("insync_offset" == _manifest_key) {
                _insync_offset = model::offset(u);
            } else if ("cloud_log_size_bytes" == _manifest_key) {
                _cloud_log_size_bytes = u;
            } else if ("archive_start_offset" == _manifest_key) {
                _archive_start_offset = model::offset(u);
            } else if ("archive_start_offset_delta" == _manifest_key) {
                _archive_start_offset_delta = model::offset_delta(u);
            } else if ("archive_clean_offset" == _manifest_key) {
                _archive_clean_offset = model::offset(u);
            } else {
                return false;
            }
            _state = state::expect_manifest_key;
            return true;
        case state::expect_replaced_meta_value:
        case state::expect_segment_meta_value:
            if ("size_bytes" == _segment_meta_key) {
                _size_bytes = static_cast<size_t>(u);
            } else if ("base_offset" == _segment_meta_key) {
                _base_offset = model::offset(u);
            } else if ("committed_offset" == _segment_meta_key) {
                _committed_offset = model::offset(u);
            } else if ("base_timestamp" == _segment_meta_key) {
                _base_timestamp = model::timestamp(u);
            } else if ("max_timestamp" == _segment_meta_key) {
                _max_timestamp = model::timestamp(u);
            } else if ("delta_offset" == _segment_meta_key) {
                _delta_offset = model::offset_delta(u);
            } else if ("ntp_revision" == _segment_meta_key) {
                _ntp_revision = model::initial_revision_id(u);
            } else if ("archiver_term" == _segment_meta_key) {
                _archiver_term = model::term_id(u);
            } else if ("segment_term" == _segment_meta_key) {
                _segment_term = model::term_id(u);
            } else if ("delta_offset_end" == _segment_meta_key) {
                _delta_offset_end = model::offset_delta(u);
            } else if ("sname_format" == _segment_meta_key) {
                _meta_sname_format = segment_name_format(u);
            } else if ("metadata_size_hint") {
                _metadata_size_hint = static_cast<size_t>(u);
            }
            if (_state == state::expect_segment_meta_value) {
                _state = state::expect_segment_meta_key;
            } else {
                _state = state::expect_replaced_meta_key;
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
                    _segments = std::make_unique<segment_map>(
                      util::mem_tracked::map<
                        absl::btree_map,
                        partition_manifest::key,
                        partition_manifest::value>(_manifest_mem_tracker));
                }
                _segments->insert(
                  std::make_pair(_segment_key.base_offset, _meta));
                _state = state::expect_segment_path;
            } else {
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
        case state::terminal_state:
            return false;
        }
    }

    bool Bool(bool b) {
        switch (_state) {
        case state::expect_segment_meta_value:
            if ("is_compacted" == _segment_meta_key) {
                _is_compacted = b;
                _state = state::expect_segment_meta_key;
                return true;
            }
            return false;
        case state::expect_replaced_meta_value:
            if ("is_compacted" == _segment_meta_key) {
                _is_compacted = b;
                _state = state::expect_replaced_meta_key;
                return true;
            }
            return false;
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
        case state::terminal_state:
            return false;
        }
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

ss::future<> partition_manifest::update(ss::input_stream<char> is) {
    iobuf result;
    auto os = make_iobuf_ref_output_stream(result);
    co_await ss::copy(is, os);
    iobuf_istreambuf ibuf(result);
    std::istream stream(&ibuf);
    json::IStreamWrapper wrapper(stream);
    rapidjson::Reader reader;
    partition_manifest_handler handler(_mem_tracker);

    if (reader.Parse(wrapper, handler)) {
        partition_manifest::update(std::move(handler));
    } else {
        rapidjson::ParseErrorCode e = reader.GetParseErrorCode();
        size_t o = reader.GetErrorOffset();
        vlog(
          cst_log.debug, "Failed to parse manifest: {}", result.hexdump(2048));
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Failed to parse partition manifest {}: {} at offset {}",
          get_manifest_path(),
          rapidjson::GetParseError_En(e),
          o));
    }
    co_return;
}

void partition_manifest::update(partition_manifest_handler&& handler) {
    if (handler._version != static_cast<int>(manifest_version::v1)) {
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
        _start_offset = handler._start_offset.value();
    } else {
        _start_offset = {};
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
            _start_offset = _segments.begin()->second.base_offset;
        }
    }
    if (handler._replaced) {
        _replaced = std::move(*handler._replaced);
    }

    if (handler._cloud_log_size_bytes) {
        _cloud_log_size_bytes = handler._cloud_log_size_bytes.value();
    } else {
        _cloud_log_size_bytes = compute_cloud_log_size();
    }
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
    // Flags that indicate serialization progress
    bool prologue_done{false};
    bool segments_done{false};
    bool replaced_done{false};
    bool epilogue_done{false};
};

ss::future<serialized_json_stream> partition_manifest::serialize() const {
    auto iso = _insync_offset;
    iobuf serialized;
    iobuf_ostreambuf obuf(serialized);
    std::ostream os(&obuf);
    serialization_cursor_ptr c = make_cursor(os);
    serialize_begin(c);
    while (!c->segments_done) {
        serialize_segments(c);
        co_await ss::maybe_yield();
        if (iso != _insync_offset) {
            throw std::runtime_error(fmt_with_ctx(
              fmt::format,
              "Manifest changed duing serialization, in sync offset moved from "
              "{} to {}",
              iso,
              _insync_offset));
        }
    }
    serialize_replaced(c);
    serialize_end(c);
    if (!os.good()) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "could not serialize partition manifest {}",
          get_manifest_path()));
    }
    size_t size_bytes = serialized.size_bytes();
    co_return serialized_json_stream{
      .stream = make_iobuf_input_stream(std::move(serialized)),
      .size_bytes = size_bytes};
}

void partition_manifest::serialize(std::ostream& out) const {
    serialization_cursor_ptr c = make_cursor(out);
    serialize_begin(c);
    while (!c->segments_done) {
        serialize_segments(c);
    }
    serialize_replaced(c);
    serialize_end(c);
}

ss::future<>
partition_manifest::serialize(ss::output_stream<char>& output) const {
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
        cursor->next_offset = _segments.begin()->second.base_offset;
    }
    if (!_segments.empty()) {
        auto it = _segments.lower_bound(cursor->next_offset);
        for (; it != _segments.end(); it++) {
            const auto& [key, meta] = *it;
            serialize_segment_meta(meta, cursor);
            cursor->segments_serialized++;
            if (cursor->segments_serialized >= cursor->max_segments_per_call) {
                cursor->segments_serialized = 0;
                it++;
                break;
            }
        }
        if (it == _segments.end()) {
            cursor->next_offset = _last_offset;
        } else {
            // We hit the limit on number of serialized segment
            // metadata objects and 'it' points to the first segment
            // which is not serialized yet.
            cursor->next_offset = it->second.base_offset;
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

    it = std::prev(it);
    if (it->second.base_offset <= o && it->second.committed_offset >= o) {
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
std::optional<std::reference_wrapper<const partition_manifest::segment_meta>>
partition_manifest::timequery(model::timestamp t) const {
    if (_segments.empty()) {
        return std::nullopt;
    }

    auto base_t = _segments.begin()->second.base_timestamp;
    auto max_t = _segments.rbegin()->second.max_timestamp;
    auto base_offset = _segments.begin()->second.base_offset;
    auto max_offset = _segments.rbegin()->second.committed_offset;

    // Fast handling of bounds/edge cases to simplify subsequent
    // arithmetic steps
    if (t < base_t) {
        return _segments.begin()->second;
    } else if (t > max_t) {
        return std::nullopt;
    } else if (max_t == base_t) {
        // This is plausible in the case of a single segment with
        // a single batch using LogAppendTime.
        return _segments.begin()->second;
    }

    // Single-offset case should have hit max_t==base_t above
    vassert(
      max_offset > base_offset,
      "Unexpected offsets {} {} (times {} {})",
      base_offset,
      max_offset,
      base_t,
      max_t);

    // From this point on, we have finite positive differences between
    // base and max for both offset and time.
    model::offset interpolated_offset
      = base_offset
        + model::offset{
          (float(t() - base_t()) / float(max_t() - base_t()))
          * float(max_offset() - base_offset())};

    vlog(
      cst_log.debug, "timequery t={} offset guess {}", t, interpolated_offset);

    // 2. Convert offset guess into segment guess.  This is not a strictly
    // correct offset lookup, we just want something close.
    auto segment_iter = _segments.lower_bound(interpolated_offset);
    if (segment_iter == _segments.end()) {
        segment_iter = --_segments.end();
    }
    vlog(
      cst_log.debug,
      "timequery t={} segment initial guess {}",
      t,
      segment_iter->second);

    // 3. Do linear search backward or forward from the location
    //    we probed, to find the right segment.
    if (segment_iter->second.base_timestamp < t) {
        // Our guess's base_timestamp is before the search point, so our
        // result must be this segment or a later one: search forward
        for (; segment_iter != _segments.end(); ++segment_iter) {
            auto base_timestamp = segment_iter->second.base_timestamp;
            auto max_timestamp = segment_iter->second.max_timestamp;
            if (max_timestamp >= t) {
                // We found a segment bounding the search point
                break;
            } else if (base_timestamp > t) {
                // We found the first segment after the search point
                break;
            }
        }

        return segment_iter->second;
    } else {
        // Search backwards: we must peek at each preceding segment
        // to see if its max_timestamp is >= the query t, before
        // deciding whether to walk back to it.
        vlog(
          cst_log.debug, "timequery t={} reverse {}", t, segment_iter->second);

        while (segment_iter != _segments.begin()) {
            vlog(
              cst_log.debug,
              "timequery t={} reverse {}",
              t,
              segment_iter->second);
            auto prev = std::prev(segment_iter);
            if (prev->second.max_timestamp >= t) {
                segment_iter = prev;
            } else {
                break;
            }
        }
        return segment_iter->second;
    }
}

} // namespace cloud_storage
