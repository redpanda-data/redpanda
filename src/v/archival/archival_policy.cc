/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/archival_policy.h"

#include "archival/logger.h"
#include "storage/disk_log_impl.h"
#include "storage/fs_utils.h"
#include "storage/segment.h"
#include "storage/segment_set.h"
#include "storage/version.h"
#include "vlog.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/util/log.hh>

namespace archival {

using namespace std::chrono_literals;

std::ostream& operator<<(std::ostream& s, const upload_candidate& c) {
    s << "{ exposed_name: " << c.exposed_name
      << ", starting_offset: " << c.starting_offset
      << ", segment_file_name: " << c.source->reader().filename() << " }";
    return s;
}

archival_policy::archival_policy(
  model::ntp ntp, service_probe& svc_probe, ntp_level_probe& ntp_probe)
  : _ntp(std::move(ntp))
  , _svc_probe(svc_probe)
  , _ntp_probe(ntp_probe) {}

archival_policy::lookup_result archival_policy::find_segment(
  model::offset last_offset,
  model::offset high_watermark,
  storage::log_manager& lm) {
    vlog(
      archival_log.debug,
      "Upload policy for {} invoked, last-offset: {}",
      _ntp,
      last_offset);
    std::optional<storage::log> log = lm.get(_ntp);
    if (!log) {
        vlog(archival_log.warn, "Upload policy for {} no such ntp", _ntp);
        return {};
    }
    auto plog = dynamic_cast<storage::disk_log_impl*>(log->get_impl());
    // NOTE: we need to break encapsulation here to access underlying
    // implementation because upload policy and archival subsystem needs to
    // access individual log segments (disk backed).
    if (plog == nullptr || plog->segment_count() == 0) {
        vlog(
          archival_log.debug,
          "Upload policy for {} no segments or in-memory log",
          _ntp);
        return {};
    }
    const auto& set = plog->segments();

    if (last_offset <= high_watermark) {
        _ntp_probe.upload_lag(high_watermark - last_offset);
    }

    const auto& ntp_conf = plog->config();
    auto it = set.lower_bound(last_offset);
    if (it == set.end() || (*it)->is_compacted_segment()) {
        // Skip forward if we hit a gap or compacted segment
        for (auto i = set.begin(); i != set.end(); i++) {
            const auto& sg = *i;
            if (last_offset < sg->offsets().base_offset) {
                // Move last offset forward
                it = i;
                auto offset = sg->offsets().base_offset;
                auto delta = offset - last_offset;
                last_offset = offset;
                _svc_probe.add_gap();
                _ntp_probe.gap_detected(delta);
                break;
            }
        }
    }
    if (it == set.end()) {
        vlog(
          archival_log.trace,
          "Upload policy for {}, upload candidate is not found",
          _ntp);
        return {};
    }
    // Invariant: it != set.end()
    bool closed = !(*it)->has_appender();
    if (!closed) {
        // Fast path, next upload candidate is not yet sealed. We may want to
        // optimize this case because it's expected to happen pretty often. This
        // can be done by saving weak_ptr to the segment inside the policy
        // object. The segment must be changed to derive from
        // ss::weakly_referencable.
        vlog(
          archival_log.trace,
          "Upload policy for {}, upload candidate is not closed",
          _ntp);
        return {};
    }
    auto dirty_offset = (*it)->offsets().dirty_offset;
    if (dirty_offset > high_watermark) {
        vlog(
          archival_log.debug,
          "Upload policy for {}, upload candidate offset {} is below high "
          "watermark {}",
          dirty_offset,
          high_watermark,
          _ntp);
        return {};
    }
    return {.segment = *it, .ntp_conf = &ntp_conf};
}

/// \brief Initializes upload_candidate structure taking into account
///        possible segment overlaps at 'off'
///
/// \param off is a last_offset from manifest
/// \param segment is a segment that has this offset
/// \param ntp_conf is a ntp_config of the partition
///
/// \note Normally, the segments on a single node doesn't overlap,
///       but when leadership changes the new node will have to deal
///       with the situaiton when 'last_offset' doesn't match base
///       offset of any segment. In this situation we need to find
///       the segment that contains the 'last_offset' and find the
///       exact location of the 'last_offset' inside the segment (
///       or nearset offset, because index is sampled). Then we need
///       to compute file offset of the upload, content length and
///       new base offset (and new segment name for S3).
///       Example: last_offset is 1000, and we have segment with the
///       name '900-1-v1.log'. We should find offset 1000 inside it
///       and upload starting from it. The resulting file should have
///       a name '1000-1-v1.log'. If we were only able to find offset
///       990 instead of 1000, we will upload starting from it and
///       the name will be '990-1-v1.log'.
static upload_candidate create_upload_candidate(
  model::offset off,
  const ss::lw_shared_ptr<storage::segment>& segment,
  const storage::ntp_config* ntp_conf) {
    auto term = segment->offsets().term;
    auto version = storage::record_version_type::v1;
    auto meta = storage::segment_path::parse_segment_filename(
      segment->reader().filename());
    if (meta) {
        version = meta->version;
    }
    size_t fsize = segment->reader().file_size();
    auto ix = segment->index().find_nearest(off);
    if (ix.has_value() == false || segment->offsets().base_offset == off) {
        // Fast path, base_offset matches the query or we can't lookup offsets
        // because there is no index
        auto orig_path = std::filesystem::path(segment->reader().filename());
        vlog(
          archival_log.debug,
          "Uploading full segment {}, last_offset: {}, located segment: {}, "
          "file size: {}",
          segment->reader().filename(),
          off,
          segment->offsets(),
          fsize);
        return {
          .source = segment,
          .exposed_name = segment_name(orig_path.filename().string()),
          .starting_offset = segment->offsets().base_offset,
          .file_offset = 0,
          .content_length = fsize};
    }
    size_t pos = ix ? ix->filepos : 0;
    model::offset exposed_offset = ix ? ix->offset : meta->base_offset;
    size_t clen = fsize - pos;
    auto path = storage::segment_path::make_segment_path(
      *ntp_conf, exposed_offset, term, version);
    vlog(
      archival_log.debug,
      "Uploading part of the segment {}, last_offset: {}, located segment: {}, "
      "file size: {}, starting from the offset {}",
      segment->reader().filename(),
      off,
      segment->offsets(),
      clen,
      path);
    return {
      .source = segment,
      .exposed_name = segment_name(path.filename().string()),
      .starting_offset = exposed_offset,
      .file_offset = pos,
      .content_length = clen};
}

upload_candidate archival_policy::get_next_candidate(
  model::offset last_offset,
  model::offset high_watermark,
  storage::log_manager& lm) {
    auto [segment, ntp_conf] = find_segment(last_offset, high_watermark, lm);
    if (segment.get() == nullptr || ntp_conf == nullptr) {
        return {};
    }
    return create_upload_candidate(last_offset, segment, ntp_conf);
}

} // namespace archival
