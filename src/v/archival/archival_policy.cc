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
#include "storage/parser.h"
#include "storage/segment.h"
#include "storage/segment_set.h"
#include "storage/version.h"
#include "vlog.h"

#include <seastar/core/io_priority_class.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/util/log.hh>

namespace archival {

using namespace std::chrono_literals;

std::ostream& operator<<(std::ostream& s, const upload_candidate& c) {
    fmt::print(
      s,
      "{{segment: {}, exposed_name: {}, starting_offset:{}, "
      "file_offset: {}, content_length: {}, final_offset: {}, "
      "final_file_offset: {}}}",
      *c.source,
      c.exposed_name,
      c.starting_offset,
      c.file_offset,
      c.content_length,
      c.final_offset,
      c.final_file_offset);
    return s;
}

archival_policy::archival_policy(
  model::ntp ntp,
  service_probe& svc_probe,
  ntp_level_probe& ntp_probe,
  std::optional<segment_time_limit> limit)
  : _ntp(std::move(ntp))
  , _svc_probe(svc_probe)
  , _ntp_probe(ntp_probe)
  , _upload_limit(limit) {}

bool archival_policy::upload_deadline_reached() {
    if (!_upload_limit.has_value()) {
        return false;
    } else if (_upload_limit.value() == 0s) {
        // This code path is only used to trigger partial upload
        // in test envronment.
        return true;
    }
    auto now = ss::lowres_clock::now();

    if (!_upload_deadline.has_value()) {
        _upload_deadline = now + (*_upload_limit)();
    }
    return _upload_deadline < now;
}

archival_policy::lookup_result archival_policy::find_segment(
  model::offset last_offset,
  model::offset adjusted_lso,
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

    if (last_offset <= adjusted_lso) {
        _ntp_probe.upload_lag(adjusted_lso - last_offset);
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
    bool force_upload = upload_deadline_reached();
    if (!closed && !force_upload) {
        // Fast path, next upload candidate is not yet ready. We may want to
        // optimize this case because it's expected to happen pretty often. This
        // can be done by saving weak_ptr to the segment inside the policy
        // object. The segment must be changed to derive from
        // ss::weakly_referencable.
        if (archival_log.is_enabled(ss::log_level::debug)) {
            vlog(
              archival_log.debug,
              "Upload policy for {}, upload candidate is not closed",
              _ntp);
        }
        return {};
    }
    auto dirty_offset = (*it)->offsets().dirty_offset;
    if (dirty_offset > adjusted_lso && !force_upload) {
        vlog(
          archival_log.debug,
          "Upload policy for {}, upload candidate offset {} is above high "
          "watermark {}",
          _ntp,
          dirty_offset,
          adjusted_lso);
        return {};
    }
    if (_upload_limit) {
        _upload_deadline = ss::lowres_clock::now() + _upload_limit.value()();
    }
    return {.segment = *it, .ntp_conf = &ntp_conf, .forced = force_upload};
}

// Data sink for noop output_stream instance
// needed to implement scanning
struct null_data_sink final : ss::data_sink_impl {
    ss::future<> put(ss::net::packet data) final { return put(data.release()); }
    ss::future<> put(std::vector<ss::temporary_buffer<char>> all) final {
        return ss::do_with(
          std::move(all), [this](std::vector<ss::temporary_buffer<char>>& all) {
              return ss::do_for_each(
                all, [this](ss::temporary_buffer<char>& buf) {
                    return put(std::move(buf));
                });
          });
    }
    ss::future<> put(ss::temporary_buffer<char>) final { return ss::now(); }
    ss::future<> flush() final { return ss::now(); }
    ss::future<> close() final { return ss::now(); }
};

static ss::output_stream<char> make_null_output_stream() {
    auto ds = ss::data_sink(std::make_unique<null_data_sink>());
    ss::output_stream<char> ostr(std::move(ds), 4_KiB);
    return ostr;
}

/// This function computes offsets for the upload (inc. file offets)
/// If the full segment is uploaded the segment is not scanned.
/// If the upload is partial, the partial scan will be performed if
/// the segment has the index and full scan otherwise.
static ss::future<> get_file_range(
  model::offset begin_inclusive,
  std::optional<model::offset> end_inclusive,
  const ss::lw_shared_ptr<storage::segment>& segment,
  upload_candidate& upl) {
    size_t fsize = segment->reader().file_size();
    // These are default values for full segment upload.
    // We start with these values and refine them further
    // down the code path.
    upl.starting_offset = segment->offsets().base_offset;
    upl.file_offset = 0;
    upl.content_length = fsize;
    upl.final_offset = segment->offsets().dirty_offset;
    upl.final_file_offset = fsize;
    upl.base_timestamp = segment->index().base_timestamp();
    upl.max_timestamp = segment->index().max_timestamp();
    if (!end_inclusive && segment->offsets().base_offset == begin_inclusive) {
        // Fast path, the upload is started at the begining of the segment
        // and not truncted at the end.
        vlog(
          archival_log.debug,
          "Full segment upload {}, file size: {}",
          upl,
          fsize);
        co_return;
    }
    if (begin_inclusive != segment->offsets().base_offset) {
        // The upload is not started at the begining of the segment.
        // The segment may or may not be sealed and the final offset
        // may or may not be the last offset of the segment. This code
        // path only deals with the starting point.
        //
        // Note that the offset can be inside the record batch. We can
        // only start the upload on record batch boundary. The implementation
        // below tries to find out real starting offset of the upload
        // as well as file offset.
        // Lookup the index, if the index is available and some value is found
        // use it as a starting point otherwise, start from the begining.
        auto ix_begin = segment->index().find_nearest(begin_inclusive);
        size_t scan_from = ix_begin ? ix_begin->filepos : 0;
        model::offset sto = ix_begin ? ix_begin->offset
                                     : segment->offsets().base_offset;
        auto istr = segment->reader().data_stream(
          scan_from, ss::default_priority_class());
        auto ostr = make_null_output_stream();

        size_t bytes_to_skip = 0;
        model::timestamp ts = upl.base_timestamp;
        auto res = co_await storage::transform_stream(
          std::move(istr),
          std::move(ostr),
          [begin_inclusive, &sto, &ts](model::record_batch_header& hdr) {
              if (hdr.last_offset() < begin_inclusive) {
                  // The current record batch is accepted and will contribute to
                  // skipped length. This means that if we will read segment
                  // file starting from the 'scan_from' + 'res' we will be
                  // looking at the next record batch. We might not see the
                  // offset that we're looking for in this segment. This is why
                  // we need to update 'sto' per batch.
                  sto = hdr.last_offset() + model::offset(1);
                  // TODO: update base_timestamp
                  return storage::batch_consumer::consume_result::accept_batch;
              }
              ts = hdr.first_timestamp;
              return storage::batch_consumer::consume_result::stop_parser;
          });
        if (res.has_error()) {
            vlog(
              archival_log.error,
              "Can't read segment file, error: {}",
              res.error());
        } else {
            bytes_to_skip = scan_from + res.value();
            vlog(
              archival_log.debug,
              "Scanned {} bytes starting from {}, total {}. Adjusted starting "
              "offset: {}",
              res.value(),
              scan_from,
              bytes_to_skip,
              sto);
        }
        // Adjust content lenght and offsets at the begining of the file
        upl.starting_offset = sto;
        upl.file_offset = bytes_to_skip;
        upl.content_length -= bytes_to_skip;
        upl.base_timestamp = ts;
    }
    if (end_inclusive) {
        // Handle truncated segment upload (if the upload was triggered by time
        // limit). Note that the upload is not necessary started at the begining
        // of the segment.
        // Lookup the index, if the index is available and some value is found
        // use it as a starting point otherwise, start from the begining.
        auto ix_end = segment->index().find_nearest(end_inclusive.value());

        // NOTE: Index lookup might return an offset which isn't committed yet.
        // Subsequent call to segment_reader::data_stream will fail in this
        // case. In order to avoid this we need to make another index lookup
        // to find a lower offset which is committed.
        auto fsize = segment->reader().file_size();
        while (ix_end && ix_end->filepos > fsize) {
            vlog(archival_log.debug, "The position is not flushed {}", *ix_end);
            auto lookup_offset = ix_end->offset - model::offset(1);
            ix_end = segment->index().find_nearest(lookup_offset);
            vlog(archival_log.debug, "Re-adjusted position {}", *ix_end);
        }

        size_t scan_from = ix_end ? ix_end->filepos : 0;
        model::offset fo = ix_end ? ix_end->offset
                                  : segment->offsets().base_offset;
        vlog(
          archival_log.debug,
          "Sement index lookup returned: {}, scanning from pos {} - offset {}",
          ix_end,
          scan_from,
          fo);

        auto istr = segment->reader().data_stream(
          scan_from, ss::default_priority_class());
        auto ostr = make_null_output_stream();
        model::timestamp ts = upl.max_timestamp;
        size_t stop_at = 0;
        auto res = co_await storage::transform_stream(
          std::move(istr),
          std::move(ostr),
          [off_end = end_inclusive.value(), &fo, &ts](
            model::record_batch_header& hdr) {
              if (hdr.last_offset() <= off_end) {
                  // If last offset of the record batch is within the range
                  // we need to add it to the output stream (to calculate the
                  // total size).
                  fo = hdr.last_offset();
                  // TODO: update max_timestamp
                  return storage::batch_consumer::consume_result::accept_batch;
              }
              ts = hdr.max_timestamp;
              return storage::batch_consumer::consume_result::stop_parser;
          });
        if (res.has_error()) {
            vlog(
              archival_log.error,
              "Can't read segment file, error: {}",
              res.error());
        } else {
            stop_at = scan_from + res.value();
            vlog(
              archival_log.debug,
              "Scanned {} bytes starting from {}, total {}. Adjusted final "
              "offset: {}",
              res.value(),
              scan_from,
              stop_at,
              fo);
        }
        upl.final_offset = fo;
        upl.final_file_offset = stop_at;
        upl.content_length -= std::min(upl.content_length, (fsize - stop_at));
        upl.max_timestamp = ts;
    }
    vlog(
      archival_log.debug,
      "Partial segment upload {}, original file size: {}",
      upl,
      fsize);
}

/// \brief Initializes upload_candidate structure taking into account
///        possible segment overlaps at 'off'
///
/// \param begin_inclusive is a last_offset from manifest
/// \param end_inclusive is a last offset to upload
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
static ss::future<upload_candidate> create_upload_candidate(
  model::offset begin_inclusive,
  std::optional<model::offset> end_inclusive,
  const ss::lw_shared_ptr<storage::segment>& segment,
  const storage::ntp_config* ntp_conf) {
    auto term = segment->offsets().term;
    auto version = storage::record_version_type::v1;
    auto meta = storage::segment_path::parse_segment_filename(
      segment->reader().filename());
    if (meta) {
        version = meta->version;
    }

    upload_candidate result{.source = segment};
    co_await get_file_range(begin_inclusive, end_inclusive, segment, result);
    if (result.starting_offset != segment->offsets().base_offset) {
        // We need to generate new name for the segment
        auto path = storage::segment_path::make_segment_path(
          *ntp_conf, result.starting_offset, term, version);
        result.exposed_name = segment_name(path.filename().string());
        vlog(archival_log.debug, "Using adjusted segment name: {}", result);
    } else {
        auto orig_path = std::filesystem::path(segment->reader().filename());
        result.exposed_name = segment_name(orig_path.filename().string());
        vlog(archival_log.debug, "Using original segment name: {}", result);
    }
    co_return result;
}

ss::future<upload_candidate> archival_policy::get_next_candidate(
  model::offset begin_inclusive,
  model::offset end_exclusive,
  storage::log_manager& lm) {
    // NOTE: end_exclusive (which is initialized with LSO) points to the first
    // unstable recordbatch we need to look at the previous batch if needed.
    auto adjusted_lso = end_exclusive - model::offset(1);
    auto [segment, ntp_conf, forced] = find_segment(
      begin_inclusive, adjusted_lso, lm);
    if (segment.get() == nullptr || ntp_conf == nullptr) {
        co_return upload_candidate{};
    }
    // We need to adjust LSO since it points to the first
    // recordbatch with uncommitted transactions data
    auto last = forced ? std::make_optional(adjusted_lso) : std::nullopt;
    vlog(
      archival_log.debug,
      "Create upload candidate: Last uploaded {}, segment CO {}, adjusted LSO "
      "{}",
      begin_inclusive,
      segment->offsets().committed_offset,
      adjusted_lso);
    auto upload = co_await create_upload_candidate(
      begin_inclusive, last, segment, ntp_conf);
    if (upload.content_length == 0) {
        co_return upload_candidate{};
    }
    co_return upload;
}

} // namespace archival
