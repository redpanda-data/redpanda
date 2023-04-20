/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/partition_recovery_manager.h"

#include "bytes/iobuf_istreambuf.h"
#include "cloud_storage/logger.h"
#include "cloud_storage/recovery_utils.h"
#include "cloud_storage/topic_manifest.h"
#include "cloud_storage/types.h"
#include "cluster/topic_recovery_status_frontend.h"
#include "hashing/xx.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record_batch_types.h"
#include "model/timestamp.h"
#include "storage/ntp_config.h"
#include "storage/offset_translator_state.h"
#include "storage/parser.h"
#include "utils/gate_guard.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/file-types.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/log.hh>

#include <absl/container/btree_map.h>
#include <boost/algorithm/string/detail/sequence.hpp>

#include <chrono>
#include <exception>
#include <variant>

namespace cloud_storage {
using namespace std::chrono_literals;

static constexpr ss::lowres_clock::duration download_timeout = 300s;
static constexpr ss::lowres_clock::duration initial_backoff = 200ms;

/// Partition that we're trying to fetch from S3 is missing
class missing_partition_exception final : public std::exception {
public:
    explicit missing_partition_exception(const storage::ntp_config& ntpc)
      : _msg(ssx::sformat(
        "missing partition {}, rev {}", ntpc.ntp(), ntpc.get_revision())) {}

    explicit missing_partition_exception(const remote_manifest_path& path)
      : _msg(ssx::sformat("missing partition s3://{}", path)) {}

    const char* what() const noexcept override { return _msg.c_str(); }

private:
    ss::sstring _msg;
};

partition_recovery_manager::partition_recovery_manager(
  cloud_storage_clients::bucket_name bucket, ss::sharded<remote>& remote)
  : _bucket(std::move(bucket))
  , _remote(remote)
  , _root(_as) {}

partition_recovery_manager::~partition_recovery_manager() {
    vassert(_gate.is_closed(), "S3 downloader is not stopped properly");
}

ss::future<> partition_recovery_manager::stop() {
    vlog(cst_log.debug, "Stopping partition_recovery_manager");
    _as.request_abort();
    return _gate.close();
}

ss::future<log_recovery_result> partition_recovery_manager::download_log(
  const storage::ntp_config& ntp_cfg,
  model::initial_revision_id remote_revision,
  int32_t remote_partition_count) {
    if (!ntp_cfg.has_overrides()) {
        vlog(
          cst_log.debug, "No overrides for {} found, skipping", ntp_cfg.ntp());
        co_return log_recovery_result{};
    }
    auto enabled = ntp_cfg.get_overrides().recovery_enabled;
    if (!enabled) {
        vlog(
          cst_log.debug,
          "No manifest override for {} found, skipping",
          ntp_cfg.ntp());
        co_return log_recovery_result{};
    }
    partition_downloader downloader(
      ntp_cfg,
      &_remote.local(),
      remote_revision,
      remote_partition_count,
      _bucket,
      _gate,
      _root,
      _as);
    auto result = co_await downloader.maybe_download_log();
    retry_chain_node fib{_as, download_timeout, initial_backoff};
    if (co_await is_topic_recovery_active()) {
        vlog(
          cst_log.debug,
          "topic recovery service is active, uploading result: {} for {}",
          result.logs_recovered,
          result.manifest.get_manifest_path());
        co_await cloud_storage::place_download_result(
          _remote.local(), _bucket, ntp_cfg, result.logs_recovered, fib);
    }
    co_return result;
}

ss::future<bool> partition_recovery_manager::is_topic_recovery_active() const {
    const auto is_initialized
      = _topic_recovery_status_frontend.has_value()
        && _topic_recovery_service.has_value()
        && _topic_recovery_status_frontend->get().local_is_initialized()
        && _topic_recovery_service->get().local_is_initialized();
    if (!is_initialized) {
        co_return false;
    }

    co_return co_await _topic_recovery_status_frontend->get()
      .local()
      .is_recovery_running(
        _topic_recovery_service->get(),
        cluster::topic_recovery_status_frontend::skip_this_node::no);
}

void partition_recovery_manager::set_topic_recovery_components(
  ss::sharded<cluster::topic_recovery_status_frontend>&
    topic_recovery_status_frontend,
  ss::sharded<cloud_storage::topic_recovery_service>& topic_recovery_service) {
    _topic_recovery_status_frontend.emplace(topic_recovery_status_frontend);
    _topic_recovery_service.emplace(topic_recovery_service);
}

partition_downloader::partition_downloader(
  const storage::ntp_config& ntpc,
  remote* remote,
  model::initial_revision_id remote_rev_id,
  int32_t remote_partition_count,
  cloud_storage_clients::bucket_name bucket,
  ss::gate& gate_root,
  retry_chain_node& parent,
  storage::opt_abort_source_t as)
  : _ntpc(ntpc)
  , _bucket(std::move(bucket))
  , _remote(remote)
  , _remote_revision_id(remote_rev_id)
  , _remote_partition_count(remote_partition_count)
  , _gate(gate_root)
  , _rtcnode(download_timeout, initial_backoff, &parent)
  , _ctxlog(
      cst_log,
      _rtcnode,
      ssx::sformat("[{}, rev: {}]", ntpc.ntp().path(), ntpc.get_revision()))
  , _as(as) {}

ss::future<log_recovery_result> partition_downloader::maybe_download_log() {
    vlog(_ctxlog.debug, "Check conditions for S3 recovery for {}", _ntpc);
    if (!_ntpc.has_overrides()) {
        vlog(_ctxlog.debug, "No overrides for {} found, skipping", _ntpc.ntp());
        co_return log_recovery_result{};
    }
    // TODO (evgeny): maybe check the condition differently
    bool exists = co_await ss::file_exists(_ntpc.work_directory());
    if (exists) {
        co_return log_recovery_result{};
    }
    auto enabled = _ntpc.get_overrides().recovery_enabled;
    if (!enabled) {
        vlog(
          _ctxlog.debug,
          "No manifest override for {} found, skipping",
          _ntpc.ntp());
        co_return log_recovery_result{};
    }
    vlog(_ctxlog.info, "Downloading log for {}", _ntpc.ntp());
    try {
        co_return co_await download_log();

    } catch (const ss::abort_requested_exception&) {
        throw;
    } catch (const ss::gate_closed_exception&) {
        throw;
    } catch (const missing_partition_exception& err) {
        // We can get here in case if manifest doesn't exist in S3. In
        // this case the exception can't be propagated since the partition
        // manager will retry and it will create an infinite loop.
        //
        // The only possible solution here is to discard the exception and
        // continue with normal partition creation process.
        vlog(_ctxlog.error, "Error during log recovery: {}", err);
    } catch (...) {
        // We can get here in case of transient download error.
        // The controller will retry recovery after some time.
        vlog(
          _ctxlog.error,
          "Error during log recovery: {}",
          std::current_exception());
        throw;
    }
    co_return log_recovery_result{};
}

// Parameters used to exclude data based on total size.
struct size_bound_deletion_parameters {
    size_t bytes;
};

// Parameters used to exclude data based on time.
struct time_bound_deletion_parameters {
    std::chrono::milliseconds duration;
};

// Retention policy that should be used during recovery
using retention = std::variant<
  std::monostate,
  size_bound_deletion_parameters,
  time_bound_deletion_parameters>;

std::ostream& operator<<(std::ostream& o, const retention& r) {
    if (std::holds_alternative<std::monostate>(r)) {
        fmt::print(o, "{{none}}");
    } else if (std::holds_alternative<size_bound_deletion_parameters>(r)) {
        auto p = std::get<size_bound_deletion_parameters>(r);
        fmt::print(o, "{{size-bytes: {}}}", p.bytes);
    } else if (std::holds_alternative<time_bound_deletion_parameters>(r)) {
        auto p = std::get<time_bound_deletion_parameters>(r);
        fmt::print(o, "{{time-ms: {}}}", p.duration.count());
    }
    return o;
}

static retention
get_retention_policy(const storage::ntp_config::default_overrides& prop) {
    auto flags = prop.cleanup_policy_bitflags;
    if (
      flags
      && (flags.value() & model::cleanup_policy_bitflags::deletion)
           == model::cleanup_policy_bitflags::deletion) {
        // If a space constraint is set on the topic, use that: otherwise
        // use time based constraint if present.  If total retention setting
        // is less than local retention setting, take the smallest.
        //
        // This differs from ordinary storage GC, in that we are applying
        // space _or_ time bounds: not both together.
        //
        // This will also drop the compact settings and replace it with
        // delete.
        if (prop.retention_local_target_bytes.has_optional_value()) {
            auto v = prop.retention_local_target_bytes.value();

            if (prop.retention_bytes.has_optional_value()) {
                v = std::min(prop.retention_bytes.value(), v);
            }
            return size_bound_deletion_parameters{v};
        } else if (prop.retention_local_target_ms.has_optional_value()) {
            auto v = prop.retention_local_target_ms.value();
            if (prop.retention_time.has_optional_value()) {
                v = std::min(prop.retention_time.value(), v);
            }
            return time_bound_deletion_parameters{v};
        }
    }
    return std::monostate();
}

static auto build_offset_map(const partition_manifest& manifest) {
    absl::btree_map<model::offset, segment_meta> offset_map;
    for (const auto& segm : manifest) {
        offset_map.insert_or_assign(segm.second.base_offset, segm.second);
    }
    return offset_map;
}

// Return previous offset. This is different from
// model::prev_offset because it returns -1 for offset 0.
// The model::offset{} is a special case since the result
// of the decrement in this case is undefined.
static model::offset get_prev_offset(model::offset o) {
    vassert(o != model::offset{}, "Can't return previous offset");
    return o - model::offset{1};
}

// entry point for the whole thing
ss::future<log_recovery_result> partition_downloader::download_log() {
    auto prefix = std::filesystem::path(_ntpc.work_directory());
    auto retention = get_retention_policy(_ntpc.get_overrides());
    vlog(
      _ctxlog.info,
      "The target path: {}, ntp-config revision: {}, retention: {}",
      prefix,
      _ntpc.get_revision(),
      retention);
    auto mat = co_await find_recovery_material();
    if (cst_log.is_enabled(ss::log_level::debug)) {
        std::stringstream ostr;
        mat.partition_manifest.serialize(ostr);
        vlog(
          _ctxlog.debug,
          "Partition manifest used for recovery: {}",
          ostr.str());
    }
    if (mat.partition_manifest.size() == 0) {
        // If the downloaded manifest doesn't have any segments
        log_recovery_result result{
          .logs_recovered = true,
          .clean_download = true,
          .min_offset = model::offset{0},
          .max_offset = model::offset{0},
          .manifest = mat.partition_manifest,
          .ot_state = nullptr,
        };
        co_return result;
    }
    download_part part;
    if (std::holds_alternative<std::monostate>(retention)) {
        static constexpr std::chrono::seconds one_day = 86400s;
        static constexpr auto one_week = one_day * 7;
        vlog(_ctxlog.info, "Default retention parameters are used.");
        part = co_await download_log_with_capped_time(
          build_offset_map(mat.partition_manifest),
          mat.partition_manifest,
          prefix,
          one_week);
    } else if (std::holds_alternative<size_bound_deletion_parameters>(
                 retention)) {
        auto r = std::get<size_bound_deletion_parameters>(retention);
        vlog(
          _ctxlog.info,
          "Size bound retention is used. Size limit: {} bytes.",
          r.bytes);
        part = co_await download_log_with_capped_size(
          build_offset_map(mat.partition_manifest),
          mat.partition_manifest,
          prefix,
          r.bytes);
    } else if (std::holds_alternative<time_bound_deletion_parameters>(
                 retention)) {
        auto r = std::get<time_bound_deletion_parameters>(retention);
        vlog(
          _ctxlog.info,
          "Time bound retention is used. Time limit: {}ms.",
          r.duration.count());
        part = co_await download_log_with_capped_time(
          build_offset_map(mat.partition_manifest),
          mat.partition_manifest,
          prefix,
          r.duration);
    }
    // Move parts to final destinations
    if (part.num_files > 0) {
        co_await move_parts(part);
    }

    log_recovery_result result{
      .logs_recovered = true,
      .clean_download = part.clean_download,
      .min_offset = part.range.min_offset,
      .max_offset = part.range.max_offset,
      .manifest = mat.partition_manifest,
      .ot_state = part.ot_state,
    };
    co_return result;
}

void partition_downloader::update_downloaded_offsets(
  std::vector<partition_downloader::offset_range> dloffsets,
  partition_downloader::download_part& dlpart) {
    auto to_erase = std::remove_if(
      dloffsets.begin(), dloffsets.end(), [](offset_range r) {
          return r.max_offset == model::offset::min();
      });
    dloffsets.erase(to_erase, dloffsets.end());
    std::sort(dloffsets.begin(), dloffsets.end());
    bool missing_offsets = false;
    for (auto it = dloffsets.rbegin(); it != dloffsets.rend(); it++) {
        auto offsets = *it;
        if (dlpart.range.min_offset < dlpart.range.max_offset) {
            // This will be triggered for every iteration except the first one
            auto expected = offsets.max_offset + model::offset(1);
            if (dlpart.range.min_offset != expected) {
                vlog(
                  _ctxlog.warn,
                  "Gap detected at {}, restored offset range {}-{}",
                  expected,
                  dlpart.range.min_offset,
                  dlpart.range.max_offset);
                missing_offsets = true;
                break;
            }
        }
        // Only count non-empty files
        dlpart.range.min_offset = std::min(
          dlpart.range.min_offset, offsets.min_offset);
        dlpart.range.max_offset = std::max(
          dlpart.range.max_offset, offsets.max_offset);
        if (dlpart.range.max_offset > dlpart.range.min_offset) {
            ++dlpart.num_files;
        }
    }

    dlpart.clean_download = !missing_offsets;
}

ss::future<partition_downloader::download_part>
partition_downloader::download_log_with_capped_size(
  offset_map_t offset_map,
  const partition_manifest& manifest,
  const std::filesystem::path& prefix,
  size_t max_size) {
    vlog(_ctxlog.info, "Starting log download with size limit at {}", max_size);
    gate_guard guard(_gate);

    std::deque<segment_meta> staged_downloads;
    model::offset start_offset{0};
    model::offset_delta start_delta{0};
    size_t total_size = 0;
    if (!_ntpc.is_remote_fetch_enabled()) {
        // Download logs only if tiered storage is disabled.
        // Otherwise we should do shallow recovery.
        for (auto it = offset_map.rbegin(); it != offset_map.rend(); it++) {
            const auto& meta = it->second;
            if (total_size + meta.size_bytes >= max_size) {
                // At the moment we can't safely restore the partition without
                // downloading at least one segment.
                break;
            }
            total_size += meta.size_bytes;
            staged_downloads.push_front(it->second);
            start_offset = meta.base_offset;
            start_delta = meta.delta_offset == model::offset_delta()
                            ? start_delta
                            : meta.delta_offset;
        }
    }

    std::vector<offset_range> dloffsets;
    auto ot_state = ss::make_lw_shared<storage::offset_translator_state>(
      _ntpc.ntp(), get_prev_offset(start_offset), start_delta());
    download_part dlpart{
      .part_prefix = std::filesystem::path(prefix.string() + "_part"),
      .dest_prefix = prefix,
      .num_files = 0,
      .range = {
        .min_offset = model::offset::max(),
        .max_offset = model::offset::min(),
      },
      .ot_state = ot_state};

    retry_chain_node fib(&_rtcnode);
    retry_chain_logger dllog(cst_log, fib);

    vlog(
      dllog.debug,
      "Setting up offset translator for the partition, start offset: {}, start "
      "delta: {}",
      start_offset,
      start_delta);

    for (auto s : staged_downloads) {
        vlog(
          dllog.debug,
          "Starting download, base-offset: {}, term: {}, size: {}, fs "
          "prefix: {}, "
          "destination: {}",
          s.base_offset,
          s.segment_term,
          s.size_bytes,
          dlpart.part_prefix,
          dlpart.dest_prefix);
        auto offsets = co_await download_segment_file(s, dlpart);
        if (offsets.has_value()) {
            dloffsets.push_back(offset_range{
              .min_offset = offsets->min_offset,
              .max_offset = offsets->max_offset,
            });
        }
    }

    update_downloaded_offsets(std::move(dloffsets), dlpart);
    co_return dlpart;
}

ss::future<partition_downloader::download_part>
partition_downloader::download_log_with_capped_time(
  offset_map_t offset_map,
  const partition_manifest& manifest,
  const std::filesystem::path& prefix,
  model::timestamp_clock::duration retention_time) {
    vlog(
      _ctxlog.info,
      "Starting log download with time limit of {}s",
      std::chrono::duration_cast<std::chrono::milliseconds>(retention_time)
          .count()
        / 1000);
    gate_guard guard(_gate);
    auto time_threshold = model::to_timestamp(
      model::timestamp_clock::now() - retention_time);

    std::deque<segment_meta> staged_downloads;
    model::offset start_offset{0};
    model::offset_delta start_delta{0};
    if (!_ntpc.is_remote_fetch_enabled()) {
        // Download logs only if tiered storage is disabled.
        // Otherwise we should do shallow recovery.
        for (auto it = offset_map.rbegin(); it != offset_map.rend(); it++) {
            const auto& meta = it->second;
            if (
              meta.max_timestamp == model::timestamp::missing()
              || meta.max_timestamp < time_threshold) {
                // At least one segment has to be downloaded to guarantee
                // successful recovery.
                vlog(
                  _ctxlog.debug,
                  "Time threshold {} reached at {}, skipping {}",
                  time_threshold,
                  meta.max_timestamp,
                  it->second.base_offset);
                break;
            } else {
                vlog(
                  _ctxlog.debug,
                  "Found {}, max_timestamp {} is within the time threshold {}",
                  it->second.base_offset,
                  meta.max_timestamp,
                  time_threshold);
            }
            staged_downloads.push_front(it->second);
            start_offset = meta.base_offset;
            start_delta = meta.delta_offset == model::offset_delta()
                            ? start_delta
                            : meta.delta_offset;
        }
    }
    vlog(
      _ctxlog.info,
      "Downloading log with time based retention, start_offset: {}, "
      "start_delta: {}",
      start_offset,
      start_delta);
    std::vector<offset_range> dloffsets;
    auto ot_state = ss::make_lw_shared<storage::offset_translator_state>(
      _ntpc.ntp(), get_prev_offset(start_offset), start_delta());
    download_part dlpart = {
      .part_prefix = std::filesystem::path(prefix.string() + "_part"),
      .dest_prefix = prefix,
      .num_files = 0,
      .range = {
        .min_offset = model::offset::max(),
        .max_offset = model::offset::min(),
      },
      .ot_state = ot_state};

    retry_chain_node fib(&_rtcnode);
    retry_chain_logger dllog(cst_log, fib);

    vlog(
      dllog.debug,
      "Setting up offset translator for the partition, start offset: {}, start "
      "delta: {}",
      start_offset,
      start_delta);

    for (auto s : staged_downloads) {
        vlog(
          dllog.debug,
          "Starting download, base_offset: {}, term: {}, fs prefix: {}, "
          "dest: {}",
          s.base_offset,
          s.segment_term,
          dlpart.part_prefix,
          dlpart.dest_prefix);
        auto offsets = co_await download_segment_file(s, dlpart);
        if (offsets.has_value()) {
            dloffsets.push_back({
              .min_offset = offsets->min_offset,
              .max_offset = offsets->max_offset,
            });
        }
    }
    update_downloaded_offsets(std::move(dloffsets), dlpart);
    co_return dlpart;
}

ss::future<partition_downloader::recovery_material>
partition_downloader::find_recovery_material() {
    recovery_material recovery_mat;
    partition_manifest tmp(_ntpc.ntp(), _remote_revision_id);
    vlog(
      _ctxlog.info,
      "Downloading partition manifest {}",
      tmp.get_manifest_path());
    auto res = co_await _remote->download_manifest(
      _bucket, tmp.get_manifest_path(), tmp, _rtcnode);
    if (res == download_result::success) {
        recovery_mat.partition_manifest = std::move(tmp);
        co_return recovery_mat;
    }
    if (res == download_result::notfound) {
        // Manifest is not available in the cloud
        throw missing_partition_exception(tmp.get_manifest_path());
    }
    // Some other, possibly transient error
    throw std::runtime_error(
      fmt_with_ctx(fmt::format, "Can't download manifest: {}", res));
}

static ss::future<ss::output_stream<char>>
open_output_file_stream(const std::filesystem::path& path) {
    auto file = co_await ss::open_file_dma(
      path.native(), ss::open_flags::rw | ss::open_flags::create);
    auto stream = co_await ss::make_file_output_stream(std::move(file));
    co_return std::move(stream);
}

ss::future<uint64_t> partition_downloader::download_segment_file_stream(
  uint64_t len,
  ss::input_stream<char> in,
  const download_part& part,
  remote_segment_path remote_path,
  std::filesystem::path local_path,
  offset_translator otl,
  stream_stats& stats) {
    vlog(
      _ctxlog.info,
      "Copying s3 path {} to local location {}",
      remote_path,
      local_path.string());
    co_await ss::recursive_touch_directory(part.part_prefix.string());
    auto fs = co_await open_output_file_stream(local_path);
    stats = co_await otl.copy_stream(std::move(in), std::move(fs), _rtcnode);

    if (stats.size_bytes == 0) {
        vlog(
          _ctxlog.debug,
          "Log segment downloaded empty. Original size: {}.",
          len);
        // The segment is empty after filtering
        co_await ss::remove_file(local_path.native());
    } else {
        vlog(
          _ctxlog.debug,
          "Log segment downloaded. {} bytes expected, {} bytes after "
          "pre-processing.",
          len,
          stats.size_bytes);
    }

    co_return stats.size_bytes;
}

ss::future<std::optional<cloud_storage::stream_stats>>
partition_downloader::download_segment_file(
  const segment_meta& segm, const download_part& part) {
    auto name = generate_local_segment_name(
      segm.base_offset, segm.segment_term);
    auto remote_path = partition_manifest::generate_remote_segment_path(
      _ntpc.ntp(), segm);

    auto localpath = part.part_prefix / std::filesystem::path(name());

    vlog(
      _ctxlog.info,
      "Downloading segment with base offset {} and term {} of size: {}, fs "
      "prefix: {}",
      segm.base_offset,
      segm.segment_term,
      segm.size_bytes,
      part.part_prefix.string(),
      localpath);

    offset_translator otl{segm.delta_offset, part.ot_state, _as};

    if (co_await ss::file_exists(localpath.string())) {
        vlog(
          _ctxlog.info,
          "The local file {} is already downloaded but its size doesn't match "
          "the manifest and will be deleted",
          localpath);
        co_await ss::remove_file(localpath.string());
    }

    auto stream_stats = cloud_storage::stream_stats{};

    auto stream = [this,
                   &stream_stats,
                   _part{part},
                   _remote_path{remote_path},
                   _localpath{localpath},
                   _otl{otl}](
                    uint64_t len,
                    ss::input_stream<char> in) -> ss::future<uint64_t> {
        return download_segment_file_stream(
          len,
          std::move(in),
          _part,
          _remote_path,
          _localpath,
          _otl,
          stream_stats);
    };

    auto result = co_await _remote->download_segment(
      _bucket, remote_path, stream, _rtcnode);

    if (result != download_result::success) {
        // The individual segment might be missing for varios reasons but
        // it shouldn't prevent us from restoring the remaining data
        vlog(_ctxlog.error, "Failed segment download for {}", remote_path);
        co_return std::nullopt;
    }

    co_return stream_stats;
}

ss::future<> partition_downloader::move_parts(download_part dls) {
    vlog(
      _ctxlog.info,
      "Renaming directory {} to {}",
      dls.part_prefix,
      dls.dest_prefix);
    co_await ss::rename_file(
      dls.part_prefix.string(), dls.dest_prefix.string());
}

} // namespace cloud_storage
