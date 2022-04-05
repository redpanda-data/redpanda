/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/partition_recovery_manager.h"

#include "bytes/iobuf_istreambuf.h"
#include "cloud_storage/logger.h"
#include "cloud_storage/types.h"
#include "config/configuration.h"
#include "hashing/xx.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/timestamp.h"
#include "s3/client.h"
#include "s3/error.h"
#include "storage/log_reader.h"
#include "storage/logger.h"
#include "storage/ntp_config.h"
#include "storage/parser.h"
#include "storage/segment_appender_utils.h"
#include "utils/gate_guard.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file-types.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/iostream-impl.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/log.hh>

#include <absl/container/btree_map.h>
#include <boost/algorithm/string/detail/sequence.hpp>
#include <boost/lexical_cast.hpp>

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
  s3::bucket_name bucket, ss::sharded<remote>& remote)
  : _bucket(std::move(bucket))
  , _remote(remote) {}

partition_recovery_manager::~partition_recovery_manager() {
    vassert(_gate.is_closed(), "S3 downloader is not stopped properly");
}

ss::future<> partition_recovery_manager::stop() { co_await _gate.close(); }

ss::future<log_recovery_result>
partition_recovery_manager::download_log(const storage::ntp_config& ntp_cfg) {
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
      ntp_cfg, &_remote.local(), _bucket, _gate, _root);
    co_return co_await downloader.download_log();
}

partition_downloader::partition_downloader(
  const storage::ntp_config& ntpc,
  remote* remote,
  s3::bucket_name bucket,
  ss::gate& gate_root,
  retry_chain_node& parent)
  : _ntpc(ntpc)
  , _bucket(std::move(bucket))
  , _remote(remote)
  , _gate(gate_root)
  , _rtcnode(download_timeout, initial_backoff, &parent)
  , _ctxlog(
      cst_log,
      _rtcnode,
      ssx::sformat("[{}, rev: {}]", ntpc.ntp().path(), ntpc.get_revision())) {}

ss::future<log_recovery_result> partition_downloader::download_log() {
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
        auto topic_manifest_path = topic_manifest::get_topic_manifest_path(
          _ntpc.ntp().ns, _ntpc.ntp().tp.topic);
        co_return co_await download_log(topic_manifest_path);
    } catch (...) {
        // We can get here if the parttion manifest is missing (or some
        // other failure is preventing us from recovering the partition). In
        // this case the exception can't be propagated since the partition
        // manager will retry and it will create an infinite loop.
        //
        // The only possible solution here is to discard the exception and
        // continue with normal partition creation process.
        //
        // Normally, this will happen when one of the partitions doesn't
        // have any data.
        vlog(
          _ctxlog.error,
          "Error during log recovery: {}",
          std::current_exception());
    }
    co_return log_recovery_result{};
}

// Parameters used to exclude data based on total size.
struct size_bound_deletion_parameters {
    size_t retention_bytes;
};

// Parameters used to exclude data based on time.
struct time_bound_deletion_parameters {
    std::chrono::milliseconds retention_duration;
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
        fmt::print(o, "{{size-bytes: {}}}", p.retention_bytes);
    } else if (std::holds_alternative<time_bound_deletion_parameters>(r)) {
        auto p = std::get<time_bound_deletion_parameters>(r);
        fmt::print(o, "{{time-ms: {}}}", p.retention_duration.count());
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
        if (prop.retention_bytes.has_value()) {
            return size_bound_deletion_parameters{prop.retention_bytes.value()};
        } else if (prop.retention_time.has_value()) {
            return time_bound_deletion_parameters{prop.retention_time.value()};
        }
    }
    return std::monostate();
}

ss::future<partition_downloader::offset_map_t>
partition_downloader::build_offset_map(const recovery_material& mat) {
    // We have multiple versions of the same partition here, some segments
    // may overlap so we need to deduplicate. Also, to take retention into
    // account.
    offset_map_t offset_map;
    const auto& manifest = mat.partition_manifest;
    for (const auto& segm : manifest) {
        if (offset_map.contains(segm.second.base_offset)) {
            auto committed
              = offset_map.at(segm.second.base_offset).meta.committed_offset;
            if (committed > segm.second.committed_offset) {
                continue;
            }
        }
        offset_map.insert_or_assign(
          segm.second.base_offset,
          segment{.manifest_key = segm.first, .meta = segm.second});
    }
    co_return std::move(offset_map);
}

// entry point for the whole thing
ss::future<log_recovery_result>
partition_downloader::download_log(const remote_manifest_path& manifest_key) {
    auto prefix = std::filesystem::path(_ntpc.work_directory());
    auto retention = get_retention_policy(_ntpc.get_overrides());
    vlog(
      _ctxlog.info,
      "The target path: {}, ntp-config revision: {}, retention: {}",
      prefix,
      _ntpc.get_revision(),
      retention);
    auto mat = co_await find_recovery_material(manifest_key);
    auto offset_map = co_await build_offset_map(mat);
    partition_manifest target(_ntpc.ntp(), _ntpc.get_initial_revision());
    for (const auto& kv : offset_map) {
        target.add(kv.second.manifest_key, kv.second.meta);
    }
    if (cst_log.is_enabled(ss::log_level::debug)) {
        std::stringstream ostr;
        target.serialize(ostr);
        vlog(_ctxlog.debug, "Generated partition manifest: {}", ostr.str());
    }
    // Here the partition manifest 'target' may contain segments
    // that have different revision ids inside the path.
    if (target.size() == 0) {
        vlog(
          _ctxlog.error,
          "No segments found. Empty partition manifest generated.");
        throw missing_partition_exception(_ntpc);
    }
    download_part part;
    if (std::holds_alternative<std::monostate>(retention)) {
        static constexpr std::chrono::seconds one_day = 86400s;
        static constexpr auto one_week = one_day * 7;
        vlog(_ctxlog.info, "Default retention parameters are used.");
        part = co_await download_log_with_capped_time(
          offset_map, target, prefix, one_week);
    } else if (std::holds_alternative<size_bound_deletion_parameters>(
                 retention)) {
        auto r = std::get<size_bound_deletion_parameters>(retention);
        vlog(
          _ctxlog.info,
          "Size bound retention is used. Size limit: {} bytes.",
          r.retention_bytes);
        part = co_await download_log_with_capped_size(
          offset_map, target, prefix, r.retention_bytes);
    } else if (std::holds_alternative<time_bound_deletion_parameters>(
                 retention)) {
        auto r = std::get<time_bound_deletion_parameters>(retention);
        vlog(
          _ctxlog.info,
          "Time bound retention is used. Time limit: {}ms.",
          r.retention_duration.count());
        part = co_await download_log_with_capped_time(
          offset_map, target, prefix, r.retention_duration);
    }
    // Move parts to final destinations
    co_await move_parts(part);

    auto upl_result = co_await _remote->upload_manifest(
      _bucket, target, _rtcnode);
    // If the manifest upload fails we can't continue
    // since it will damage the data in S3. The archival subsystem
    // will pick new partition after the leader will be elected. Then
    // it won't find the manifest in place and will create a new one.
    // If the manifest name in S3 matches the old manifest name it will
    // be overwriten and some data may be lost as a result.
    vassert(
      upl_result == upload_result::success,
      "Can't upload new manifest {} after recovery",
      target.get_manifest_path());

    // TODO (evgeny): clean up old manifests on success. Take into account
    //                that old and new manifest names could match.

    // Upload topic manifest for re-created topic (here we don't prevent
    // other partitions of the same topic to read old topic manifest if the
    // revision is different).
    if (mat.topic_manifest.get_revision() != _ntpc.get_initial_revision()) {
        mat.topic_manifest.set_revision(_ntpc.get_initial_revision());
        upl_result = co_await _remote->upload_manifest(
          _bucket, mat.topic_manifest, _rtcnode);
        if (upl_result != upload_result::success) {
            // That's probably fine since the archival subsystem will
            // re-upload topic manifest eventually.
            vlog(
              _ctxlog.warn,
              "Failed to upload new topic manifest {} after recovery",
              target.get_manifest_path());
        }
    }
    log_recovery_result result{
      .completed = true,
      .min_kafka_offset = part.range.min_offset,
      .max_kafka_offset = part.range.max_offset,
      .manifest = target,
    };
    co_return result;
}

ss::future<partition_downloader::download_part>
partition_downloader::download_log_with_capped_size(
  const offset_map_t& offset_map,
  const partition_manifest& manifest,
  const std::filesystem::path& prefix,
  size_t max_size) {
    vlog(_ctxlog.info, "Starting log download with size limit at {}", max_size);
    gate_guard guard(_gate);
    size_t total_size = 0;
    std::deque<segment> staged_downloads;
    model::offset start_offset{0};
    model::offset start_delta{0};
    for (auto it = offset_map.rbegin(); it != offset_map.rend(); it++) {
        const auto& meta = it->second.meta;
        if (total_size > max_size) {
            vlog(
              _ctxlog.debug,
              "Max size {} reached, skipping {}",
              total_size,
              it->second.manifest_key);
            break;
        } else {
            vlog(
              _ctxlog.debug,
              "Found {}, total log size {}",
              it->second.manifest_key,
              total_size);
        }
        staged_downloads.push_front(it->second);
        total_size += meta.size_bytes;
        start_offset = meta.base_offset;
        start_delta = meta.delta_offset == model::offset() ? start_delta
                                                           : meta.delta_offset;
    }
    vlog(
      _ctxlog.info,
      "Downloading {} bytes, start_offset: {}, start_delta: {}",
      total_size,
      start_offset,
      start_delta);

    download_part dlpart{
      .part_prefix = std::filesystem::path(prefix.string() + "_part"),
      .dest_prefix = prefix,
      .num_files = 0,
      .range = {
        .min_offset = model::offset::max(),
        .max_offset = model::offset::min(),
      }};

    co_await ss::max_concurrent_for_each(
      staged_downloads,
      max_concurrency,
      [this, _dlpart{&dlpart}](const segment& s) -> ss::future<> {
          auto& dlpart{*_dlpart};
          retry_chain_node fib(&_rtcnode);
          retry_chain_logger dllog(cst_log, fib);
          vlog(
            dllog.debug,
            "Starting download, base-offset: {}, term: {}, size: {}, fs "
            "prefix: {}, "
            "destination: {}",
            s.manifest_key.base_offset,
            s.manifest_key.term,
            s.meta.size_bytes,
            dlpart.part_prefix,
            dlpart.dest_prefix);
          auto offsets = co_await download_segment_file(s, dlpart);
          dlpart.range.min_offset = std::min(
            dlpart.range.min_offset, offsets.min_offset);
          dlpart.range.max_offset = std::max(
            dlpart.range.max_offset, offsets.max_offset);
          ++dlpart.num_files;
      });
    co_return dlpart;
}

ss::future<partition_downloader::download_part>
partition_downloader::download_log_with_capped_time(
  const offset_map_t& offset_map,
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

    std::deque<segment> staged_downloads;
    model::offset start_offset{0};
    model::offset start_delta{0};
    for (auto it = offset_map.rbegin(); it != offset_map.rend(); it++) {
        const auto& meta = it->second.meta;
        if (
          meta.max_timestamp == model::timestamp::missing()
          || meta.max_timestamp < time_threshold) {
            vlog(
              _ctxlog.debug,
              "Time threshold {} reached at {}, skipping {}",
              time_threshold,
              meta.max_timestamp,
              it->second.manifest_key);
            break;
        } else {
            vlog(
              _ctxlog.debug,
              "Found {}, max_timestamp {} is within the time threshold {}",
              it->second.manifest_key,
              meta.max_timestamp,
              time_threshold);
        }
        staged_downloads.push_front(it->second);
        start_offset = meta.base_offset;
        start_delta = meta.delta_offset == model::offset() ? start_delta
                                                           : meta.delta_offset;
    }
    vlog(
      _ctxlog.info,
      "Downloading log with time based retention, start_offset: {}, "
      "start_delta: {}",
      start_offset,
      start_delta);
    download_part dlpart = {
      .part_prefix = std::filesystem::path(prefix.string() + "_part"),
      .dest_prefix = prefix,
      .num_files = 0,
      .range = {
        .min_offset = model::offset::max(),
        .max_offset = model::offset::min(),
      }};

    co_await ss::max_concurrent_for_each(
      staged_downloads,
      max_concurrency,
      [this, _dlpart{&dlpart}](const segment& s) -> ss::future<> {
          auto& dlpart{*_dlpart};
          retry_chain_node fib(&_rtcnode);
          retry_chain_logger dllog(cst_log, fib);
          vlog(
            dllog.debug,
            "Starting download, base_offset: {}, term: {}, fs prefix: {}, "
            "dest: {}",
            s.manifest_key.base_offset,
            s.manifest_key.term,
            dlpart.part_prefix,
            dlpart.dest_prefix);
          auto offsets = co_await download_segment_file(s, dlpart);
          dlpart.range.min_offset = std::min(
            dlpart.range.min_offset, offsets.min_offset);
          dlpart.range.max_offset = std::max(
            dlpart.range.max_offset, offsets.max_offset);
          ++dlpart.num_files;
      });
    co_return dlpart;
}

ss::future<partition_manifest>
partition_downloader::download_manifest(const remote_manifest_path& key) {
    vlog(_ctxlog.info, "Downloading manifest {}", key);
    partition_manifest manifest(_ntpc.ntp(), _ntpc.get_initial_revision());
    auto result = co_await _remote->download_manifest(
      _bucket, key, manifest, _rtcnode);
    if (result != download_result::success) {
        throw missing_partition_exception(_ntpc);
    }
    co_return manifest;
}

ss::future<partition_downloader::recovery_material>
partition_downloader::find_recovery_material(const remote_manifest_path& key) {
    vlog(_ctxlog.info, "Downloading topic manifest {}", key);
    recovery_material recovery_mat;
    auto result = co_await _remote->download_manifest(
      _bucket, key, recovery_mat.topic_manifest, _rtcnode);
    if (result != download_result::success) {
        throw missing_partition_exception(key);
    }
    auto orig_rev = recovery_mat.topic_manifest.get_revision();
    partition_manifest tmp(_ntpc.ntp(), orig_rev);
    auto res = co_await _remote->download_manifest(
      _bucket, tmp.get_manifest_path(), tmp, _rtcnode);
    if (res != download_result::success) {
        throw missing_partition_exception(tmp.get_manifest_path());
    }
    recovery_mat.partition_manifest = std::move(tmp);
    co_return recovery_mat;
}

static ss::future<ss::output_stream<char>>
open_output_file_stream(const std::filesystem::path& path) {
    auto file = co_await ss::open_file_dma(
      path.native(), ss::open_flags::rw | ss::open_flags::create);
    auto stream = co_await ss::make_file_output_stream(std::move(file));
    co_return std::move(stream);
}

ss::future<partition_downloader::offset_range>
partition_downloader::download_segment_file(
  const segment& segm, const download_part& part) {
    auto name = generate_segment_name(
      segm.manifest_key.base_offset, segm.manifest_key.term);
    auto remote_path = generate_remote_segment_path(
      _ntpc.ntp(), segm.meta.ntp_revision, name, segm.meta.archiver_term);

    vlog(
      _ctxlog.info,
      "Downloading segment with base offset {} and term {} of size: {}, fs "
      "prefix: {}",
      segm.manifest_key.base_offset,
      segm.manifest_key.term,
      segm.meta.size_bytes,
      part.part_prefix.string());

    offset_translator otl{segm.meta.delta_offset};

    auto localpath = part.part_prefix
                     / std::string{otl.get_adjusted_segment_name(
                       segm.manifest_key, _rtcnode)()};

    if (co_await ss::file_exists(localpath.string())) {
        vlog(
          _ctxlog.info,
          "The local file {} is already downloaded but its size doesn't match "
          "the manifest and will be deleted",
          localpath);
        co_await ss::remove_file(localpath.string());
    }

    model::offset min_offset;
    model::offset max_offset;
    auto stream = [this,
                   _part{part},
                   _remote_path{remote_path},
                   _localpath{localpath},
                   _otl{otl},
                   _min_offset{&min_offset},
                   _max_offset{&max_offset}](
                    uint64_t len,
                    ss::input_stream<char> in) -> ss::future<uint64_t> {
        auto part{_part};
        auto remote_path{_remote_path};
        auto localpath{_localpath};
        auto otl{_otl};
        auto& min_offset{*_min_offset};
        auto& max_offset{*_max_offset};
        vlog(
          _ctxlog.info,
          "Copying s3 path {} to local location {}",
          remote_path,
          localpath.string());
        co_await ss::recursive_touch_directory(part.part_prefix.string());
        auto fs = co_await open_output_file_stream(localpath);
        auto stream_stats = co_await otl.copy_stream(
          std::move(in), std::move(fs), _rtcnode);

        min_offset = stream_stats.min_offset;
        max_offset = stream_stats.max_offset;

        vlog(
          _ctxlog.debug,
          "Log segment downloaded. {} bytes expected, {} bytes after "
          "pre-processing.",
          len,
          stream_stats.size_bytes);
        co_return stream_stats.size_bytes;
    };

    auto result = co_await _remote->download_segment(
      _bucket, remote_path, stream, _rtcnode);

    if (result != download_result::success) {
        // The individual segment might be missing for varios reasons but
        // it shouldn't prevent us from restoring the remaining data
        vlog(_ctxlog.error, "Failed segment download for {}", remote_path);
    }

    co_return offset_range{
      .min_offset = min_offset,
      .max_offset = max_offset,
    };
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
