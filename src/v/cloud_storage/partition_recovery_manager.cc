#include "cloud_storage/partition_recovery_manager.h"

#include "bytes/iobuf_istreambuf.h"
#include "cloud_storage/logger.h"
#include "cloud_storage/manifest.h"
#include "cloud_storage/offset_translation_layer.h"
#include "cloud_storage/types.h"
#include "config/configuration.h"
#include "hashing/xx.h"
#include "json/json.h"
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

/// Download full log based on manifest data.
/// The 'ntp_config' should have corresponding override. If override
/// is not set nothing will happen and the returned future will be
/// ready (not in failed state).
/// \return true if log was actually downloaded, false otherwise
ss::future<bool>
partition_recovery_manager::download_log(const storage::ntp_config& ntp_cfg) {
    if (!ntp_cfg.has_overrides()) {
        vlog(
          cst_log.debug, "No overrides for {} found, skipping", ntp_cfg.ntp());
        co_return false;
    }
    auto enabled = ntp_cfg.get_overrides().recovery_enabled;
    if (!enabled) {
        vlog(
          cst_log.debug,
          "No manifest override for {} found, skipping",
          ntp_cfg.ntp());
        co_return false;
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

ss::future<bool> partition_downloader::download_log() {
    vlog(_ctxlog.debug, "Check conditions for S3 recovery for {}", _ntpc);
    if (!_ntpc.has_overrides()) {
        vlog(_ctxlog.debug, "No overrides for {} found, skipping", _ntpc.ntp());
        co_return false;
    }
    // TODO (evgeny): maybe check the condition differently
    bool exists = co_await ss::file_exists(_ntpc.work_directory());
    if (exists) {
        co_return false;
    }
    auto enabled = _ntpc.get_overrides().recovery_enabled;
    if (!enabled) {
        vlog(
          _ctxlog.debug,
          "No manifest override for {} found, skipping",
          _ntpc.ntp());
        co_return false;
    }
    vlog(_ctxlog.info, "Downloading log for {}", _ntpc.ntp());
    try {
        auto topic_manifest_path = topic_manifest::get_topic_manifest_path(
          _ntpc.ntp().ns, _ntpc.ntp().tp.topic);
        co_await download_log(topic_manifest_path);
        co_return true;
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
    co_return false;
}

static bool same_ntp(const manifest_path_components& c, const model::ntp& ntp) {
    return c._ns == ntp.ns && c._topic == ntp.tp.topic
           && c._part == ntp.tp.partition;
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
    for (const auto& p : mat.paths) {
        auto manifest = co_await download_manifest(p);
        for (const auto& segm : manifest) {
            if (offset_map.contains(segm.second.base_offset)) {
                auto committed = offset_map.at(segm.second.base_offset)
                                   .meta.committed_offset;
                if (committed > segm.second.committed_offset) {
                    continue;
                }
            }
            auto path = manifest.get_remote_segment_path(segm.first);
            offset_map.insert_or_assign(
              segm.second.base_offset,
              segment{.path = path, .meta = segm.second});
        }
    }
    co_return std::move(offset_map);
}

// entry point for the whole thing
ss::future<>
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
    manifest target(_ntpc.ntp(), _ntpc.get_revision());
    for (const auto& kv : offset_map) {
        // Original manifests contain short names (e.g. 1029-4-v1.log).
        // This is because they belong to the same revision and the details are
        // encoded in the manifest itself.
        // To create a compound manifest we need to add full names (e.g.
        // 6fab5988/kafka/redpanda-test/5_6/80651-2-v1.log). Otherwise the
        // information in the manifest won't be suffecient.
        target.add(kv.second.path, kv.second.meta);
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
    co_await move_parts(std::move(part));

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
    if (mat.topic_manifest.get_revision() != _ntpc.get_revision()) {
        mat.topic_manifest.set_revision(_ntpc.get_revision());
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
    co_return;
}

ss::future<partition_downloader::download_part>
partition_downloader::download_log_with_capped_size(
  const offset_map_t& offset_map,
  const manifest& manifest,
  const std::filesystem::path& prefix,
  size_t max_size) {
    vlog(_ctxlog.info, "Starting log download with size limit at {}", max_size);
    gate_guard guard(_gate);
    size_t total_size = 0;
    struct download {
        remote_segment_path fname;
        size_t size_bytes;
    };
    std::deque<download> staged_downloads;
    for (auto it = offset_map.rbegin(); it != offset_map.rend(); it++) {
        const auto& meta = it->second.meta;
        if (total_size > max_size) {
            vlog(
              _ctxlog.debug,
              "Max size {} reached, skipping {}",
              total_size,
              it->second.path);
            break;
        } else {
            vlog(
              _ctxlog.debug,
              "Found {}, total log size {}",
              it->second.path,
              total_size);
        }
        auto fname = it->second.path;
        staged_downloads.push_front({
          .fname = fname,
          .size_bytes = meta.size_bytes,
        });
        total_size += meta.size_bytes;
    }
    download_part dlpart{
      .part_prefix = std::filesystem::path(prefix.string() + "_part"),
      .dest_prefix = prefix,
      .num_files = 0};

    co_await ss::max_concurrent_for_each(
      staged_downloads,
      max_concurrency,
      [this, &manifest, &dlpart](download& dl) -> ss::future<> {
          retry_chain_node fib(&_rtcnode);
          retry_chain_logger dllog(cst_log, fib);
          vlog(
            dllog.debug,
            "Starting download, fname: {} of size: {}, fs prefix: {}, "
            "destination: {}",
            dl.fname,
            dl.size_bytes,
            dlpart.part_prefix,
            dlpart.dest_prefix);
          co_await download_file(dl.fname, manifest, dlpart);
          ++dlpart.num_files;
      });
    co_return dlpart;
}

ss::future<partition_downloader::download_part>
partition_downloader::download_log_with_capped_time(
  const offset_map_t& offset_map,
  const manifest& manifest,
  const std::filesystem::path& prefix,
  model::timestamp_clock::duration retention_time) {
    vlog(
      _ctxlog.info,
      "Starting log download with time limit of {}s",
      retention_time.count() / 1000);
    gate_guard guard(_gate);
    auto time_threshold = model::to_timestamp(
      model::timestamp_clock::now() - retention_time);

    std::deque<remote_segment_path> staged_downloads;

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
              it->second.path);
            break;
        } else {
            vlog(
              _ctxlog.debug,
              "Found {}, max_timestamp {} is within the time threshold {}",
              it->second.path,
              meta.max_timestamp,
              time_threshold);
        }
        auto fname = it->second.path;
        staged_downloads.push_front(fname);
    }
    download_part dlpart = {
      .part_prefix = std::filesystem::path(prefix.string() + "_part"),
      .dest_prefix = prefix,
      .num_files = 0};

    co_await ss::max_concurrent_for_each(
      staged_downloads,
      max_concurrency,
      [this, &manifest, &dlpart](
        const remote_segment_path& fname) -> ss::future<> {
          retry_chain_node fib(&_rtcnode);
          retry_chain_logger dllog(cst_log, fib);
          vlog(
            dllog.debug,
            "Starting download, fname: {}, fs prefix: {}, dest: {}",
            fname,
            dlpart.part_prefix,
            dlpart.dest_prefix);
          co_await download_file(fname, manifest, dlpart);
          ++dlpart.num_files;
      });
    co_return dlpart;
}

ss::future<manifest>
partition_downloader::download_manifest(const remote_manifest_path& key) {
    vlog(_ctxlog.info, "Downloading manifest {}", key);
    manifest manifest(_ntpc.ntp(), _ntpc.get_revision());
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
    recovery_mat.paths = co_await find_matching_partition_manifests(
      recovery_mat.topic_manifest);
    co_return recovery_mat;
}

ss::future<std::vector<remote_manifest_path>>
partition_downloader::find_matching_partition_manifests(
  topic_manifest& manifest) {
    // TODO: use only selected prefixes
    auto topic_rev = manifest.get_revision();
    std::vector<remote_manifest_path> all_manifests;
    auto obj_iter = [this, &all_manifests, topic_rev](
                      const ss::sstring& key,
                      std::chrono::system_clock::time_point,
                      size_t,
                      const ss::sstring&) {
        std::filesystem::path path(key);
        auto res = get_manifest_path_components(path);
        if (
          res.has_value() && same_ntp(*res, _ntpc.ntp())
          && res->_rev >= topic_rev) {
            vlog(_ctxlog.debug, "Found matching manifest path: {}", *res);
            all_manifests.emplace_back(remote_manifest_path(std::move(path)));
        }
        return ss::stop_iteration::no;
    };
    auto res = co_await _remote->list_objects(
      obj_iter, _bucket, std::nullopt, std::nullopt, _rtcnode);
    if (res == download_result::success) {
        co_return all_manifests;
    }
    auto key = manifest.get_manifest_path();
    throw missing_partition_exception(key);
}

static ss::future<ss::output_stream<char>>
open_output_file_stream(const std::filesystem::path& path) {
    auto file = co_await ss::open_file_dma(
      path.native(), ss::open_flags::rw | ss::open_flags::create);
    auto stream = co_await ss::make_file_output_stream(std::move(file));
    co_return std::move(stream);
}

ss::future<> partition_downloader::download_file(
  const remote_segment_path& remote_location,
  const manifest& manifest,
  const partition_downloader::download_part& part) {
    offset_translator otl;
    otl.update(manifest);

    vlog(
      _ctxlog.info,
      "Downloading segment {} -> {}",
      remote_location,
      part.part_prefix.string());

    auto adjusted_path = otl.get_adjusted_segment_name(
      remote_location, _rtcnode)();
    auto localpath = part.part_prefix / adjusted_path.filename();

    if (co_await ss::file_exists(localpath.string())) {
        // we don't need to re-download file if it's already on disk
        // and the size is matching the one in manifest
        auto sz = co_await ss::file_size(localpath.string());
        auto meta = manifest.get(remote_location);
        vassert(
          meta != nullptr,
          "Can't find segment meta in the manifest {}",
          remote_location);
        if (sz == meta->size_bytes) {
            vlog(
              _ctxlog.info,
              "The local file {} is already downloaded and its size matches "
              "the manifest",
              localpath);
            co_return;
        }
        vlog(
          _ctxlog.info,
          "The local file {} is already downloaded but its size doesn't match "
          "the manifest and will be deleted",
          localpath);
        co_await ss::remove_file(localpath.string());
    }

    auto stream = [this, part, remote_location, localpath, &otl](
                    uint64_t len,
                    ss::input_stream<char> in) -> ss::future<uint64_t> {
        vlog(
          _ctxlog.info,
          "Copying s3 path {} to local location {}",
          remote_location,
          localpath.string());
        co_await ss::recursive_touch_directory(part.part_prefix.string());
        auto fs = co_await open_output_file_stream(localpath);
        auto actual_len = co_await otl.copy_stream(
          remote_location, std::move(in), std::move(fs), _rtcnode);
        vlog(
          _ctxlog.debug,
          "Log segment downloaded. {} bytes expected, {} bytes after "
          "pre-processing.",
          len,
          actual_len);
        co_return len;
    };

    auto result = co_await _remote->download_segment(
      _bucket, remote_location, manifest, stream, _rtcnode);

    if (result != download_result::success) {
        // The individual segment might be missing for varios reasons but
        // it shouldn't prevent us from restoring the remaining data
        vlog(_ctxlog.error, "Failed segment download for {}", remote_location);
    }

    co_return;
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
