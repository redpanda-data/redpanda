/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iostream.h"
#include "cloud_storage/access_time_tracker.h"
#include "cloud_storage/logger.h"
#include "seastar/util/file.hh"
#include "ssx/future-util.h"
#include "storage/segment.h"
#include "utils/gate_guard.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/defer.hh>

#include <cloud_storage/cache_service.h>

#include <algorithm>
#include <exception>
#include <filesystem>
#include <stdexcept>
#include <string_view>

using namespace std::chrono_literals;

namespace cloud_storage {

static constexpr auto access_timer_period = 60s;
static constexpr const char* access_time_tracker_file_name = "accesstime";
static constexpr const char* access_time_tracker_file_name_tmp
  = "accesstime.tmp";

std::ostream& operator<<(std::ostream& o, cache_element_status s) {
    switch (s) {
    case cache_element_status::available:
        o << "cache_element_available";
        break;
    case cache_element_status::not_available:
        o << "cache_element_not_available";
        break;
    case cache_element_status::in_progress:
        o << "cache_element_in_progress";
        break;
    }
    return o;
}

static constexpr std::string_view tmp_extension{".part"};

cache::cache(
  std::filesystem::path cache_dir,
  size_t disk_size,
  config::binding<double> disk_reservation,
  config::binding<uint64_t> max_bytes_cfg,
  config::binding<std::optional<double>> max_percent,
  config::binding<uint32_t> max_objects) noexcept
  : _cache_dir(std::move(cache_dir))
  , _disk_size(disk_size)
  , _disk_reservation(std::move(disk_reservation))
  , _max_bytes_cfg(std::move(max_bytes_cfg))
  , _max_percent(std::move(max_percent))
  , _max_bytes(_max_bytes_cfg())
  , _max_objects(std::move(max_objects))
  , _cnt(0)
  , _total_cleaned(0) {
    if (ss::this_shard_id() == ss::shard_id{0}) {
        update_max_bytes(); // initialize _max_bytes
        _disk_reservation.watch([this]() { update_max_bytes(); });
        _max_bytes_cfg.watch([this]() { update_max_bytes(); });
        _max_percent.watch([this]() { update_max_bytes(); });
    }
}

void cache::update_max_bytes() {
    // amount of data disk reserved for non-redpanda use
    const uint64_t reservation_size = _disk_size
                                      * (_disk_reservation() / 100.0);

    // unreserved data disk space
    const auto usable_size = _disk_size - reservation_size;

    // percent-based target size
    const uint64_t max_size_pct = usable_size
                                  * (_max_percent().value_or(0) / 100.0);

    auto max_size_bytes = _max_bytes_cfg();
    if (max_size_bytes > usable_size) {
        vlog(
          cst_log.info,
          "Clamping requested max bytes {} to usable size {}",
          max_size_bytes,
          usable_size);
        max_size_bytes = usable_size;
    }

    if (max_size_pct > 0 && max_size_bytes == 0) {
        // only percent target
        _max_bytes = max_size_pct;
    } else if (max_size_pct == 0 && max_size_bytes > 0) {
        // only bytes target
        _max_bytes = max_size_bytes;
    } else {
        _max_bytes = std::min(max_size_pct, max_size_bytes);
    }

    vlog(
      cst_log.info,
      "Cache max_bytes adjusted to {} (current size {}). Disk size {} "
      "reservation {}% max size {} / {}%",
      _max_bytes,
      _current_cache_size,
      _disk_size,
      _disk_reservation(),
      _max_bytes_cfg(),
      _max_percent());

    if (_current_cache_size > _max_bytes) {
        ssx::spawn_with_gate(_gate, [this]() {
            return ss::with_semaphore(
              _cleanup_sm, 1, [this]() { return trim_throttled(); });
        });
    }
}

ss::future<bool>
cache::delete_file_and_empty_parents(const std::string_view& key) {
    gate_guard guard{_gate};

    std::filesystem::path normal_path
      = std::filesystem::path(key).lexically_normal();
    std::filesystem::path normal_cache_dir = _cache_dir.lexically_normal();

    size_t deletions = 0;

    auto [p1, p2] = std::mismatch(
      normal_cache_dir.begin(), normal_cache_dir.end(), normal_path.begin());
    if (p1 != normal_cache_dir.end()) {
        throw std::invalid_argument(fmt_with_ctx(
          fmt::format,
          "Tried to clean up {}, which is outside of cache_dir {}.",
          normal_path.native(),
          normal_cache_dir.native()));
    }

    // Delete the specified file, and iterate through parents
    // attempting to delete them (will delete empty directories,
    // and then drop out when we hit a non-empty directory).
    while (normal_path != normal_cache_dir) {
        try {
            vlog(cst_log.trace, "Removing {}", normal_path);
            co_await ss::remove_file(normal_path.native());
            deletions++;
        } catch (std::filesystem::filesystem_error& e) {
            if (e.code() == std::errc::directory_not_empty) {
                // we stop when we find a non-empty directory
                co_return deletions > 1;
            } else {
                throw;
            }
        }
        normal_path = normal_path.parent_path();
    }

    co_return deletions > 1;
}

uint64_t cache::get_total_cleaned() { return _total_cleaned; }

ss::future<> cache::clean_up_at_start() {
    gate_guard guard{_gate};
    auto [walked_size, filtered_out_files, candidates_for_deletion, empty_dirs]
      = co_await _walker.walk(_cache_dir.native(), _access_time_tracker);

    vassert(
      filtered_out_files == 0,
      "Start-up cache clean-up should not apply filtering");

    // The state of the _access_time_tracker and the actual content of the
    // cache directory might diverge over time (if the user removes segment
    // files manually). We need to take this into account.
    co_await _access_time_tracker.trim(candidates_for_deletion);

    uint64_t deleted_bytes{0};
    size_t deleted_count{0};
    for (const auto& file_item : candidates_for_deletion) {
        auto filepath_to_remove = file_item.path;

        // delete only tmp files that are left from previous RedPanda run
        if (std::string_view(filepath_to_remove).ends_with(tmp_extension)) {
            try {
                co_await delete_file_and_empty_parents(filepath_to_remove);
                deleted_bytes += file_item.size;
                deleted_bytes++;
            } catch (std::exception& e) {
                vlog(
                  cst_log.error,
                  "Startup cache cleanup couldn't delete {}: {}.",
                  filepath_to_remove,
                  e.what());
            }
        }
    }

    for (const auto& path : empty_dirs) {
        try {
            co_await ss::remove_file(path);
        } catch (std::exception& e) {
            // Leaving an empty dir will not prevent progress, so tolerate
            // errors on deletion (could be e.g. a permissions error)
            vlog(
              cst_log.error,
              "Startup cache cleanup couldn't delete {}: {}.",
              path,
              e);
        }
    }

    _total_cleaned = deleted_bytes;
    _current_cache_size = walked_size - deleted_bytes;
    _current_cache_objects = filtered_out_files + candidates_for_deletion.size()
                             - deleted_count;
    probe.set_size(_current_cache_size - deleted_bytes);
    probe.set_num_files(_current_cache_objects);

    vlog(
      cst_log.debug,
      "Clean up at start deleted {} files of total size {}.  Size is now {}/{}",
      deleted_count,
      deleted_bytes,
      _current_cache_size,
      _current_cache_objects);
}

std::optional<std::chrono::milliseconds> cache::get_trim_delay() const {
    auto now = ss::lowres_clock::now();
    std::chrono::milliseconds interval
      = config::shard_local_cfg().cloud_storage_cache_check_interval_ms();
    if (now - _last_clean_up >= interval) {
        return std::nullopt;
    } else {
        auto delta = std::chrono::duration_cast<std::chrono::milliseconds>(
          now - _last_clean_up);
        return interval - delta;
    }
}

ss::future<> cache::trim_throttled() {
    // If we trimmed very recently then do not do it immediately:
    // this reduces load and improves chance of currently promoted
    // segments finishing their read work before we demote their
    // data from cache.
    auto trim_delay = get_trim_delay();

    if (trim_delay.has_value()) {
        vlog(
          cst_log.info,
          "Cache trimming throttled, waiting {}ms",
          std::chrono::duration_cast<std::chrono::milliseconds>(*trim_delay)
            .count());
        co_await ss::sleep_abortable(*trim_delay, _as);
    }

    co_await trim();
}

ss::future<> cache::trim_manually(
  std::optional<uint64_t> size_limit_override,
  std::optional<size_t> object_limit_override) {
    vassert(ss::this_shard_id() == 0, "Method can only be invoked on shard 0");
    auto units = co_await ss::get_units(_cleanup_sm, 1);
    co_return co_await trim(size_limit_override, object_limit_override);
}

ss::future<> cache::trim(
  std::optional<uint64_t> size_limit_override,
  std::optional<size_t> object_limit_override) {
    vassert(ss::this_shard_id() == 0, "Method can only be invoked on shard 0");
    gate_guard guard{_gate};
    auto [walked_cache_size, filtered_out_files, candidates_for_deletion, _]
      = co_await _walker.walk(
        _cache_dir.native(), _access_time_tracker, [](std::string_view path) {
            return !(
              std::string_view(path).ends_with(".tx")
              || std::string_view(path).ends_with(".index"));
        });

    // Updating the access time tracker in case if some files were removed
    // from cache directory by the user manually.
    co_await _access_time_tracker.trim(candidates_for_deletion);

    auto size_limit = size_limit_override.value_or(_max_bytes);
    auto object_limit = object_limit_override.value_or(_max_objects());

    // We aim to trim to within the upper size limit, and additionally
    // free enough space for anyone waiting in `reserve_space` to proceed
    auto target_size = uint64_t(
      (size_limit - std::min(_reservations_pending, size_limit)));

    size_t target_objects = static_cast<size_t>(object_limit)
                            - std::min(
                              _reservations_pending_objects,
                              static_cast<size_t>(object_limit));

    // Apply _cache_size_low_watermark to the size and/or the object count,
    // depending on which is currently the limiting factor for the trim.
    if (_current_cache_objects + _reserved_cache_objects > target_objects) {
        target_objects *= _cache_size_low_watermark;
    }

    if (_current_cache_size + _reserved_cache_size > target_size) {
        target_size *= _cache_size_low_watermark;
    }

    // In the extreme case where even trimming to the low watermark wouldn't
    // free enough space to enable writing to the cache, go even further.
    if (_free_space < config::shard_local_cfg().storage_min_free_bytes()) {
        target_size = std::min(
          target_size,
          _current_cache_size
            - std::min(
              _current_cache_size,
              config::shard_local_cfg().storage_min_free_bytes()));
        vlog(
          cst_log.warn,
          "Critically low space, trimming to {} bytes",
          target_size);
    }

    // Calculate total space used by tmp files: we will use this later
    // when updating current_cache_size.
    uint64_t tmp_files_size{0};
    for (const auto& i : candidates_for_deletion) {
        if (std::string_view(i.path).ends_with(tmp_extension)) {
            tmp_files_size += i.size;
        }
    }

    vlog(
      cst_log.debug,
      "trim: set target_size {}/{}, size {}/{}, walked size {} (max {}/{}), "
      " reserved {}/{}, pending {}/{})",
      target_size,
      target_objects,
      _current_cache_size,
      _current_cache_objects,
      walked_cache_size,
      size_limit,
      object_limit,
      _reserved_cache_size,
      _reserved_cache_objects,
      _reservations_pending,
      _reservations_pending_objects);

    if (
      _current_cache_size + _reserved_cache_size < target_size
      && _current_cache_objects + _reserved_cache_objects < target_objects) {
        // Exit early if we are already within the target
        co_return;
    }

    // Sort by atime for the subsequent LRU trimming loop
    std::sort(
      candidates_for_deletion.begin(),
      candidates_for_deletion.end(),
      [](auto& a, auto& b) { return a.access_time < b.access_time; });

    // Calculate how much to delete
    auto size_to_delete
      = (_current_cache_size + _reserved_cache_size)
        - std::min(target_size, _current_cache_size + _reserved_cache_size);
    auto objects_to_delete
      = _current_cache_objects + _reserved_cache_objects
        - std::min(
          target_objects, _current_cache_objects + _reserved_cache_objects);

    vlog(
      cst_log.debug,
      "trim: removing {}/{} bytes, {}/{} objects ({}% of cache) to reach "
      "target {} (tmp size {})",
      size_to_delete,
      _current_cache_size,
      objects_to_delete,
      _current_cache_objects,
      _current_cache_size > 0 ? (size_to_delete * 100) / _current_cache_size
                              : 0,
      target_size,
      tmp_files_size);

    // Execute the ordinary trim, prioritize removing
    auto fast_result = co_await trim_fast(
      candidates_for_deletion, size_to_delete, objects_to_delete);

    // Subsequent calculations require knowledge of how much data cannot
    // possibly be deleted (because all trims skip it) in order to decide
    // whether the trim worked properly.
    static constexpr size_t undeletable_objects = 1;
    auto undeletable_bytes = (co_await access_time_tracker_size()).value_or(0);

    // We aim to keep current_cache_size continuously up to date, but
    // in case of housekeeping issues, correct it if it apepars to have
    // drifted too far from the result of our directory walk.
    // This is a lower bound that permits current cache size to deviate
    // by the amount of data currently in tmp files, because they may be
    // updated while the walk is happening.
    uint64_t cache_size_lower_bound = walked_cache_size
                                      - fast_result.deleted_size
                                      - tmp_files_size - undeletable_bytes;
    if (_current_cache_size < cache_size_lower_bound) {
        vlog(
          cst_log.debug,
          "Correcting cache size drift ({} -> {})",
          _current_cache_size,
          cache_size_lower_bound);
        _current_cache_size = cache_size_lower_bound;
        _current_cache_objects = filtered_out_files
                                 + candidates_for_deletion.size()
                                 - fast_result.deleted_count;
    }

    const auto cache_entries_before_trim = candidates_for_deletion.size()
                                           + filtered_out_files;

    vlog(
      cst_log.debug,
      "trim: deleted {}/{} files of total size {}.  Undeletable size {}.",
      fast_result.deleted_count,
      cache_entries_before_trim,
      fast_result.deleted_size,
      undeletable_bytes);

    _total_cleaned += fast_result.deleted_size;
    probe.set_size(_current_cache_size);
    probe.set_num_files(cache_entries_before_trim - fast_result.deleted_count);

    size_to_delete -= std::min(fast_result.deleted_size, size_to_delete);
    objects_to_delete -= std::min(fast_result.deleted_count, objects_to_delete);

    // Before we (maybe) proceed to do an exhaustive trim, make sure we're not
    // trying to trim more data than was physically seen while walking the
    // cache.
    size_to_delete = std::min(
      walked_cache_size - fast_result.deleted_size, size_to_delete);
    objects_to_delete = std::min(
      candidates_for_deletion.size() - fast_result.deleted_count,
      objects_to_delete);

    if (
      size_to_delete > undeletable_bytes
      || objects_to_delete > undeletable_objects) {
        vlog(
          cst_log.info,
          "trim: fast trim did not free enough space, executing exhaustive "
          "trim to free {}/{}...",
          size_to_delete,
          objects_to_delete);

        auto exhaustive_result = co_await trim_exhaustive(
          size_to_delete, objects_to_delete);
        size_to_delete -= std::min(
          exhaustive_result.deleted_size, size_to_delete);
        objects_to_delete -= std::min(
          exhaustive_result.deleted_count, objects_to_delete);
        if ((size_to_delete > undeletable_bytes
             || objects_to_delete > undeletable_objects)) {
            vlog(
              cst_log.error,
              "trim: failed to free sufficient space in exhaustive trim, {} "
              "bytes, {} objects still require deletion",
              size_to_delete,
              objects_to_delete);
            probe.failed_trim();
        }
    }

    vlog(
      cst_log.info,
      "trim: post-trim cache size {}/{} (reserved {}/{}, pending {}/{})",
      _current_cache_size,
      _current_cache_objects,
      _reserved_cache_size,
      _reserved_cache_objects,
      _reservations_pending,
      _reserved_cache_objects);

    _last_clean_up = ss::lowres_clock::now();

    // It is up to callers to set this to true if they determine that after
    // trim, we did not free as much space as they needed.
    _last_trim_failed = false;
}

ss::future<cache::trim_result> cache::trim_fast(
  const fragmented_vector<file_list_item>& candidates,
  uint64_t size_to_delete,
  size_t objects_to_delete) {
    probe.fast_trim();

    trim_result result;

    size_t candidate_i = 0;
    while (
      candidate_i < candidates.size()
      && (result.deleted_size < size_to_delete || result.deleted_count < objects_to_delete)) {
        auto& file_stat = candidates[candidate_i++];

        if (is_trim_exempt(file_stat.path)) {
            continue;
        }

        // skip tmp files since someone may be writing to it
        if (std::string_view(file_stat.path).ends_with(tmp_extension)) {
            continue;
        }

        // Doesn't make sense to demote these independent of the segment
        // they refer to: we will clear them out along with the main log
        // segment file if they exist.
        if (
          std::string_view(file_stat.path).ends_with(".tx")
          || std::string_view(file_stat.path).ends_with(".index")) {
            continue;
        }

        try {
            uint64_t this_segment_deleted_bytes{0};

            auto deleted_parents = co_await delete_file_and_empty_parents(
              file_stat.path);
            result.deleted_size += file_stat.size;
            this_segment_deleted_bytes += file_stat.size;
            _current_cache_size -= file_stat.size;
            _current_cache_objects -= 1;
            result.deleted_count += 1;

            // Determine whether we should delete indices along with the
            // object we have just deleted
            std::optional<std::string> tx_file;
            std::optional<std::string> index_file;

            if (std::string_view(file_stat.path).ends_with(".log")) {
                // If this was a legacy whole-segment item, delete the index
                // and tx file along with the segment
                tx_file = fmt::format("{}.tx", file_stat.path);
                index_file = fmt::format("{}.index", file_stat.path);
            } else if (deleted_parents) {
                auto immediate_parent = std::string(
                  std::filesystem::path(file_stat.path).parent_path());
                static constexpr std::string_view chunks_suffix{"_chunks"};
                if (immediate_parent.ends_with(chunks_suffix)) {
                    // We just deleted the last chunk from a _chunks segment
                    // directory.  We may delete the index + tx state for
                    // that segment.
                    auto base_segment_path = immediate_parent.substr(
                      0, immediate_parent.size() - chunks_suffix.size());
                    tx_file = fmt::format("{}.tx", base_segment_path);
                    index_file = fmt::format("{}.index", base_segment_path);
                }
            }

            if (tx_file.has_value()) {
                try {
                    auto sz = co_await ss::file_size(tx_file.value());
                    co_await ss::remove_file(tx_file.value());
                    result.deleted_size += sz;
                    this_segment_deleted_bytes += sz;
                    result.deleted_count += 1;
                    _current_cache_size -= sz;
                    _current_cache_objects -= 1;
                } catch (std::filesystem::filesystem_error& e) {
                    if (e.code() != std::errc::no_such_file_or_directory) {
                        throw;
                    }
                }
            }

            if (index_file.has_value()) {
                try {
                    auto sz = co_await ss::file_size(index_file.value());
                    co_await ss::remove_file(index_file.value());
                    result.deleted_size += sz;
                    this_segment_deleted_bytes += sz;
                    result.deleted_count += 1;
                    _current_cache_size -= sz;
                    _current_cache_objects -= 1;
                } catch (std::filesystem::filesystem_error& e) {
                    if (e.code() != std::errc::no_such_file_or_directory) {
                        throw;
                    }
                }
            }

            // Remove key if possible to make sure there is no resource
            // leak
            _access_time_tracker.remove_timestamp(
              std::string_view(file_stat.path));

            vlog(
              cst_log.trace,
              "trim: reclaimed(fast) {} bytes from {}",
              this_segment_deleted_bytes,
              file_stat.path);
        } catch (const ss::gate_closed_exception&) {
            // We are shutting down, stop iterating and propagate
            throw;
        } catch (const std::exception& e) {
            vlog(
              cst_log.error,
              "trim: couldn't delete {}: {}.",
              file_stat.path,
              e.what());
        }
    }

    co_return result;
}

bool cache::is_trim_exempt(const ss::sstring& path) const {
    if (
      path == (_cache_dir / access_time_tracker_file_name).string()
      || path == (_cache_dir / access_time_tracker_file_name_tmp).string()) {
        return true;
    }

    return false;
}

ss::future<cache::trim_result>
cache::trim_exhaustive(uint64_t size_to_delete, size_t objects_to_delete) {
    probe.exhaustive_trim();
    trim_result result;

    // Enumerate ALL files in the cache (as opposed to trim_fast that strips out
    // indices/tx/tmp files)
    auto [walked_cache_size, _filtered_out, candidates, _]
      = co_await _walker.walk(_cache_dir.native(), _access_time_tracker);

    vlog(
      cst_log.debug,
      "trim: exhaustive trim of {} candidates, walked size {} (cache size "
      "{}/{}), {}/{} to delete",
      candidates.size(),
      walked_cache_size,
      _current_cache_size,
      _current_cache_objects,
      size_to_delete,
      objects_to_delete);

    // Sort by atime
    std::sort(candidates.begin(), candidates.end(), [](auto& a, auto& b) {
        return a.access_time < b.access_time;
    });

    size_t candidate_i = 0;
    while (
      candidate_i < candidates.size()
      && (result.deleted_size < size_to_delete || result.deleted_count < objects_to_delete)) {
        auto& file_stat = candidates[candidate_i++];

        if (is_trim_exempt(file_stat.path)) {
            continue;
        }

        // Unlike the fast trim, we *do not* skip .tmp files.  This is to handle
        // the case where we have some abandoned tmp files, and have hit the
        // exhaustive trim because they are occupying too much space.
        try {
            co_await delete_file_and_empty_parents(file_stat.path);
            _access_time_tracker.remove_timestamp(
              std::string_view(file_stat.path));

            _current_cache_size -= std::min(
              file_stat.size, _current_cache_size);
            _current_cache_objects -= std::min(
              size_t{1}, _current_cache_objects);

            result.deleted_count += 1;
            result.deleted_size += file_stat.size;

            vlog(
              cst_log.trace,
              "trim: reclaimed(exhaustive) {} bytes from {}",
              file_stat.size,
              file_stat.path);
        } catch (const ss::gate_closed_exception&) {
            // We are shutting down, stop iterating and propagate
            throw;
        } catch (const std::exception& e) {
            // This is only a warning, because in exhaustive scan we might hit a
            // .part file and get ENOENT, this is expected behavior
            // occasionally.
            vlog(
              cst_log.warn,
              "trim: couldn't delete {}: {}.",
              file_stat.path,
              e.what());
        }
    }

    co_return result;
}

ss::future<std::optional<uint64_t>> cache::access_time_tracker_size() const {
    auto path = _cache_dir / access_time_tracker_file_name;
    try {
        co_return static_cast<uint64_t>(co_await ss::file_size(path.string()));
    } catch (std::filesystem::filesystem_error& e) {
        if (e.code() == std::errc::no_such_file_or_directory) {
            co_return std::nullopt;
        } else {
            throw;
        }
    }
}

ss::future<> cache::load_access_time_tracker() {
    ss::gate::holder guard{_gate};
    vassert(ss::this_shard_id() == 0, "Method can only be invoked on shard 0");
    auto source = _cache_dir / access_time_tracker_file_name;
    auto present = co_await ss::file_exists(source.native());
    if (!present) {
        vlog(cst_log.info, "Access time tracker doesn't exist at '{}'", source);
        co_return;
    }
    vlog(
      cst_log.info, "Trying to hydrate access time tracker from '{}'", source);

    ss::file_open_options open_opts;

    ss::file_input_stream_options input_opts{};
    input_opts.buffer_size = config::shard_local_cfg().storage_read_buffer_size;
    input_opts.read_ahead
      = config::shard_local_cfg().storage_read_readahead_count;
    input_opts.io_priority_class
      = priority_manager::local().shadow_indexing_priority();

    auto exists = co_await ss::file_exists(source.string());
    if (exists) {
        try {
            co_await ss::util::with_file_input_stream(
              source,
              [this](ss::input_stream<char>& in) {
                  return _access_time_tracker.read(in);
              },
              open_opts,
              input_opts);
        } catch (...) {
            vlog(
              cst_log.warn,
              "Failed to materialize access time tracker '{}'. Error: {}",
              source,
              std::current_exception());
        }
    } else {
        vlog(
          cst_log.info, "Access time tracker is not available at '{}'", source);
    }
}

/**
 * Inner part of save_access_time_tracker, to be called with a file
 * that the caller will close for us after we return.
 */
ss::future<> cache::_save_access_time_tracker(ss::file f) {
    auto out = co_await ss::make_file_output_stream(std::move(f));
    co_await _access_time_tracker.write(out);
    co_await out.flush();
}

ss::future<> cache::save_access_time_tracker() {
    ss::gate::holder guard{_gate};
    vassert(ss::this_shard_id() == 0, "Method can only be invoked on shard 0");
    auto tmp_path = _cache_dir / access_time_tracker_file_name_tmp;

    ss::file_open_options open_opts;
    co_await ss::with_file(
      ss::open_file_dma(
        tmp_path.string(),
        ss::open_flags::create | ss::open_flags::wo,
        open_opts),
      [this](ss::file f) -> ss::future<> {
          return _save_access_time_tracker(std::move(f));
      });

    auto final_path = _cache_dir / access_time_tracker_file_name;
    co_await ss::rename_file(tmp_path.string(), final_path.string());
}

ss::future<> cache::maybe_save_access_time_tracker() {
    vassert(ss::this_shard_id() == 0, "Method can only be invoked on shard 0");
    if (_access_time_tracker.is_dirty() && !_gate.is_closed()) {
        co_await save_access_time_tracker();
    }
}

ss::future<> cache::start() {
    vlog(
      cst_log.debug,
      "Starting archival cache service, data directory: {}",
      _cache_dir);

    if (ss::this_shard_id() == 0) {
        // access time tracker has to be initialized before
        // cleanup
        co_await load_access_time_tracker();
        co_await clean_up_at_start();

        _tracker_timer.set_callback([this] {
            ssx::spawn_with_gate(_gate, [this] {
                return maybe_save_access_time_tracker().handle_exception(
                  [](auto eptr) {
                      vlog(
                        cst_log.error,
                        "failed to save access time tracker: {}",
                        eptr);
                  });
            });
        });
        _tracker_timer.arm_periodic(access_timer_period);
    }
}

ss::future<> cache::stop() {
    vlog(cst_log.debug, "Stopping archival cache service");
    _tracker_timer.cancel();
    _as.request_abort();
    _block_puts_cond.broken();
    _cleanup_sm.broken();
    if (ss::this_shard_id() == 0) {
        co_await save_access_time_tracker();
    }
    co_await _walker.stop();
    co_await _gate.close();
}

ss::future<std::optional<cache_item>> cache::get(std::filesystem::path key) {
    gate_guard guard{_gate};
    vlog(cst_log.debug, "Trying to get {} from archival cache.", key.native());
    probe.get();
    ss::file cache_file;
    try {
        auto source = (_cache_dir / key).native();
        cache_file = co_await ss::open_file_dma(source, ss::open_flags::ro);

        // Bump access time of the file
        if (ss::this_shard_id() == 0) {
            _access_time_tracker.add_timestamp(
              source, std::chrono::system_clock::now());
        } else {
            ssx::spawn_with_gate(_gate, [this, source] {
                return container().invoke_on(0, [source](cache& c) {
                    c._access_time_tracker.add_timestamp(
                      source, std::chrono::system_clock::now());
                });
            });
        }
    } catch (std::filesystem::filesystem_error& e) {
        if (e.code() == std::errc::no_such_file_or_directory) {
            probe.miss_get();
            co_return std::nullopt;
        } else {
            throw;
        }
    }

    auto data_size = co_await cache_file.size();
    probe.cached_get();
    co_return std::optional(cache_item{std::move(cache_file), data_size});
}

ss::future<> cache::put(
  std::filesystem::path key,
  ss::input_stream<char>& data,
  space_reservation_guard& reservation,
  ss::io_priority_class io_priority,
  size_t write_buffer_size,
  unsigned int write_behind) {
    gate_guard guard{_gate};
    vlog(cst_log.debug, "Trying to put {} to archival cache.", key.native());
    probe.put();

    std::filesystem::path normal_cache_dir = _cache_dir.lexically_normal();
    std::filesystem::path normal_key_path
      = std::filesystem::path(normal_cache_dir / key).lexically_normal();

    auto [p1, p2] = std::mismatch(
      normal_cache_dir.begin(),
      normal_cache_dir.end(),
      normal_key_path.begin());
    if (p1 != normal_cache_dir.end()) {
        throw std::invalid_argument(fmt_with_ctx(
          fmt::format,
          "Tried to put {}, which is outside of cache_dir {}.",
          normal_key_path.native(),
          normal_cache_dir.native()));
    }

    _files_in_progress.insert(key);
    probe.put_started();
    auto deferred = ss::defer([this, key] {
        _files_in_progress.erase(key);
        probe.put_ended();
    });
    auto filename = normal_key_path.filename();
    if (std::string_view(filename.native()).ends_with(tmp_extension)) {
        throw std::invalid_argument(fmt::format(
          "Cache file key {} is ending with tmp extension {}.",
          normal_key_path.native(),
          tmp_extension));
    }
    auto dir_path = normal_key_path.remove_filename();

    // tmp file is used to protect against concurrent writes to the same
    // file. One tmp file is written only once by one thread. tmp file
    // should not be read directly. _cnt is an atomic counter that
    // ensures the uniqueness of names for tmp files within one shard,
    // while shard_id ensures uniqueness across multiple shards.
    auto tmp_filename = std::filesystem::path(ss::format(
      "{}_{}_{}{}",
      filename.native(),
      ss::this_shard_id(),
      (++_cnt),
      tmp_extension));

    ss::file tmp_cache_file;
    while (true) {
        try {
            // delete_file_and_empty_parents may delete dir_path before
            // we open file, in this case we recreate dir_path and try again
            if (!co_await ss::file_exists(dir_path.string())) {
                co_await ss::recursive_touch_directory(dir_path.string());
            }

            auto flags = ss::open_flags::wo | ss::open_flags::create
                         | ss::open_flags::exclusive;

            tmp_cache_file = co_await ss::open_file_dma(
              (dir_path / tmp_filename).native(), flags);
            break;
        } catch (std::filesystem::filesystem_error& e) {
            if (e.code() == std::errc::no_such_file_or_directory) {
                vlog(
                  cst_log.debug,
                  "Couldn't open {}, gonna retry",
                  (dir_path / tmp_filename).native());
            } else {
                throw;
            }
        }
    }

    ss::file_output_stream_options options{};
    options.buffer_size = write_buffer_size;
    options.write_behind = write_behind;
    options.io_priority_class = io_priority;
    auto out = co_await ss::make_file_output_stream(tmp_cache_file, options);

    std::exception_ptr disk_full_error;
    try {
        co_await ss::copy(data, out)
          .then([&out]() { return out.flush(); })
          .finally([&out]() { return out.close(); });
    } catch (std::filesystem::filesystem_error& e) {
        // For ENOSPC errors, delay handling so that we can do a trim
        if (e.code() == std::errc::no_space_on_device) {
            disk_full_error = std::current_exception();
        } else {
            throw;
        }
    }

    if (disk_full_error) {
        vlog(cst_log.error, "Out of space while writing to cache");

        // Block further puts from being attempted until notify_disk_status
        // reports that there is space available.
        set_block_puts(true);

        // Trim proactively: if many fibers hit this concurrently,
        // they'll contend for cleanup_sm and the losers will skip
        // trim due to throttling.
        {
            auto units = co_await ss::get_units(_cleanup_sm, 1);
            co_await trim_throttled();
        }

        throw disk_full_error;
    }

    // commit write transaction
    auto src = (dir_path / tmp_filename).native();
    auto dest = (dir_path / filename).native();

    auto put_size = co_await ss::file_size(src);

    co_await ss::rename_file(src, dest);

    // We will now update
    reservation.wrote_data(put_size, 1);
}

ss::future<cache_element_status>
cache::is_cached(const std::filesystem::path& key) {
    gate_guard guard{_gate};
    vlog(cst_log.debug, "Checking {} in archival cache.", key.native());
    if (_files_in_progress.contains(key)) {
        return seastar::make_ready_future<cache_element_status>(
          cache_element_status::in_progress);
    } else {
        return ss::file_exists((_cache_dir / key).native())
          .then([](bool exists) {
              return exists ? cache_element_status::available
                            : cache_element_status::not_available;
          });
    }
}

ss::future<> cache::invalidate(const std::filesystem::path& key) {
    gate_guard guard{_gate};
    vlog(
      cst_log.debug,
      "Trying to invalidate {} from archival cache.",
      key.native());
    try {
        auto path = (_cache_dir / key).native();
        auto stat = co_await ss::file_stat(path);
        _access_time_tracker.remove_timestamp(key.native());
        co_await delete_file_and_empty_parents(path);
        _current_cache_size -= stat.size;
        _current_cache_objects -= 1;
        probe.set_size(_current_cache_size);
        probe.set_num_files(_current_cache_objects);
    } catch (std::filesystem::filesystem_error& e) {
        if (e.code() == std::errc::no_such_file_or_directory) {
            vlog(
              cst_log.debug,
              "Could not invalidate {} from archival cache: {}",
              key.native(),
              e.what());
            co_return;
        } else {
            throw;
        }
    }
};

ss::future<space_reservation_guard>
cache::reserve_space(uint64_t bytes, size_t objects) {
    while (_block_puts) {
        vlog(
          cst_log.warn,
          "Blocking tiered storage cache write, disk space critically low.");
        co_await _block_puts_cond.wait();
    }

    co_await container().invoke_on(0, [bytes, objects](cache& c) {
        return c.do_reserve_space(bytes, objects);
    });

    vlog(
      cst_log.trace,
      "reserve_space: reserved {}/{} bytes/objects",
      bytes,
      objects);

    co_return space_reservation_guard(*this, bytes, objects);
}

void cache::reserve_space_release(
  uint64_t bytes,
  size_t objects,
  uint64_t wrote_bytes,
  uint64_t wrote_objects) {
    vlog(
      cst_log.trace,
      "reserve_space_release: releasing {}/{} reserved bytes/objects (wrote "
      "{}/{})",
      bytes,
      objects,
      wrote_bytes,
      wrote_objects);

    if (ss::this_shard_id() == ss::shard_id{0}) {
        do_reserve_space_release(bytes, objects, wrote_bytes, wrote_objects);
    } else {
        ssx::spawn_with_gate(
          _gate, [this, bytes, objects, wrote_bytes, wrote_objects]() {
              return container().invoke_on(
                0, [bytes, objects, wrote_bytes, wrote_objects](cache& c) {
                    return c.do_reserve_space_release(
                      bytes, objects, wrote_bytes, wrote_objects);
                });
          });
    }
}

void cache::do_reserve_space_release(
  uint64_t bytes, size_t objects, uint64_t wrote_bytes, size_t wrote_objects) {
    vassert(ss::this_shard_id() == ss::shard_id{0}, "Only call on shard 0");
    vassert(_reserved_cache_size >= bytes, "Double free of reserved bytes?");
    _reserved_cache_size -= bytes;
    _reserved_cache_objects -= objects;

    _current_cache_size += wrote_bytes;
    _current_cache_objects += wrote_objects;
    probe.set_size(_current_cache_size);
    probe.set_num_files(_current_cache_objects);
    if (
      _current_cache_size > _max_bytes
      || _current_cache_objects > _max_objects()) {
        // This should not happen, because callers to put() should have used
        // reserve_space() to ensure they stay within the cache size limit. This
        // is not a fatal error in itself, so we do not assert, but emitting as
        // an ERROR log ensures detection if we hit this path in our integration
        // tests.
        vlog(
          cst_log.error,
          "Exceeded cache size limit!  (size={}/{} reserved={}/{} "
          "pending={}/{} max={}/{})",
          _current_cache_size,
          _current_cache_objects,
          _reserved_cache_size,
          _reserved_cache_objects,
          _reservations_pending,
          _reservations_pending_objects,
          _max_bytes,
          _max_objects());
    }
}

bool cache::may_reserve_space(uint64_t bytes, size_t objects) {
    auto bytes_available = _current_cache_size + _reserved_cache_size + bytes
                           <= _max_bytes;
    auto objects_available = _current_cache_objects + _reserved_cache_objects
                               + objects
                             <= _max_objects();

    return bytes_available && objects_available;
}

bool cache::may_exceed_limits(uint64_t bytes, size_t objects) {
    // We may overshoot the configured *bytes* limit if there is plenty of
    // disk space available.  However, we may never overshoot the configured
    // object count limit, because this exists to control size of metadata and
    // keep trim runtime bounded.
    //
    // Conditions:
    // - puts must not be blocked (i.e. disk is not critically low)
    // - allowing the put must not consume more than 10% of free space
    // - allowing the put must not violate the object count limit
    // - the put must be blocked by actual cache space unavailable, not just
    //   other reservations.

    auto would_fit_in_cache = _current_cache_size + bytes <= _max_bytes;

    return !_block_puts && _free_space > bytes * 10
           && _current_cache_objects + _reserved_cache_objects + objects
                < _max_objects()
           && !would_fit_in_cache;
}

ss::future<> cache::do_reserve_space(uint64_t bytes, size_t objects) {
    vassert(ss::this_shard_id() == ss::shard_id{0}, "Only call on shard 0");

    if (may_reserve_space(bytes, objects)) {
        // Fast path: space was available.
        _reserved_cache_size += bytes;
        _reserved_cache_objects += objects;
        co_return;
    }

    // Slow path: register a pending need for bytes that will be used in
    // clean_up_cache to make space available, and then proceed to cooperatively
    // call clean_up_cache along with anyone else who is waiting.
    _reservations_pending += bytes;
    _reservations_pending_objects += objects;

    vlog(
      cst_log.trace,
      "Out of space reserving {} bytes (size={}/{} "
      "reserved={}/{} pending={}/{}): proceeding to maybe trim",
      bytes,
      _current_cache_size,
      _current_cache_objects,
      _reserved_cache_size,
      _reserved_cache_objects,
      _reservations_pending,
      _reservations_pending_objects);

    try {
        auto units = co_await ss::get_units(_cleanup_sm, 1);
        while (!may_reserve_space(bytes, objects)) {
            bool may_exceed = may_exceed_limits(bytes, objects)
                              && _last_trim_failed;
            bool may_trim_now = !get_trim_delay().has_value();

            // We will attempt trimming (including waiting for trim throttle)
            // if:
            //  - may_trim_now: we haven't trimmed recently
            //  - !may_exceed: we absolutely must trim before reserving the
            //  space
            //
            // The purpose of this logic is to enforce blocking on trim when
            // space is low, but enable reservations to proceed without throttle
            // delay if trimming is failing to clear enough space but the disk
            // free space is plentiful.
            bool did_trim = false;
            if (may_trim_now || !may_exceed) {
                // After taking lock, there still isn't space: means someone
                // else didn't take it and free space for us already, so we will
                // do the trim.
                co_await trim_throttled();
                did_trim = true;
            }

            if (!may_reserve_space(bytes, objects)) {
                if (did_trim) {
                    // Recalculate: things may have changed significantly during
                    // trim and/or trim throttle delay
                    may_exceed = may_exceed_limits(bytes, objects);
                    if (may_exceed) {
                        // Tip off the next caller that they may proactively
                        // exceed the cache size without waiting for a trim.
                        _last_trim_failed = true;
                    }
                }
                // Even after trimming, the reservation cannot be accommodated.
                // This is unexpected: either something is wrong with the trim,
                // or the requested bytes cannot fit into the max cache size.
                // In this case, we log a warning and exceed the configured
                // cache size limit, because the alternative would be to
                // stall entirely.
                vlog(
                  cst_log.warn,
                  "Failed to trim cache enough to reserve {}/{} bytes "
                  "(size={}/{} "
                  "reserved={}/{} pending={}/{})",
                  bytes,
                  objects,
                  _current_cache_size,
                  _current_cache_objects,
                  _reserved_cache_size,
                  _reserved_cache_objects,
                  _reservations_pending,
                  _reservations_pending_objects);

                // If there is a lot of free space on the disk, and we already
                // tried our best to trim, then we may exceed the cache size
                // limit. Approximate "a lot" of free disk space as 10x the size
                // of what we're trying to promote.
                if (may_exceed) {
                    vlog(
                      cst_log.info,
                      "Intentionally exceeding cache size limit {},"
                      "there are {} bytes of free space on the cache disk",
                      _max_bytes,
                      _free_space);

                    // Deduct the amount by which we're about to violate the
                    // cache allowance, in order to avoid many reservations
                    // all skipping the cache limit based on the same apparent
                    // free bytes.  This counter will get reset to ground
                    // truth the next time we get a disk status notification.
                    _free_space -= bytes;
                    break;
                } else {
                    // No allowance, and the disk does not have a lot of
                    // slack free space: we must wait.
                    vlog(
                      cst_log.debug,
                      "Could not reserve {} bytes, waiting",
                      bytes);

                    // No explicit sleep needed: we will proceed around the
                    // loop into trim_throttled, and sleep waiting for the
                    // throttle period to expire.
                }
            }
        }
    } catch (...) {
        _reservations_pending -= bytes;
        _reservations_pending_objects -= objects;
        throw;
    }

    _reservations_pending -= bytes;
    _reservations_pending_objects -= objects;
    _reserved_cache_size += bytes;
    _reserved_cache_objects += objects;
}

void cache::set_block_puts(bool block_puts) {
    if (_block_puts && !block_puts) {
        _block_puts_cond.signal();
    }
    _block_puts = block_puts;
}

void cache::notify_disk_status(
  [[maybe_unused]] uint64_t total_space,
  [[maybe_unused]] uint64_t free_space,
  storage::disk_space_alert alert) {
    vassert(ss::this_shard_id() == 0, "Called on wrong shard");

    _free_space = free_space;

    bool block_puts = (alert == storage::disk_space_alert::degraded);

    if (block_puts != _block_puts) {
        if (block_puts) {
            // Start blocking
            vlog(
              cst_log.warn,
              "Tiered storage cache blocking segment promotions, disk space is "
              "critically low.");
        } else {
            // Stop blocking
            vlog(
              cst_log.info,
              "Tiered storage cache un-blocking promotions, disk space is no "
              "longer critical.");
        }

        ssx::spawn_with_gate(_gate, [this, block_puts]() {
            return container().invoke_on_all(
              [block_puts](cache& c) { c.set_block_puts(block_puts); });
        });
    }
}

void space_reservation_guard::wrote_data(
  uint64_t written_bytes, size_t written_objects) {
    // Release the reservation, and update usage stats for how much we actually
    // wrote.
    _cache.reserve_space_release(
      _bytes, _objects, written_bytes, written_objects);

    // This reservation is now used up.
    _bytes = 0;
    _objects = 0;
}

space_reservation_guard::~space_reservation_guard() {
    if (_bytes || _objects) {
        // This is the case of a failed write, where wrote_data was never
        // called: release the reservation and do not acquire any space
        // usage for the written data (there should be none).
        _cache.reserve_space_release(_bytes, _objects, 0, 0);
    }
}

ss::future<> cache::initialize(std::filesystem::path cache_dir) {
    // Create this up-front, we will need it even if cache is
    // never used, e.g. when saving access time tracker.
    if (!co_await ss::file_exists(cache_dir.string())) {
        vlog(cst_log.info, "Creating cache directory {}", cache_dir);
        co_await ss::recursive_touch_directory(cache_dir.string());
    }
}

} // namespace cloud_storage
