/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/logger.h"
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

#include <exception>
#include <stdexcept>
#include <string_view>

namespace cloud_storage {

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
  size_t max_cache_size,
  ss::lowres_clock::duration check_period) noexcept
  : _cache_dir(std::move(cache_dir))
  , _max_cache_size(max_cache_size)
  , _check_period(check_period)
  , _cnt(0)
  , _total_cleaned(0) {}

uint64_t cache::get_total_cleaned() { return _total_cleaned; }

ss::future<> cache::clean_up_at_start() {
    gate_guard guard{_gate};
    auto [unused, candidates_for_deletion] = co_await _walker.walk(
      _cache_dir.native());

    for (auto& file_item : candidates_for_deletion) {
        auto filepath_to_remove = file_item.path;
        vassert(
          std::string_view(filepath_to_remove).starts_with(_cache_dir.native()),
          "Tried to clean up {}, which is outside of cache_dir {}.",
          filepath_to_remove,
          _cache_dir.native());

        // delete only tmp files that are left from previous RedPanda run
        if (std::string_view(filepath_to_remove).ends_with(tmp_extension)) {
            try {
                co_await ss::remove_file(filepath_to_remove);
                _total_cleaned += file_item.size;
            } catch (std::exception& e) {
                vlog(
                  cst_log.error,
                  "Cache eviction couldn't delete {}: {}.",
                  filepath_to_remove,
                  e.what());
            }
        }
    }
    vlog(
      cst_log.debug,
      "Clean up at start deleted files of total size {}.",
      _total_cleaned);
}

ss::future<> cache::clean_up_cache() {
    gate_guard guard{_gate};
    auto [curr_cache_size, candidates_for_deletion] = co_await _walker.walk(
      _cache_dir.native());

    if (curr_cache_size >= _max_cache_size) {
        auto size_to_delete
          = curr_cache_size
            - (_max_cache_size * (long double)_cache_size_low_watermark);

        uint64_t deleted_size = 0;
        size_t i_to_delete = 0;
        while (i_to_delete < candidates_for_deletion.size()
               && deleted_size < size_to_delete) {
            auto filename_to_remove = candidates_for_deletion[i_to_delete].path;
            vassert(
              std::string_view(filename_to_remove)
                .starts_with(_cache_dir.native()),
              "Tried to clean up {}, which is outside of cache_dir {}.",
              filename_to_remove,
              _cache_dir.native());

            // skip tmp files since someone may be writing to it
            if (!std::string_view(filename_to_remove)
                   .ends_with(tmp_extension)) {
                try {
                    co_await ss::remove_file(filename_to_remove);
                    deleted_size += candidates_for_deletion[i_to_delete].size;
                } catch (std::exception& e) {
                    vlog(
                      cst_log.error,
                      "Cache eviction couldn't delete {}: {}.",
                      filename_to_remove,
                      e.what());
                }
            }
            i_to_delete++;
        }
        _total_cleaned += deleted_size;
        vlog(
          cst_log.debug,
          "Cache eviction deleted {} files of total size {}.",
          i_to_delete,
          deleted_size);
    }
}

ss::future<> cache::start() {
    vlog(
      cst_log.debug,
      "Starting archival cache service, data directory: {}",
      _cache_dir);
    // TODO: implement more optimal cache eviction
    if (ss::this_shard_id() == 0) {
        co_await clean_up_at_start();

        _timer.set_callback([this] { return clean_up_cache(); });
        _timer.arm_periodic(_check_period);
    }
}

ss::future<> cache::stop() {
    vlog(cst_log.debug, "Stopping archival cache service");
    _timer.cancel();
    co_await _walker.stop();
    co_await _gate.close();
}

ss::future<std::optional<cache_item>>
cache::get(std::filesystem::path key, size_t file_pos) {
    gate_guard guard{_gate};
    vlog(cst_log.debug, "Trying to get {} from archival cache.", key.native());
    ss::file cache_file;
    try {
        /*
         * TODO: update access time of file. Cache eviction uses file access
         * timestamp to delete files from oldest to newest. File access time
         * should be updated every time file is returned by cache, see
         *
         *  https://github.com/vectorizedio/redpanda/issues/2459
         */
        cache_file = co_await ss::open_file_dma(
          (_cache_dir / key).native(), ss::open_flags::ro);
    } catch (std::filesystem::filesystem_error& e) {
        if (e.code() == std::errc::no_such_file_or_directory) {
            co_return std::nullopt;
        } else {
            throw;
        }
    }

    ss::file_input_stream_options options{};
    options.buffer_size = storage::default_segment_readahead_size;
    options.read_ahead = 10;
    auto data_size = co_await cache_file.size();
    auto data_stream = ss::make_file_input_stream(
      cache_file, file_pos, std::move(options));
    co_return std::optional(cache_item{std::move(data_stream), data_size});
}

ss::future<>
cache::put(std::filesystem::path key, ss::input_stream<char>& data) {
    gate_guard guard{_gate};
    vlog(cst_log.debug, "Trying to put {} to archival cache.", key.native());

    _files_in_progress.insert(key);
    auto deferred = ss::defer([this, key] { _files_in_progress.erase(key); });
    auto filename = (_cache_dir / key).filename();
    if (std::string_view(filename.native()).ends_with(tmp_extension)) {
        throw std::invalid_argument(fmt::format(
          "Cache file key {} is ending with tmp extension {}.",
          filename.native(),
          tmp_extension));
    }
    auto dir_path = (_cache_dir / key).remove_filename();
    co_await ss::recursive_touch_directory(dir_path.string());

    // tmp file is used to protect against concurrent writes to the same file.
    // One tmp file is written only once by one thread. tmp file should not be
    // read directly. _cnt is an atomic counter that ensures the uniqueness of
    // names for tmp files within one shard, while shard_id ensures uniqueness
    // across multiple shards.
    auto tmp_filename = std::filesystem::path(ss::format(
      "{}_{}_{}{}",
      filename.native(),
      ss::this_shard_id(),
      (++_cnt),
      tmp_extension));
    auto flags = ss::open_flags::wo | ss::open_flags::create
                 | ss::open_flags::exclusive;

    auto tmp_cache_file = co_await ss::open_file_dma(
      (dir_path / tmp_filename).native(), flags);
    auto out = co_await ss::make_file_output_stream(tmp_cache_file);

    co_await ss::copy(data, out)
      .then([&out]() { return out.flush(); })
      .finally([&out]() { return out.close(); });

    // commit write transaction
    co_await ss::rename_file(
      (dir_path / tmp_filename).native(), (dir_path / filename).native());
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
        co_await ss::remove_file((_cache_dir / key).native());
    } catch (std::filesystem::filesystem_error& e) {
        if (e.code() == std::errc::no_such_file_or_directory) {
            co_return;
        } else {
            throw;
        }
    }
};

} // namespace cloud_storage
