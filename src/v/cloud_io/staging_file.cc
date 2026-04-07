/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_io/staging_file.h"

#include "base/vlog.h"
#include "bytes/iobuf.h"
#include "cloud_io/cache_service.h"
#include "cloud_io/logger.h"

namespace cloud_io {

staging_file::staging_file(staging_file&& o) noexcept
  : _cache(o._cache)
  , _gate_holder(std::move(o._gate_holder))
  , _key(std::move(o._key))
  , _staging_path(std::move(o._staging_path))
  , _output(std::move(o._output))
  , _closed(o._closed)
  , _reservation(std::move(o._reservation))
  , _opts(o._opts)
  , _written(o._written)
  , _reserved(o._reserved) {
    o._closed = true;
}

staging_file::~staging_file() noexcept {
    if (!_closed) {
        vlog(
          log.error,
          "Destructing staging file {} for key {} without closing",
          _staging_path,
          _key);
    }
}

staging_file::staging_file(
  cache& cache,
  ss::gate::holder gate_holder,
  std::filesystem::path key,
  std::filesystem::path staging_path,
  ss::output_stream<char> output,
  space_reservation_guard reservation,
  staging_file_options opts) noexcept
  : _cache(&cache)
  , _gate_holder(std::move(gate_holder))
  , _key(std::move(key))
  , _staging_path(std::move(staging_path))
  , _output(std::move(output))
  , _reservation(std::move(reservation))
  , _opts(opts)
  , _reserved(opts.initial_reservation_bytes) {}

const std::filesystem::path& staging_file::path() const noexcept {
    return _staging_path;
}

const std::filesystem::path& staging_file::key() const noexcept { return _key; }

ss::future<> staging_file::flush() { return _output.flush(); }

size_t staging_file::written() const noexcept { return _written; }

ss::future<> staging_file::append(
  iobuf data, std::optional<ss::lowres_clock::time_point> deadline) {
    auto new_written = _written + data.size_bytes();
    co_await ensure_reserved(new_written, deadline);
    for (auto& frag : data) {
        co_await _output.write(frag.get(), frag.size());
    }
    _written = new_written;
}

ss::future<> staging_file::ensure_reserved(
  size_t total_bytes, std::optional<ss::lowres_clock::time_point> deadline) {
    while (total_bytes > _reserved) {
        auto bytes_to_reserve = std::max(
          total_bytes - _reserved, _opts.reservation_min_chunk_size);
        try {
            auto guard = co_await _cache->reserve_space(
              bytes_to_reserve, /*objects=*/0, deadline);
            _reservation->merge(std::move(guard));
            _reserved += bytes_to_reserve;
        } catch (ss::condition_variable_timed_out&) {
            throw ss::timed_out_error();
        } catch (ss::semaphore_timed_out&) {
            throw ss::timed_out_error();
        } catch (...) {
            throw;
        }
    }
}

ss::future<> staging_file::close() {
    // NOTE: may be closed after we've already set _closed (e.g. if we called
    // commit() and threw in close, in which case don't want to call close the
    // output again (Seastar doesn't like this), but still want to clear the
    // reservation.
    if (!_closed) {
        _closed = true;
        co_await _output.close();
    }
    _reservation.reset();
}

ss::future<> staging_file::commit() {
    co_await _output.flush();
    _closed = true;
    co_await _output.close();
    auto reservation = std::exchange(_reservation, std::nullopt).value();
    co_await _cache->commit_staging_file(
      _key, _staging_path, std::move(reservation));
}

} // namespace cloud_io
