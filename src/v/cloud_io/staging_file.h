/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_io/basic_cache_service_api.h"

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/lowres_clock.hh>

#include <cstdint>
#include <filesystem>
#include <optional>

class iobuf;

namespace cloud_io {

class cache;

struct staging_file_options {
    // Minimum size at which reservations will be incrementally requested when
    // appending.
    size_t reservation_min_chunk_size = 1_MiB;

    // Bytes to reserve up front when creating the staging file. The object
    // count (1) is always reserved; this controls initial byte reservation.
    size_t initial_reservation_bytes = 1_MiB;
};

/// \brief Handle for writing a temporary file to the cache before it's
/// uploaded, and then making it available as a full-fledged, consumable cached
/// file (presumably after uploading).
///
/// Callers must call either commit() or close() before destruction to
/// properly close the output stream. Orphaned .part files are cleaned
/// on cache restart.
class staging_file {
public:
    ~staging_file() noexcept;
    staging_file(staging_file&&) noexcept;
    staging_file& operator=(staging_file&&) noexcept = delete;

    /// Path to the staging file. Before commit() is called, callers may read
    /// this file after writing (e.g. for upload to cloud storage).
    const std::filesystem::path& path() const noexcept;

    /// Key in cloud storage.
    const std::filesystem::path& key() const noexcept;

    /// Append data to the staging file. Reserves cache space as needed before
    /// writing. If a deadline is provided, throws ss::timed_out_error if the
    /// reservation wait exceeds it.
    ss::future<> append(
      iobuf data,
      std::optional<ss::lowres_clock::time_point> deadline = std::nullopt);

    /// Flush buffered data to disk without closing the stream.
    ss::future<> flush();

    /// Number of bytes written so far.
    size_t written() const noexcept;

    /// Close the output stream without committing. Releases all
    /// reservations. Call this instead of commit() on error paths, including
    /// on failure of commit().
    ss::future<> close();

    /// Flush and close the output stream, and atomically commit the
    /// file into the cache.
    ss::future<> commit();

private:
    friend class cache;

    using space_reservation_guard
      = basic_space_reservation_guard<ss::lowres_clock>;

    staging_file(
      cache& cache,
      ss::gate::holder gate_holder,
      std::filesystem::path key,
      std::filesystem::path staging_path,
      ss::output_stream<char> output,
      space_reservation_guard reservation,
      staging_file_options opts) noexcept;

    ss::future<> ensure_reserved(
      size_t total_bytes, std::optional<ss::lowres_clock::time_point> deadline);

    cache* _cache;
    ss::gate::holder _gate_holder;
    std::filesystem::path _key;
    std::filesystem::path _staging_path;
    ss::output_stream<char> _output;
    bool _closed{false};
    // Only nullopt after committing/closing.
    std::optional<space_reservation_guard> _reservation;
    staging_file_options _opts;
    uint64_t _written{0};
    uint64_t _reserved{0};
};

} // namespace cloud_io
