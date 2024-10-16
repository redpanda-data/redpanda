/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "base/seastarx.h"
#include "base/units.h"

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/lowres_clock.hh>

#include <filesystem>
#include <optional>

namespace cloud_io {

inline constexpr size_t default_write_buffer_size = 128_KiB;
inline constexpr unsigned default_write_behind = 10;
inline constexpr size_t default_read_buffer_size = 128_KiB;
inline constexpr unsigned default_read_ahead = 10;

struct [[nodiscard]] cache_item {
    ss::file body;
    size_t size;
};

struct [[nodiscard]] cache_item_stream {
    ss::input_stream<char> body;
    size_t size;
};

enum class [[nodiscard]] cache_element_status {
    available,
    not_available,
    in_progress,
};

std::ostream& operator<<(std::ostream& o, cache_element_status);

template<class Clock>
class basic_cache_service_api;

/// RAII guard for bytes reserved in the cache: constructed prior to a call
/// to cache::put, and may be destroyed afterwards.
template<class Clock>
class basic_space_reservation_guard {
public:
    basic_space_reservation_guard(
      basic_cache_service_api<Clock>& cache,
      uint64_t bytes,
      size_t objects) noexcept
      : _cache(cache)
      , _bytes(bytes)
      , _objects(objects) {}

    basic_space_reservation_guard(const basic_space_reservation_guard&)
      = delete;
    basic_space_reservation_guard() = delete;
    basic_space_reservation_guard(basic_space_reservation_guard&& rhs) noexcept
      : _cache(rhs._cache)
      , _bytes(rhs._bytes)
      , _objects(rhs._objects) {
        rhs._bytes = 0;
        rhs._objects = 0;
    }

    ~basic_space_reservation_guard();

    /// After completing the write operation that this space reservation
    /// protected, indicate how many bytes were really written: this is used to
    /// atomically update cache usage stats to free the reservation and update
    /// the bytes used stats together.
    ///
    /// May only be called once per reservation.
    void wrote_data(uint64_t, size_t);

private:
    basic_cache_service_api<Clock>& _cache;

    // Size acquired at time of reservation
    uint64_t _bytes{0};
    size_t _objects{0};
};

template<class Clock = ss::lowres_clock>
class basic_cache_service_api {
public:
    basic_cache_service_api() = default;
    basic_cache_service_api(const basic_cache_service_api&) = default;
    basic_cache_service_api& operator=(const basic_cache_service_api&)
      = default;
    basic_cache_service_api(basic_cache_service_api&&) = default;
    basic_cache_service_api& operator=(basic_cache_service_api&&) = default;
    virtual ~basic_cache_service_api() = default;

    /// Get cached value as a stream if it exists
    ///
    /// \param key is a cache key
    /// \param read_buffer_size is a read buffer size for the iostream
    /// \param readahead number of pages that can be read asynchronously
    virtual ss::future<std::optional<cache_item_stream>> get(
      std::filesystem::path key,
      ss::io_priority_class io_priority,
      size_t read_buffer_size = default_read_buffer_size,
      unsigned int read_ahead = default_read_ahead)
      = 0;

    /// Add new value to the cache, overwrite if it's already exist
    ///
    /// \param key is a cache key
    /// \param io_priority is an io priority of disk write operation
    /// \param data is an input stream containing data
    /// \param write_buffer_size is a write buffer size for disk write
    /// \param write_behind number of pages that can be written asynchronously
    /// \param reservation caller must have reserved cache space before
    /// proceeding with put
    virtual ss::future<> put(
      std::filesystem::path key,
      ss::input_stream<char>& data,
      basic_space_reservation_guard<Clock>& reservation,
      ss::io_priority_class io_priority,
      size_t write_buffer_size = default_write_buffer_size,
      unsigned int write_behind = default_write_behind)
      = 0;

    /// \brief Checks if the value is cached
    ///
    /// \note returned value \c cache_element_status::in_progress is
    /// shard-local, it indicates that the object is being written by this
    /// shard. If the value is being written by another shard, this function
    /// returns only committed result. The value
    /// \c cache_element_status::in_progress can be used as a hint since ntp are
    /// stored on the same shard most of the time.
    virtual ss::future<cache_element_status>
    is_cached(const std::filesystem::path& key) = 0;

    // Call this before starting a download, to trim the cache if necessary
    // and wait until enough free space is available.
    virtual ss::future<basic_space_reservation_guard<Clock>>
      reserve_space(uint64_t, size_t) = 0;

    // Release capacity acquired via `reserve_space`.  This spawns
    // a background fiber in order to be callable from the guard destructor.
    virtual void reserve_space_release(uint64_t, size_t, uint64_t, size_t) = 0;
};
} // namespace cloud_io
