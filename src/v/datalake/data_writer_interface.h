/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/outcome.h"
#include "datalake/base_types.h"
#include "iceberg/datatypes.h"
#include "iceberg/values.h"

#include <seastar/core/iostream.hh>

#include <cstddef>

namespace datalake {

enum class writer_error {
    ok = 0,
    parquet_conversion_error,
    retryable_type_resolution_error,
    file_io_error,
    no_data,
    flush_error,
    oom_error,
    time_limit_exceeded,
    shutting_down,
    out_of_disk,
    unknown_error,
};
std::ostream& operator<<(std::ostream&, const writer_error&);

// Recoverable errors are the class of errors that donot leave the underlying
// writers in a bad shape. Upon recoverable errors the translator may choose to
// flush and continue as if nothing happened, so we preserve the state to
// facilitate that.
bool is_recoverable_error(datalake::writer_error);

struct data_writer_error_category : std::error_category {
    const char* name() const noexcept final { return "Data Writer Error"; }

    std::string message(int ev) const final;

    static const std::error_category& error_category() {
        static data_writer_error_category e;
        return e;
    }
};

inline std::error_code make_error_code(writer_error e) noexcept {
    return {static_cast<int>(e), data_writer_error_category::error_category()};
}

enum reservation_error {
    ok = 0,
    shutting_down = 1,
    out_of_memory = 2,
    time_quota_exceeded = 3,
    out_of_disk = 4,
    unknown = 5,
};

writer_error map_to_writer_error(reservation_error);

/**
 * Interface to track disk used by parquet writers.
 *
 * disk space tracking considers the combined total of buffered and already
 * flushed data. this is because in the current implementation when a translator
 * uploads and removes data from disk it also flushes all of its bufferred data.
 * if those two actions are decoupled, then this accounting could be updated to
 * provide more flexibility in controlling resource usage.
 */
class writer_disk_tracker {
public:
    writer_disk_tracker() = default;
    writer_disk_tracker(const writer_disk_tracker&) = delete;
    writer_disk_tracker(writer_disk_tracker&&) = default;
    writer_disk_tracker& operator=(const writer_disk_tracker&) = delete;
    writer_disk_tracker& operator=(writer_disk_tracker&&) = delete;

    virtual ~writer_disk_tracker() = default;

    /**
     * Reserves passed input bytes.
     */
    virtual ss::future<reservation_error>
    reserve_bytes(size_t bytes, ss::abort_source&) noexcept = 0;

    /**
     * Frees up passed input bytes.
     *
     * The amount of data on disk isn't being reduced here, but disk accounting
     * is the total of buffered and flushed, and the buffered component may
     * shrink (e.g. due to compression).
     */
    virtual ss::future<> free_bytes(size_t bytes, ss::abort_source&) = 0;

    /**
     * Releases all the reservations. After this caller, the reserved bytes
     * tracked is 0. May not be called concurrently with other methods.
     */
    virtual void release() = 0;

    /**
     * Release unused reservation units. Invoke this if units are not expected
     * be used in the near term. See reservation_tracker::reserve_disk for
     * additional information.
     */
    virtual void release_unused() = 0;
};

/**
 * Interface to track memory used by the parquet writers. The reservations are
 * held until the tracker object is alive or release is explicitly called.
 */
class writer_mem_tracker {
public:
    writer_mem_tracker() = default;
    writer_mem_tracker(const writer_mem_tracker&) = delete;
    writer_mem_tracker(writer_mem_tracker&&) = default;
    writer_mem_tracker& operator=(const writer_mem_tracker&) = delete;
    writer_mem_tracker& operator=(writer_mem_tracker&&) = delete;

    virtual ~writer_mem_tracker() = default;

    /**
     * Reserves passed input bytes.
     */
    virtual ss::future<reservation_error>
    reserve_bytes(size_t bytes, ss::abort_source&) noexcept = 0;

    /**
     * Frees up passed input bytes.
     */
    virtual ss::future<> free_bytes(size_t bytes, ss::abort_source&) = 0;

    /**
     * Releases all the reservations. After this caller, the reserved bytes
     * tracked is 0. May not be called concurrently with other methods.
     */
    virtual void release() = 0;

    /*
     * Return a reference to the disk resource tracker.
     *
     * TODO: instead of plumbing additional resource trackers around in
     * parallel to the writer_mem_tracker, the writer_mem_tracker should be
     * renamed to resource_tracker and expose the writer_mem_tracker like we are
     * doing here for the disk resource.
     */
    virtual writer_disk_tracker& disk() = 0;
};

/**
 * Parquet writer interface. The writer should write parquet serialized data to
 * the output stream provided during its creation.
 */
class parquet_ostream {
public:
    explicit parquet_ostream() = default;
    parquet_ostream(const parquet_ostream&) = delete;
    parquet_ostream(parquet_ostream&&) = default;
    parquet_ostream& operator=(const parquet_ostream&) = delete;
    parquet_ostream& operator=(parquet_ostream&&) = default;
    virtual ~parquet_ostream() = default;

    virtual ss::future<writer_error>
    add_data_struct(iceberg::struct_value, size_t, ss::abort_source&) = 0;

    /**
     * Returns the total bytes buffered in the writer pending flush.
     */
    virtual size_t buffered_bytes() const = 0;
    /**
     * Returns the total bytes flushed to the ostream.
     */
    virtual size_t flushed_bytes() const = 0;
    /**
     * Forces a flush of bytes to ostream. Guarantees that all the buffered
     * memory is released.
     */
    virtual ss::future<> flush() = 0;

    virtual ss::future<writer_error> finish() = 0;
};

class parquet_ostream_factory {
public:
    parquet_ostream_factory() = default;
    parquet_ostream_factory(const parquet_ostream_factory&) = default;
    parquet_ostream_factory(parquet_ostream_factory&&) = delete;
    parquet_ostream_factory&
    operator=(const parquet_ostream_factory&) = default;
    parquet_ostream_factory& operator=(parquet_ostream_factory&&) = delete;

    virtual ~parquet_ostream_factory() = default;

    virtual ss::future<std::unique_ptr<parquet_ostream>> create_writer(
      const iceberg::struct_type&,
      ss::output_stream<char>,
      writer_mem_tracker&) = 0;
};

/**
 * Interface of a parquet file writer. The file writer finishes by returning
 * file metadata. In future we may want to change the return type of this
 * interface to me more generic and allow to express that writer can return
 * either a local file path or a remote path.
 */
class parquet_file_writer {
public:
    parquet_file_writer() = default;
    parquet_file_writer(const parquet_file_writer&) = delete;
    parquet_file_writer(parquet_file_writer&&) = default;
    parquet_file_writer& operator=(const parquet_file_writer&) = delete;
    parquet_file_writer& operator=(parquet_file_writer&&) = delete;

    virtual ~parquet_file_writer() = default;

    virtual ss::future<writer_error> add_data_struct(
      iceberg::struct_value /* data */,
      int64_t /* approx_size */,
      ss::abort_source&) = 0;

    /**
     * Returns the total bytes buffered in the writer pending flush.
     */
    virtual size_t buffered_bytes() const = 0;
    /**
     * Returns the total bytes flushed to the ostream.
     */
    virtual size_t flushed_bytes() const = 0;

    virtual ss::future<writer_error> flush() = 0;

    virtual ss::future<result<local_file_metadata, writer_error>> finish() = 0;
};

class parquet_file_writer_factory {
public:
    parquet_file_writer_factory() = default;
    parquet_file_writer_factory(const parquet_file_writer_factory&) = delete;
    parquet_file_writer_factory(parquet_file_writer_factory&&) = default;
    parquet_file_writer_factory&
    operator=(const parquet_file_writer_factory&) = delete;
    parquet_file_writer_factory&
    operator=(parquet_file_writer_factory&&) = default;
    virtual ~parquet_file_writer_factory() = default;

    virtual ss::future<
      result<std::unique_ptr<parquet_file_writer>, writer_error>>
    create_writer(const iceberg::struct_type& /* schema */, ss::abort_source&)
      = 0;
};

} // namespace datalake

namespace std {
template<>
struct is_error_code_enum<datalake::writer_error> : true_type {};
} // namespace std
