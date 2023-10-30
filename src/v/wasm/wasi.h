/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

// These APIs are defined and documented in the wasi-libc project,
// but Redpanda implements it's own version of the WASI standard,
// so that it works with the model we want to expose and have full
// control over all the details. Most APIs here are stubbed out as
// unsupported.
//
// Supported functionality:
// - Program args + environment variables
// - Manual clock implementation (with millisecond resolution)
// - Reading from "random" (it's actually deterministic)
// - exitting the process (just forces a trap within the VM)
// - yield (yields to the seastar scheduler)
// - "sleeping" via poll_oneoff (needed for golang support)
//
// For the full spec in a C API see:
// https://github.com/WebAssembly/wasi-libc/blob/main/libc-bottom-half/headers/public/wasi/api.h

#pragma once

#include "model/timestamp.h"
#include "utils/named_type.h"
#include "wasm/ffi.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/noncopyable_function.hh>

#include <absl/container/flat_hash_map.h>

#include <chrono>
#include <string_view>
#include <vector>

namespace wasm::wasi {

/**
 * An exit code to show that the VM has exited.
 */
class exit_exception : public std::exception {
public:
    explicit exit_exception(int32_t exit_code);
    const char* what() const noexcept override;
    int32_t code() const noexcept;

private:
    int32_t _code;
    ss::sstring _msg;
};

/**
 * This is the function that the wasi standard expects to be exported to call
 * into the user's main function.
 */
constexpr std::string_view preview_1_start_function_name = "_start";

// Identifiers for errors.
using errno_t = named_type<uint16_t, struct errc_tag>;
constexpr errno_t ERRNO_SUCCESS{0};
constexpr errno_t ERRNO_INVAL{16};
constexpr errno_t ERRNO_NOSYS{52};
constexpr errno_t ERRNO_BADF{8};

// Identifiers for clocks.
using clock_id_t = named_type<uint32_t, struct clock_id_tag>;
constexpr clock_id_t REALTIME_CLOCK_ID{0};
constexpr clock_id_t MONOTONIC_CLOCK_ID{1};
constexpr clock_id_t PROCESS_CPUTIME_CLOCK_ID{2};
constexpr clock_id_t THREAD_CPUTIME_CLOCK_ID{3};

// A timestamp in nanoseconds
using timestamp_t = named_type<uint64_t, struct timestamp_tag>;
// A file descriptor
using fd_t = named_type<int32_t, struct fd_tag>;

// Type of a subscription to an event or its occurrence.
using event_type_t = named_type<uint8_t, struct eventtype_tag>;
/**
 * The time value of clock `subscription_clock::id` has
 * reached timestamp `subscription_clock::timeout`.
 */
constexpr event_type_t CLOCK_EVENT_TYPE{0};
/**
 * File descriptor `subscription_fd_readwrite::file_descriptor` has data
 * available for reading. This event always triggers for regular files.
 */
constexpr event_type_t FD_READ_EVENT_TYPE{1};
/**
 * File descriptor `subscription_fd_readwrite::file_descriptor` has capacity
 * available for writing. This event always triggers for regular files.
 */
constexpr event_type_t FD_WRITE_EVENT_TYPE{2};

// Arbitrary user data for subscriptions
using user_data_t = named_type<int64_t, struct user_data_tag>;

// Specify if a sleep time is relative or absolute
using subclockflags_t = named_type<int16_t, struct subclockflags_tag>;
/**
 * If set, treat the timestamp provided in
 * `subscription_clock::timeout` as an absolute timestamp of clock
 * `subscription_clock::id`. If clear, treat the timestamp
 * provided in `subscription_clock::timeout` relative to the
 * current time value of clock `subscription_clock::id`.
 */
constexpr subclockflags_t RELATIVE_TIME_SUBCLOCK_FLAG{0};
constexpr subclockflags_t ABSOLUTE_TIME_SUBCLOCK_FLAG{1};

/**
 * The contents of a `subscription` when type is `eventtype::clock`.
 */
struct subscription_clock_t {
    /**
     * The clock against which to compare the timestamp.
     */
    clock_id_t id;

    /**
     * The absolute or relative timestamp.
     */
    timestamp_t timeout;

    /**
     * The amount of time that the implementation may wait additionally
     * to coalesce with other events.
     */
    timestamp_t precision;

    /**
     * Flags specifying whether the timeout is absolute or relative
     */
    subclockflags_t flags;
};

/**
 * The contents of a `subscription` when type is type is
 * `eventtype::fd_read` or `eventtype::fd_write`.
 */
struct subscription_fd_readwrite_t {
    /**
     * The file descriptor on which to wait for it to become ready for reading
     * or writing.
     */
    fd_t file_descriptor;
};

/**
 * The contents of a `subscription`.
 */
union subscription_union_options {
    subscription_clock_t clock;
    subscription_fd_readwrite_t fd_read;
    subscription_fd_readwrite_t fd_write;
};
struct subscription_tagged_union {
    event_type_t tag;
    subscription_union_options u;
};
/**
 * Subscription to an event.
 */
struct subscription_t {
    /**
     * User-provided value that is attached to the subscription in the
     * implementation and returned through `event::userdata`.
     */
    user_data_t userdata;

    /**
     * The type of the event to which to subscribe, and its contents
     */
    subscription_tagged_union u;
};

/**
 * Non-negative file size or length of a region within a file.
 */
using filesize_t = named_type<uint64_t, struct filesize_tag>;
/**
 * The state of the file descriptor subscribed to with
 * `eventtype::fd_read` or `eventtype::fd_write`.
 */
using event_rw_flags_t = named_type<int16_t, struct eventrwflags_tag>;

/**
 * The contents of an `event` when type is `eventtype::fd_read` or
 * `eventtype::fd_write`.
 */
struct event_fd_readwrite_t {
    /**
     * The number of bytes available for reading or writing.
     */
    filesize_t nbytes;

    /**
     * The state of the file descriptor.
     */
    event_rw_flags_t flags;
};

/**
 * An event that occurred.
 */
struct event_t {
    /**
     * User-provided value that got attached to `subscription::userdata`.
     */
    user_data_t userdata;

    /**
     * If non-zero, an error that occurred while processing the subscription
     * request.
     */
    errno_t error;

    /**
     * The type of event that occured
     */
    event_type_t type;

    /**
     * The contents of the event, if it is an `eventtype::fd_read` or
     * `eventtype::fd_write`. `eventtype::clock` events ignore this field.
     */
    event_fd_readwrite_t fd_readwrite;
};

/**
 * A region of memory for scatter/gather writes.
 */
struct iovec_t {
    /** The (guest) address of the buffer to be written. */
    ffi::ptr buf_addr;
    /** The length of the buffer to be written. */
    uint32_t buf_len;
};

using environ_map_t = absl::flat_hash_map<ss::sstring, ss::sstring>;

/**
 * A converter from stdout/stderr in WASI into stderr on the broker.
 */
class log_writer {
public:
    static log_writer make_for_stderr(ss::sstring name, ss::logger*);
    static log_writer make_for_stdout(ss::sstring name, ss::logger*);
    // Writes this string to the logger, adding a prefix
    //
    // Does not make a copy of the string, so leftovers must be flushed
    uint32_t write(std::string_view);
    // Flush remaining logs
    uint32_t flush();

private:
    explicit log_writer(ss::sstring name, bool is_guest_stdout, ss::logger*);

    bool _is_guest_stdout;
    ss::sstring _name;

    ss::logger* _logger;
    std::vector<std::string_view> _buffer;
};

/**
 * Implementation of the wasi preview1 which is a snapshot of the wasi spec from
 * 2020.
 */
class preview1_module {
public:
    // Create a wasi module using the args and environ to initialize the runtime
    // with.
    preview1_module(
      std::vector<ss::sstring>, const environ_map_t&, ss::logger*);
    preview1_module(const preview1_module&) = delete;
    preview1_module& operator=(const preview1_module&) = delete;
    preview1_module(preview1_module&&) = default;
    preview1_module& operator=(preview1_module&&) = default;
    ~preview1_module() = default;

    // Set the current timestamp that clocks will return.
    void set_timestamp(model::timestamp);

    static constexpr std::string_view name = "wasi_snapshot_preview1";

    errno_t clock_res_get(clock_id_t, timestamp_t*);
    errno_t clock_time_get(clock_id_t, timestamp_t, timestamp_t*);
    errno_t args_sizes_get(uint32_t*, uint32_t*);
    errno_t args_get(ffi::memory*, ffi::ptr, ffi::ptr);
    errno_t environ_get(ffi::memory*, ffi::ptr, ffi::ptr);
    errno_t environ_sizes_get(uint32_t*, uint32_t*);
    errno_t fd_advise(fd_t, uint64_t, uint64_t, uint8_t);
    errno_t fd_allocate(fd_t, uint64_t, uint64_t);
    errno_t fd_close(fd_t);
    errno_t fd_datasync(fd_t);
    errno_t fd_fdstat_get(fd_t, void*);
    errno_t fd_fdstat_set_flags(fd_t, uint16_t);
    errno_t fd_filestat_get(fd_t, void*);
    errno_t fd_filestat_set_size(fd_t, uint64_t);
    errno_t fd_filestat_set_times(fd_t, timestamp_t, timestamp_t, uint16_t);
    errno_t fd_pread(fd_t, ffi::array<iovec_t>, uint64_t, uint32_t*);
    errno_t fd_prestat_get(fd_t, void*);
    errno_t fd_prestat_dir_name(fd_t, uint8_t*, uint32_t);
    errno_t fd_pwrite(fd_t, ffi::array<iovec_t>, uint64_t, uint32_t*);
    errno_t fd_read(fd_t, ffi::array<iovec_t>, uint32_t*);
    errno_t fd_readdir(fd_t, ffi::array<uint8_t>, uint64_t, uint32_t*);
    errno_t fd_renumber(fd_t, fd_t);
    errno_t fd_seek(fd_t, int64_t, uint8_t, uint64_t*);
    errno_t fd_sync(fd_t);
    errno_t fd_tell(fd_t, uint64_t*);
    errno_t fd_write(ffi::memory*, fd_t, ffi::array<iovec_t>, uint32_t*);
    errno_t path_create_directory(fd_t, ffi::array<uint8_t>);
    errno_t path_filestat_get(fd_t, uint32_t, ffi::array<uint8_t>, void*);
    errno_t path_filestat_set_times(
      fd_t, uint32_t, ffi::array<uint8_t>, timestamp_t, timestamp_t, uint16_t);
    errno_t
      path_link(fd_t, uint32_t, ffi::array<uint8_t>, fd_t, ffi::array<uint8_t>);
    errno_t path_open(
      fd_t,
      uint32_t,
      ffi::array<uint8_t>,
      uint16_t,
      uint64_t,
      uint64_t,
      uint16_t,
      fd_t*);
    errno_t path_readlink(
      fd_t, ffi::array<uint8_t> path, ffi::array<uint8_t>, uint32_t*);
    errno_t path_remove_directory(fd_t, ffi::array<uint8_t>);
    errno_t path_rename(fd_t, ffi::array<uint8_t>, fd_t, ffi::array<uint8_t>);
    errno_t path_symlink(ffi::array<uint8_t>, fd_t, ffi::array<uint8_t>);
    errno_t path_unlink_file(fd_t, ffi::array<uint8_t>);
    errno_t poll_oneoff(ffi::memory*, ffi::ptr, ffi::ptr, uint32_t, uint32_t*);
    errno_t random_get(ffi::array<uint8_t>);
    void proc_exit(int32_t exit_code);
    ss::future<errno_t> sched_yield();
    errno_t sock_accept(fd_t, uint16_t, fd_t*);
    errno_t
    sock_recv(fd_t, ffi::array<iovec_t>, uint16_t, uint32_t*, uint16_t*);
    errno_t sock_send(fd_t, ffi::array<iovec_t>, uint16_t, uint32_t*);
    errno_t sock_shutdown(fd_t, uint8_t);

private:
    timestamp_t _now{0};
    std::vector<ss::sstring> _args;
    std::vector<ss::sstring> _environ;
    log_writer _stdout_log_writer;
    log_writer _stderr_log_writer;
};

} // namespace wasm::wasi
