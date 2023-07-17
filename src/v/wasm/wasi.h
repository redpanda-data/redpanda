/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "ffi.h"
#include "model/timestamp.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/noncopyable_function.hh>

#include <absl/container/flat_hash_map.h>

#include <chrono>
#include <string_view>
#include <vector>

namespace wasm::wasi {

constexpr std::string_view preview_1_start_function_name = "_start";

using errno_t = named_type<uint16_t, struct errc_tag>;
// https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L110-L113C1
constexpr errno_t ERRNO_SUCCESS = errno_t(0);

// https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L250-L253C1
constexpr errno_t ERRNO_INVAL = errno_t(16);

// https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L370-L373
constexpr errno_t ERRNO_NOSYS = errno_t(52);

// https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L370-L373
constexpr errno_t ERRNO_BADF = errno_t(8);

// https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L70C1-L97
using clock_id_t = named_type<uint32_t, struct clock_id_tag>;
constexpr clock_id_t REALTIME_CLOCK_ID = clock_id_t(0);
constexpr clock_id_t MONOTONIC_CLOCK_ID = clock_id_t(1);
constexpr clock_id_t PROCESS_CPUTIME_CLOCK_ID = clock_id_t(2);
constexpr clock_id_t THREAD_CPUTIME_CLOCK_ID = clock_id_t(3);
// A timestamp in nanoseconds
using timestamp_t = named_type<uint64_t, struct timestamp_tag>;
// A file descriptor
using fd_t = named_type<int32_t, struct fd_tag>;
/**
 * A region of memory for scatter/gather writes.
 */
struct iovec_t {
    /** The (guest) address of the buffer to be written. */
    uint32_t buf;
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

    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1446-L1452
    errno_t clock_res_get(clock_id_t, timestamp_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1453-L1469
    errno_t clock_time_get(clock_id_t, timestamp_t, timestamp_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1409-L1418
    errno_t args_sizes_get(uint32_t*, uint32_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1400-L1408
    errno_t args_get(ffi::memory*, uint32_t, uint32_t);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1419-L1427
    errno_t environ_get(ffi::memory*, uint32_t, uint32_t);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1428-L1437
    errno_t environ_sizes_get(uint32_t*, uint32_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1470-L1488
    errno_t fd_advise(fd_t, uint64_t, uint64_t, uint8_t);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1493-L1503
    errno_t fd_allocate(fd_t, uint64_t, uint64_t);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1504-L1510
    errno_t fd_close(fd_t);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1504-L1510
    errno_t fd_datasync(fd_t);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1518-L1527
    errno_t fd_fdstat_get(fd_t, void*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1528-L1538
    errno_t fd_fdstat_set_flags(fd_t, uint16_t);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1551-L1559
    errno_t fd_filestat_get(fd_t, void*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1560-L1570
    errno_t fd_filestat_set_size(fd_t, uint64_t);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1571-L1589
    errno_t fd_filestat_set_times(fd_t, timestamp_t, timestamp_t, uint16_t);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1590-L1611
    errno_t fd_pread(fd_t, ffi::array<iovec_t>, uint64_t, uint32_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1612-L1620
    errno_t fd_prestat_get(fd_t, void*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1621-L1631
    errno_t fd_prestat_dir_name(fd_t, uint8_t*, uint32_t);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1632-L1653
    errno_t fd_pwrite(fd_t, ffi::array<iovec_t>, uint64_t, uint32_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1654-L1671
    errno_t fd_read(fd_t, ffi::array<iovec_t>, uint32_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1672-L1697
    errno_t fd_readdir(fd_t, ffi::array<uint8_t>, uint64_t, uint32_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1698-L1714
    errno_t fd_renumber(fd_t, fd_t);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1715-L1732
    errno_t fd_seek(fd_t, int64_t, uint8_t, uint64_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1733-L1739
    errno_t fd_sync(fd_t);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1740-L1749
    errno_t fd_tell(fd_t, uint64_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1750-L1765
    errno_t fd_write(ffi::memory*, fd_t, ffi::array<iovec_t>, uint32_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1766-L1776
    errno_t path_create_directory(fd_t, ffi::array<uint8_t>);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1777-L1794
    errno_t path_filestat_get(fd_t, uint32_t, ffi::array<uint8_t>, void*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1795-L1821
    errno_t path_filestat_set_times(
      fd_t, uint32_t, ffi::array<uint8_t>, timestamp_t, timestamp_t, uint16_t);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1822-L1844
    errno_t
      path_link(fd_t, uint32_t, ffi::array<uint8_t>, fd_t, ffi::array<uint8_t>);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1845-L1884
    errno_t path_open(
      fd_t,
      uint32_t,
      ffi::array<uint8_t>,
      uint16_t,
      uint64_t,
      uint64_t,
      uint16_t,
      fd_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1885-L1903
    errno_t path_readlink(
      fd_t, ffi::array<uint8_t> path, ffi::array<uint8_t>, uint32_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1904-L1915
    errno_t path_remove_directory(fd_t, ffi::array<uint8_t>);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1916-L1934
    errno_t path_rename(fd_t, ffi::array<uint8_t>, fd_t, ffi::array<uint8_t>);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1935-L1949
    errno_t path_symlink(ffi::array<uint8_t>, fd_t, ffi::array<uint8_t>);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1950-L1961
    errno_t path_unlink_file(fd_t, ffi::array<uint8_t>);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1962-L1981
    errno_t poll_oneoff(void*, void*, uint32_t, uint32_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L2000-L2014
    errno_t random_get(ffi::array<uint8_t>);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1982-L1992
    void proc_exit(int32_t exit_code);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L1993-L1999
    ss::future<errno_t> sched_yield();
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L2015-L2031
    errno_t sock_accept(fd_t, uint16_t, fd_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L2032-L2055
    errno_t
    sock_recv(fd_t, ffi::array<iovec_t>, uint16_t, uint32_t*, uint16_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L2056-L2078
    errno_t sock_send(fd_t, ffi::array<iovec_t>, uint16_t, uint32_t*);
    // https://github.com/WebAssembly/wasi-libc/blob/a6f871343313220b76009827ed0153586361c0d5/libc-bottom-half/headers/public/wasi/api.h#L2079-L2090
    errno_t sock_shutdown(fd_t, uint8_t);

private:
    timestamp_t _now{0};
    std::vector<ss::sstring> _args;
    std::vector<ss::sstring> _environ;
    log_writer _stdout_log_writer;
    log_writer _stderr_log_writer;
};

} // namespace wasm::wasi
