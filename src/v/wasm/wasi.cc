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

#include "wasi.h"

#include "ffi.h"
#include "seastarx.h"
#include "units.h"
#include "utils/utf8.h"
#include "vassert.h"
#include "vlog.h"
#include "wasm/logger.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/print.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/later.hh>
#include <seastar/util/log.hh>

#include <absl/strings/escaping.h>
#include <absl/strings/str_join.h>
#include <fmt/chrono.h>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <chrono>
#include <cstdint>
#include <exception>
#include <numeric>
#include <ratio>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <unistd.h>
#include <utility>

namespace wasm::wasi {

exit_exception::exit_exception(int32_t exit_code)
  : _code(exit_code)
  , _msg(ss::format("exited with code: {}", exit_code)) {}

const char* exit_exception::what() const noexcept { return _msg.c_str(); }
int32_t exit_exception::code() const noexcept { return _code; }

log_writer::log_writer(ss::sstring name, bool is_guest_stdout, ss::logger* l)
  : _is_guest_stdout(is_guest_stdout)
  , _name(std::move(name))
  , _logger(l) {}

log_writer log_writer::make_for_stderr(ss::sstring name, ss::logger* l) {
    return log_writer(std::move(name), false, l);
}
log_writer log_writer::make_for_stdout(ss::sstring name, ss::logger* l) {
    return log_writer(std::move(name), true, l);
}

uint32_t log_writer::write(std::string_view buf) {
    uint32_t amt = 0;
    while (!buf.empty()) {
        size_t pos = buf.find('\n');
        if (pos == std::string_view::npos) {
            _buffer.push_back(buf);
            return amt;
        }
        _buffer.push_back(buf.substr(0, pos));
        // Add one for the newline we stripped off and flush adds
        amt += flush() + 1;
        buf = buf.substr(pos + 1);
    }
    return amt;
}

uint32_t log_writer::flush() {
    if (_buffer.empty()) {
        return 0;
    }
    auto buf = std::exchange(_buffer, {});
    uint32_t amt = 0;
    for (auto b : buf) {
        amt += b.size();
    }
    auto level = _is_guest_stdout ? ss::log_level::info : ss::log_level::warn;
    auto joined = absl::StrJoin(buf, "");
    if (!is_valid_utf8(joined) || contains_control_character(joined))
      [[unlikely]] {
        joined = absl::CHexEscape(joined);
    }
    constexpr static size_t max_log_line = 2_KiB;
    if (joined.size() > max_log_line) {
        joined.resize(max_log_line);
    }
    _logger->log(level, "{} - {}", _name, joined);
    return amt;
}

preview1_module::preview1_module(
  std::vector<ss::sstring> args, const environ_map_t& environ, ss::logger* l)
  : _args(std::move(args))
  , _stdout_log_writer(log_writer::make_for_stdout(_args.front(), l))
  , _stderr_log_writer(log_writer::make_for_stderr(_args.front(), l)) {
    _environ.reserve(environ.size());
    for (const auto& [k, v] : environ) {
        if (k.find("=") != ss::sstring::npos) {
            throw std::runtime_error(
              ss::format("invalid environment key: {}", k));
        }
        _environ.push_back(ss::format("{}={}", k, v));
    }
}

void preview1_module::set_timestamp(model::timestamp ts) {
    using namespace std::chrono;
    milliseconds ms(ts.value());
    nanoseconds ns = duration_cast<nanoseconds>(ms);
    _now = timestamp_t(ns.count());
}

// We don't have control over this API, so there will be some redundant
// wrappers. NOLINTBEGIN(bugprone-easily-swappable-parameters)

errno_t preview1_module::clock_res_get(clock_id_t id, timestamp_t* out) {
    switch (id) {
    case REALTIME_CLOCK_ID:
    case MONOTONIC_CLOCK_ID:
    case PROCESS_CPUTIME_CLOCK_ID:
    case THREAD_CPUTIME_CLOCK_ID: {
        using namespace std::chrono;
        // We only have millisecond resolution because we're using record
        // timestamps as the clock value, and they only have millisecond
        // resolution themselves.
        milliseconds ms(1);
        nanoseconds ns = duration_cast<nanoseconds>(ms);
        *out = timestamp_t(ns.count());
        return ERRNO_SUCCESS;
    }
    default:
        return ERRNO_INVAL;
    }
}

errno_t
preview1_module::clock_time_get(clock_id_t id, timestamp_t, timestamp_t* out) {
    switch (id) {
    case REALTIME_CLOCK_ID:
    case MONOTONIC_CLOCK_ID:
    case PROCESS_CPUTIME_CLOCK_ID:
    case THREAD_CPUTIME_CLOCK_ID:
        *out = _now;
        return ERRNO_SUCCESS;
    default:
        return ERRNO_INVAL;
    }
}
namespace {
size_t serialized_args_size(const std::vector<ss::sstring>& args) {
    size_t n = 0;
    for (const auto& arg : args) {
        // Add one for the null byte
        n += arg.size() + 1;
    }
    return n;
}

void serialize_args(
  const std::vector<ss::sstring>& args,
  uint32_t offset,
  ffi::array<uint32_t> ptrs,
  ffi::writer* data_out) {
    uint32_t position = offset;
    for (size_t i = 0; i < args.size(); ++i) {
        const auto& arg = args[i];
        ptrs[i] = position;
        data_out->append(arg);
        data_out->append(std::string_view{"\0", 1});
        position += arg.size() + 1;
    }
}
} // namespace

errno_t
preview1_module::args_sizes_get(uint32_t* count_ptr, uint32_t* size_ptr) {
    *count_ptr = _args.size();
    *size_ptr = serialized_args_size(_args);
    return ERRNO_SUCCESS;
}

errno_t preview1_module::args_get(
  ffi::memory* mem, ffi::ptr args_ptrs_offset, ffi::ptr args_buf_offset) {
    try {
        auto args_ptrs_buf = mem->translate_array<uint32_t>(
          args_ptrs_offset, _args.size());
        auto args_data_buf = mem->translate_array<uint8_t>(
          args_buf_offset, serialized_args_size(_args));
        ffi::writer data_writer(args_data_buf);
        serialize_args(_args, args_buf_offset, args_ptrs_buf, &data_writer);
        return ERRNO_SUCCESS;
    } catch (const std::exception& ex) {
        vlog(wasm_log.warn, "args_get: {}", ex);
        return ERRNO_INVAL;
    }
}

errno_t
preview1_module::environ_sizes_get(uint32_t* count_ptr, uint32_t* size_ptr) {
    *count_ptr = _environ.size();
    *size_ptr = serialized_args_size(_environ);
    return ERRNO_SUCCESS;
}

errno_t preview1_module::environ_get(
  ffi::memory* mem, ffi::ptr environ_ptrs_offset, ffi::ptr environ_buf_offset) {
    try {
        auto environ_ptrs_buf = mem->translate_array<uint32_t>(
          environ_ptrs_offset, _environ.size());
        auto environ_data_buf = mem->translate_array<uint8_t>(
          environ_buf_offset, serialized_args_size(_environ));
        ffi::writer data_writer(environ_data_buf);
        serialize_args(
          _environ, environ_buf_offset, environ_ptrs_buf, &data_writer);
        return ERRNO_SUCCESS;
    } catch (const std::exception& ex) {
        vlog(wasm_log.warn, "environ_get: {}", ex);
        return ERRNO_INVAL;
    }
}

errno_t preview1_module::fd_advise(fd_t, uint64_t, uint64_t, uint8_t) {
    return ERRNO_NOSYS;
}
errno_t preview1_module::fd_allocate(fd_t, uint64_t, uint64_t) {
    return ERRNO_NOSYS;
}
errno_t preview1_module::fd_close(fd_t) { return ERRNO_NOSYS; }
errno_t preview1_module::fd_datasync(fd_t) { return ERRNO_NOSYS; }
errno_t preview1_module::fd_fdstat_get(fd_t, void*) { return ERRNO_NOSYS; }
errno_t preview1_module::fd_fdstat_set_flags(fd_t, uint16_t) {
    return ERRNO_NOSYS;
}
errno_t preview1_module::fd_filestat_get(fd_t, void*) { return ERRNO_NOSYS; }
errno_t preview1_module::fd_filestat_set_size(fd_t, uint64_t) {
    return ERRNO_NOSYS;
}
errno_t preview1_module::fd_filestat_set_times(
  fd_t, timestamp_t, timestamp_t, uint16_t) {
    return ERRNO_NOSYS;
}
errno_t
preview1_module::fd_pread(fd_t, ffi::array<iovec_t>, uint64_t, uint32_t*) {
    return ERRNO_NOSYS;
}
errno_t preview1_module::fd_prestat_dir_name(fd_t, uint8_t*, uint32_t) {
    return ERRNO_NOSYS;
}
errno_t
preview1_module::fd_pwrite(fd_t, ffi::array<iovec_t>, uint64_t, uint32_t*) {
    return ERRNO_NOSYS;
}
errno_t preview1_module::fd_read(fd_t, ffi::array<iovec_t>, uint32_t*) {
    return ERRNO_NOSYS;
}
errno_t
preview1_module::fd_readdir(fd_t, ffi::array<uint8_t>, uint64_t, uint32_t*) {
    return ERRNO_NOSYS;
}
errno_t preview1_module::fd_renumber(fd_t, fd_t) { return ERRNO_NOSYS; }
errno_t preview1_module::fd_seek(fd_t, int64_t, uint8_t, uint64_t*) {
    return ERRNO_NOSYS;
}
errno_t preview1_module::fd_sync(fd_t) { return ERRNO_NOSYS; }
errno_t preview1_module::fd_tell(fd_t, uint64_t*) { return ERRNO_NOSYS; }
errno_t preview1_module::fd_prestat_get(fd_t fd, void*) {
    if (fd == 0 || fd == 1 || fd == 2) {
        // stdin, stdout, stderr are fine but unimplemented
        return ERRNO_NOSYS;
    }
    // We don't hand out any file descriptors and this is needed for wasi_libc
    return ERRNO_BADF;
}

errno_t preview1_module::fd_write(
  ffi::memory* mem, fd_t fd, ffi::array<iovec_t> iovecs, uint32_t* written) {
    if (fd == 1 || fd == 2) {
        uint32_t amt = 0;
        auto logger = fd == 1 ? &_stdout_log_writer : &_stderr_log_writer;
        for (const iovec_t& vec : iovecs) {
            try {
                ffi::array<char> data = mem->translate_array<char>(
                  vec.buf_addr, vec.buf_len);
                amt += logger->write(
                  std::string_view(data.data(), data.size()));
            } catch (const std::exception& ex) {
                vlog(wasm_log.warn, "fd_write: {}", ex);
                return ERRNO_INVAL;
            }
        }
        // Always flush so we don't have to keep any memory around between
        // calls.
        amt += logger->flush();
        *written = amt;
        return ERRNO_SUCCESS;
    }
    return ERRNO_NOSYS;
}

errno_t preview1_module::path_create_directory(fd_t, ffi::array<uint8_t>) {
    return ERRNO_NOSYS;
}
errno_t
preview1_module::path_filestat_get(fd_t, uint32_t, ffi::array<uint8_t>, void*) {
    return ERRNO_NOSYS;
}
errno_t preview1_module::path_filestat_set_times(
  fd_t, uint32_t, ffi::array<uint8_t>, timestamp_t, timestamp_t, uint16_t) {
    return ERRNO_NOSYS;
}
errno_t preview1_module::path_link(
  fd_t, uint32_t, ffi::array<uint8_t>, fd_t, ffi::array<uint8_t>) {
    return ERRNO_NOSYS;
}
errno_t preview1_module::path_open(
  fd_t,
  uint32_t,
  ffi::array<uint8_t>,
  uint16_t,
  uint64_t,
  uint64_t,
  uint16_t,
  fd_t*) {
    return ERRNO_NOSYS;
}
errno_t preview1_module::path_readlink(
  fd_t, ffi::array<uint8_t>, ffi::array<uint8_t>, uint32_t*) {
    return ERRNO_NOSYS;
}
errno_t preview1_module::path_remove_directory(fd_t, ffi::array<uint8_t>) {
    return ERRNO_NOSYS;
}
errno_t preview1_module::path_rename(
  fd_t, ffi::array<uint8_t>, fd_t, ffi::array<uint8_t>) {
    return ERRNO_NOSYS;
}
errno_t
preview1_module::path_symlink(ffi::array<uint8_t>, fd_t, ffi::array<uint8_t>) {
    return ERRNO_NOSYS;
}
errno_t preview1_module::path_unlink_file(fd_t, ffi::array<uint8_t>) {
    return ERRNO_NOSYS;
}
errno_t preview1_module::poll_oneoff(
  ffi::memory* memory,
  ffi::ptr in_addr,
  ffi::ptr out_addr,
  uint32_t nsubscriptions,
  uint32_t* retptr) {
    // This is a minimal implementation of poll_oneoff for golang, which
    // requires it for sleep support.
    // In reality this should be a full poll(2) implemenation, but we don't
    // actually need that unless we're going to support some kind of filesystem
    // access.
    auto subscriptions = memory->translate_array<subscription_t>(
      in_addr, nsubscriptions);
    auto events = memory->translate_array<event_t>(out_addr, nsubscriptions);
    for (uint32_t i = 0; i < nsubscriptions; ++i) {
        const auto& sub = subscriptions[i];
        auto& event = events[i];
        event.userdata = sub.userdata;
        event.type = sub.u.tag;
        switch (sub.u.tag) {
        case CLOCK_EVENT_TYPE: {
            // Actually noop the real sleep.
            // There is no need to actually sleep since this it the only event
            // we support. Usually if clock is used in conjunction with
            // reads/writes to implement timeouts, but since we're not
            // supporting reads or writes of fd, we can just immediately return
            // that we've slept.
            // IF we ever actually want to truly sleep here, we need to make
            // sure it can be aborted, so that users can't hang VMs while this
            // sleep is happening.
            event.error = ERRNO_SUCCESS;
            break;
        }
        case FD_WRITE_EVENT_TYPE:
        case FD_READ_EVENT_TYPE: {
            event.error = ERRNO_NOSYS;
            break;
        }
        default:
            return ERRNO_NOSYS;
        }
    }
    // Report how many events we wrote back out.
    *retptr = nsubscriptions;
    return ERRNO_SUCCESS;
}
errno_t preview1_module::random_get(ffi::array<uint8_t> buf) {
    // https://imgur.com/uR4WuQ0
    constexpr uint8_t random_number = 9;
    std::fill(buf.begin(), buf.end(), random_number);
    return ERRNO_SUCCESS;
}
errno_t preview1_module::sock_accept(fd_t, uint16_t, fd_t*) {
    return ERRNO_NOSYS;
}
errno_t preview1_module::sock_recv(
  fd_t, ffi::array<iovec_t>, uint16_t, uint32_t*, uint16_t*) {
    return ERRNO_NOSYS;
}
errno_t
preview1_module::sock_send(fd_t, ffi::array<iovec_t>, uint16_t, uint32_t*) {
    return ERRNO_NOSYS;
}
errno_t preview1_module::sock_shutdown(fd_t, uint8_t) { return ERRNO_NOSYS; }
void preview1_module::proc_exit(int32_t exit_code) {
    throw exit_exception(exit_code);
}
ss::future<errno_t> preview1_module::sched_yield() {
    co_await ss::yield();
    co_return ERRNO_SUCCESS;
}
// NOLINTEND(bugprone-easily-swappable-parameters)
} // namespace wasm::wasi
