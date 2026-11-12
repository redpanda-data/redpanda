/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "utils/file_io.h"

#include "bytes/iobuf.h"
#include "bytes/iostream.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/temporary_buffer.hh>

#include <fmt/format.h>

#include <system_error>

ss::future<ss::temporary_buffer<char>>
read_fully_tmpbuf(const std::filesystem::path& name) {
    return ss::with_file(
      ss::open_file_dma(name.string(), ss::open_flags::ro), [](ss::file f) {
          return f.size().then([f](uint64_t size) mutable {
              return f.dma_read_bulk<char>(0, size);
          });
      });
}

ss::future<iobuf> read_fully(const std::filesystem::path& name) {
    return ss::with_file(
      ss::open_file_dma(name.string(), ss::open_flags::ro), [](ss::file file) {
          auto buf = std::make_unique<iobuf>();
          auto* buf_ptr = buf.get();
          return ss::do_with(
            std::move(buf),
            ss::make_file_input_stream(std::move(file)),
            make_iobuf_ref_output_stream(*buf_ptr),
            [](
              std::unique_ptr<iobuf>& buf,
              ss::input_stream<char>& input,
              ss::output_stream<char>& output) {
                return ss::copy(input, output).then([&buf] {
                    return std::move(*buf);
                });
            });
      });
}

/**
 * This helper is useful for YAML loading, because yaml-cpp expects
 * a string.
 *
 * Background on why: https://github.com/jbeder/yaml-cpp/issues/765
 */
ss::future<ss::sstring>
read_fully_to_string(const std::filesystem::path& name) {
    return read_fully_tmpbuf(name).then([](ss::temporary_buffer<char> buf) {
        return ss::to_sstring(std::move(buf));
    });
}

// The read helpers above open files with O_DIRECT (via Seastar's
// open_file_dma). Filesystems without direct-I/O support reject this with
// EINVAL, which surfaces as an opaque "Invalid argument". A notable case is an
// overlayfs whose lower layer is squashfs, which rejects the O_DIRECT open
// with EINVAL. Some environments bind-mount the bootstrap config from such a
// filesystem, so startup failed with no indication of the cause. Append a hint
// pointing at O_DIRECT in that case.
//
// This matters mainly for config files, which users often bind-mount onto
// arbitrary filesystems. Data directories are not a concern here: they are
// validated separately by syschecks::disk, which does a round-trip O_DIRECT
// check.
//
// See:
//   https://github.com/scylladb/seastar/issues/3518
//   https://github.com/redpanda-data/redpanda/issues/14473
std::string
format_file_io_error(std::string_view context, std::exception_ptr eptr) {
    std::string what = "unknown error";
    bool is_einval = false;
    try {
        if (eptr) {
            std::rethrow_exception(eptr);
        }
    } catch (const std::system_error& e) {
        what = e.what();
        is_einval = e.code() == std::errc::invalid_argument;
    } catch (const std::exception& e) {
        what = e.what();
    } catch (...) {
        what = "unknown exception";
    }
    return fmt::format(
      "{}: {}{}",
      context,
      what,
      is_einval ? " (EINVAL). Does this filesystem support O_DIRECT?" : "");
}

ss::future<> write_fully(const std::filesystem::path& p, iobuf buf) {
    static constexpr const size_t buf_size = 4096;
    auto flags = ss::open_flags::wo | ss::open_flags::create
                 | ss::open_flags::truncate;
    /// Closes file on failure, otherwise file is expected to be closed in the
    /// success case where the ss::output_stream calls close()
    auto out = co_await ss::with_file_close_on_failure(
      ss::open_file_dma(p.string(), flags), [](ss::file f) {
          return ss::make_file_output_stream(std::move(f), buf_size);
      });
    co_await write_iobuf_to_output_stream(std::move(buf), out).finally([&out] {
        return out.close();
    });
}

ss::future<> maybe_remove_file(std::string_view name) {
    try {
        co_await ss::remove_file(name);
    } catch (const std::filesystem::filesystem_error& e) {
        if (e.code() != std::errc::no_such_file_or_directory) {
            throw;
        }
    }
}
