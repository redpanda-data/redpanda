/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "lsm/io/file_io.h"

#include "lsm/core/exceptions.h"

#include <seastar/core/reactor.hh>
#include <seastar/coroutine/as_future.hh>

namespace lsm::io {

disk_file_reader::disk_file_reader(std::filesystem::path path, ss::file file)
  : _path(std::move(path))
  , _file(std::move(file)) {}

ss::future<ioarray> disk_file_reader::read(size_t offset, size_t n) {
    size_t memory_alignment = _file.memory_dma_alignment();
    size_t disk_alignment = _file.disk_read_dma_alignment();
    size_t adjusted_offset = ss::align_down(offset, disk_alignment);
    size_t offset_delta = offset - adjusted_offset;
    auto array = ioarray::aligned(
      memory_alignment, ss::align_up(n + offset_delta, disk_alignment));
    try {
        size_t amt = co_await _file.dma_read(adjusted_offset, array.as_iovec());
        if (amt < offset_delta + n) {
            throw io_error_exception(
              "short read {}: failed to read {} bytes from block at offset "
              "{}, "
              "got: "
              "{}",
              *this,
              array.size(),
              adjusted_offset,
              amt);
        }
        co_return array.share(offset_delta, n);
    } catch (const std::system_error& err) {
        throw io_error_exception(
          err.code(), "io error reading {}: {}", *this, err);
    } catch (...) {
        throw io_error_exception(
          "io error reading {}: {}", *this, std::current_exception());
    }
}

ss::future<> disk_file_reader::close() {
    try {
        co_await _file.close();
    } catch (const std::system_error& err) {
        throw io_error_exception(
          err.code(), "io error closing {}: {}", *this, err);
    } catch (...) {
        throw io_error_exception(
          "io error closing {}: {}", *this, std::current_exception());
    }
}

fmt::iterator disk_file_reader::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{path={}}}", _path);
}

disk_seq_file_writer::disk_seq_file_writer(
  std::filesystem::path path, ss::output_stream<char> stream)
  : _path(std::move(path))
  , _stream(std::move(stream)) {}

ss::future<> disk_seq_file_writer::append(iobuf buf) {
    try {
        for (auto& frag : buf) {
            co_await _stream.write(frag.get(), frag.size());
        }
    } catch (const std::system_error& err) {
        throw io_error_exception(
          err.code(), "io error writing {}: {}", *this, err);
    } catch (...) {
        throw io_error_exception(
          "io error writing {}: {}", *this, std::current_exception());
    }
}

ss::future<> disk_seq_file_writer::close() {
    try {
        co_await _stream.close();
    } catch (const std::system_error& err) {
        throw io_error_exception(
          err.code(), "io error closing {}: {}", *this, err);
    } catch (...) {
        throw io_error_exception(
          "io error closing {}: {}", *this, std::current_exception());
    }
}

fmt::iterator disk_seq_file_writer::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{path={}}}", _path);
}

} // namespace lsm::io
