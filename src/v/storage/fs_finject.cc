/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "fs_finject.h"

#include "seastarx.h"
#include "storage/logger.h"

#include <seastar/core/future.hh>
#include <seastar/core/io_priority_class.hh>
namespace storage {

thread_local fs_failure_probe fs_finject::_probe = {}; // NOLINT
using methods = fs_failure_probe::methods;

ss::future<size_t> fs_finject::write_dma(
  uint64_t pos,
  const void* buffer,
  size_t len,
  const ss::io_priority_class& pc) {
    return _probe.maybe_inject_failure(methods::write_dma, "write_dma")
      .then([this, pos, buffer, len, pc]() {
          return get_file_impl(_file)->write_dma(pos, buffer, len, pc);
      });
}

ss::future<size_t> fs_finject::write_dma(
  uint64_t pos, std::vector<iovec> iov, const ss::io_priority_class& pc) {
    return _probe.maybe_inject_failure(methods::write_dma, "write_dma")
      .then([this, pos, iov, pc]() {
          return get_file_impl(_file)->write_dma(pos, iov, pc);
      });
}

ss::future<size_t> fs_finject::read_dma(
  uint64_t pos, void* buffer, size_t len, const ss::io_priority_class& pc) {
    return get_file_impl(_file)->read_dma(pos, buffer, len, pc);
}

ss::future<size_t> fs_finject::read_dma(
  uint64_t pos, std::vector<iovec> iov, const ss::io_priority_class& pc) {
    return get_file_impl(_file)->read_dma(pos, iov, pc);
}

ss::future<> fs_finject::flush() { return get_file_impl(_file)->flush(); }

ss::future<struct stat> fs_finject::stat() {
    return get_file_impl(_file)->stat();
}

ss::future<> fs_finject::truncate(uint64_t length) {
    return get_file_impl(_file)->truncate(length);
}

ss::future<> fs_finject::discard(uint64_t offset, uint64_t length) {
    return get_file_impl(_file)->discard(offset, length);
}

ss::future<int> fs_finject::ioctl(uint64_t cmd, void* argp) noexcept {
    return get_file_impl(_file)->ioctl(cmd, argp);
}

ss::future<int> fs_finject::ioctl_short(uint64_t cmd, void* argp) noexcept {
    return get_file_impl(_file)->ioctl_short(cmd, argp);
}

ss::future<int> fs_finject::fcntl(int op, uintptr_t arg) noexcept {
    return get_file_impl(_file)->fcntl(op, arg);
}

ss::future<int> fs_finject::fcntl_short(int op, uintptr_t arg) noexcept {
    return get_file_impl(_file)->fcntl_short(op, arg);
}

ss::future<> fs_finject::allocate(uint64_t pos, uint64_t len) {
    vlog(stlog.info, "fs_finject hooked allocate()");
    return _probe.maybe_inject_failure(methods::allocate, "allocate")
      .then([this, pos, len]() {
          return get_file_impl(_file)->allocate(pos, len);
      });
}

ss::future<uint64_t> fs_finject::size() { return get_file_impl(_file)->size(); }

ss::future<> fs_finject::close() { return get_file_impl(_file)->close(); }

std::unique_ptr<ss::file_handle_impl> fs_finject::dup() {
    return get_file_impl(_file)->dup();
}

ss::subscription<ss::directory_entry> fs_finject::list_directory(
  std::function<ss::future<>(ss::directory_entry de)> next) {
    return get_file_impl(_file)->list_directory(next);
}

ss::future<ss::temporary_buffer<uint8_t>> fs_finject::dma_read_bulk(
  uint64_t offset, size_t range_size, const ss::io_priority_class& pc) {
    return get_file_impl(_file)->dma_read_bulk(offset, range_size, pc);
}
} // namespace storage