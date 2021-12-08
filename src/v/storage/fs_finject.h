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

#pragma once

#include "failure_probes.h"
#include "seastarx.h"
#include "vlog.h"

#include <seastar/core/file.hh>
namespace storage {
/**
 * Filesystem-layer, per-file fault injection.
 */
class fs_finject : public ss::file_impl {
public:
    explicit fs_finject(ss::file to_wrap)
      : _file(std::move(to_wrap)) {}
    fs_finject(const fs_finject&) = delete;
    fs_finject& operator=(const fs_finject&) = delete;
    fs_finject& operator=(fs_finject&&) = default;
    fs_finject(fs_finject&& o) = default;
    ~fs_finject() noexcept override = default;

    ss::future<size_t> write_dma(
      uint64_t pos,
      const void* buffer,
      size_t len,
      const ss::io_priority_class& pc) final;

    ss::future<size_t> write_dma(
      uint64_t pos,
      std::vector<iovec> iov,
      const ss::io_priority_class& pc) final;

    ss::future<size_t> read_dma(
      uint64_t pos,
      void* buffer,
      size_t len,
      const ss::io_priority_class& pc) final;

    ss::future<size_t> read_dma(
      uint64_t pos,
      std::vector<iovec> iov,
      const ss::io_priority_class& pc) final;

    // TODO wrap ss::open_file_dma, for failure injection when
    // ss::open_flags::create is set

    ss::future<> flush() final;
    ss::future<struct stat> stat() final;
    ss::future<> truncate(uint64_t length) final;
    ss::future<> discard(uint64_t offset, uint64_t length) final;
    ss::future<int> ioctl(uint64_t cmd, void* argp) noexcept final;
    ss::future<int> ioctl_short(uint64_t cmd, void* argp) noexcept final;
    ss::future<int> fcntl(int op, uintptr_t arg) noexcept final;
    ss::future<int> fcntl_short(int op, uintptr_t arg) noexcept final;
    ss::future<> allocate(uint64_t position, uint64_t length) final;
    ss::future<uint64_t> size() final;
    ss::future<> close() final;
    std::unique_ptr<ss::file_handle_impl> dup() final;
    ss::subscription<ss::directory_entry> list_directory(
      std::function<ss::future<>(ss::directory_entry de)> next) final;
    ss::future<ss::temporary_buffer<uint8_t>> dma_read_bulk(
      uint64_t offset,
      size_t range_size,
      const ss::io_priority_class& pc) final;

    static fs_failure_probe& get_probe() { return _probe; }

private:
    ss::file _file;
    static thread_local fs_failure_probe _probe; // NOLINT global for now
};
} // namespace storage