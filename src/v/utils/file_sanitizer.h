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

#pragma once

#include "ssx/sformat.h"
#include "vassert.h"

#include <seastar/core/file.hh>
#include <seastar/util/backtrace.hh>
#include <seastar/util/log.hh>

#include <boost/intrusive/list.hpp>

#include <optional>
#include <ostream>
#include <unordered_map>
#include <utility>

namespace bi = boost::intrusive;

#include "seastarx.h"

struct sanitizer_op
  : public bi::list_base_hook<bi::link_mode<bi::auto_unlink>> {
    ss::sstring name_op;
    ss::saved_backtrace bt;

    explicit sanitizer_op(ss::sstring operation)
      : name_op(std::move(operation))
      , bt(ss::current_backtrace()) {}

    friend std::ostream& operator<<(std::ostream& o, const sanitizer_op& s) {
        return o << "{sanitizer_op: " << s.name_op << ", backtrace:\n"
                 << s.bt << "\n}";
    }
};

/// Classed used to debug bad file accesses, by wrapping the file handle:
/// auto fd = file(make_shared(file_io_sanitizer(original_fd)));
class file_io_sanitizer final : public ss::file_impl {
public:
    explicit file_io_sanitizer(ss::file f)
      : _file(std::move(f)) {}
    ~file_io_sanitizer() override {
        if (!_closed && _file) {
            std::cout << "File destroying without being closed!" << std::endl;
            output_pending_ops();
        }
    }
    file_io_sanitizer(const file_io_sanitizer&) = delete;
    file_io_sanitizer& operator=(const file_io_sanitizer&) = delete;
    file_io_sanitizer(file_io_sanitizer&& o) noexcept
      : _pending_ops(std::move(o._pending_ops))
      , _file(std::move(o._file))
      , _closed(std::move(o._closed)) {}
    file_io_sanitizer& operator=(file_io_sanitizer&& o) noexcept {
        if (this != &o) {
            this->~file_io_sanitizer();
            new (this) file_io_sanitizer(std::move(o));
        }
        return *this;
    }

    ss::future<size_t> write_dma(
      uint64_t pos,
      const void* buffer,
      size_t len,
      const ss::io_priority_class& pc) final {
        assert_file_not_closed();
        return with_op(
          ssx::sformat(
            "ss::future<size_t>::write_dma(pos:{}, *void, len:{})", pos, len),
          get_file_impl(_file)->write_dma(pos, buffer, len, pc));
    }

    ss::future<size_t> write_dma(
      uint64_t pos,
      std::vector<iovec> iov,
      const ss::io_priority_class& pc) final {
        assert_file_not_closed();
        return with_op(
          ssx::sformat(
            "ss::future<size_t>::write_dma(pos:{}, vector<iovec>:{})",
            pos,
            iov.size()),
          get_file_impl(_file)->write_dma(pos, iov, pc));
    }

    ss::future<size_t> read_dma(
      uint64_t pos,
      void* buffer,
      size_t len,
      const ss::io_priority_class& pc) final {
        assert_file_not_closed();
        return with_op(
          ssx::sformat(
            "ss::future<size_t> read_dma(pos:{}, void* buffer, len:{})",
            pos,
            len),
          get_file_impl(_file)->read_dma(pos, buffer, len, pc));
    }

    ss::future<size_t> read_dma(
      uint64_t pos,
      std::vector<iovec> iov,
      const ss::io_priority_class& pc) final {
        assert_file_not_closed();
        return with_op(
          ssx::sformat(
            "ss::future<size_t> read_dma(pos{}, std::vector<iovec>:{} , "
            "const ss::io_priority_class& pc)",
            pos,
            iov.size()),
          get_file_impl(_file)->read_dma(pos, iov, pc));
    }

    ss::future<> flush() final {
        assert_file_not_closed();
        if (!_pending_ops.empty()) {
            std::cout << "flush() called concurrently with other operations.\n";
            output_pending_ops();
        }
        return with_op(
          ssx::sformat("ss::future<> flush(void)"),
          get_file_impl(_file)->flush());
    }

    ss::future<struct stat> stat() final {
        assert_file_not_closed();
        return with_op(
          ssx::sformat("ss::future<> stat(void)"),
          get_file_impl(_file)->stat());
    }

    ss::future<> truncate(uint64_t length) final {
        assert_file_not_closed();
        return with_op(
          ssx::sformat("ss::future<> truncate({})", length),
          get_file_impl(_file)->truncate(length));
    }

    ss::future<> discard(uint64_t offset, uint64_t length) final {
        assert_file_not_closed();
        return with_op(
          ssx::sformat(
            "ss::future<> discard(offset:{}, length:{})", offset, length),
          get_file_impl(_file)->discard(offset, length));
    }

    ss::future<> allocate(uint64_t position, uint64_t length) final {
        assert_file_not_closed();
        return with_op(
          ssx::sformat(
            "ss::future<> allocate(position:{}, length:{})", position, length),
          get_file_impl(_file)->allocate(position, length));
    }

    ss::future<uint64_t> size() final {
        assert_file_not_closed();
        return with_op(
          ssx::sformat("ss::future<uint64_t> size(void)"),
          get_file_impl(_file)->size());
    }

    ss::future<> close() final {
        if (_closed) {
            std::cout << "close() called again, from "
                      << ss::current_backtrace() << std::endl;
        }
        if (!_pending_ops.empty()) {
            std::cout << "close() called concurrently with other operations.\n";
            output_pending_ops();
        }
        _closed = {ss::current_backtrace()};
        return get_file_impl(_file)->close();
    }

    std::unique_ptr<ss::file_handle_impl> dup() final {
        assert_file_not_closed();
        return get_file_impl(_file)->dup();
    }

    ss::subscription<ss::directory_entry> list_directory(
      std::function<ss::future<>(ss::directory_entry de)> next) final {
        assert_file_not_closed();
        return get_file_impl(_file)->list_directory(next);
    }

    ss::future<ss::temporary_buffer<uint8_t>> dma_read_bulk(
      uint64_t offset,
      size_t range_size,
      const ss::io_priority_class& pc) final {
        assert_file_not_closed();
        return with_op(
          ssx::sformat(
            "ss::future<ss::temporary_buffer<uint8_t>> "
            "dma_read_bulk(offset:{}, range_size:{}",
            offset,
            range_size),
          get_file_impl(_file)->dma_read_bulk(offset, range_size, pc));
    }

private:
    template<typename Future>
    Future with_op(ss::sstring name, Future&& f) {
        auto pending = std::make_unique<sanitizer_op>(std::move(name));
        _pending_ops.push_back(*pending);
        return std::forward<Future>(f).finally(
          [pending = std::move(pending)] {});
    }

    void output_pending_ops() {
        std::cout << "  called from " << ss::current_backtrace();
        for (sanitizer_op& op : _pending_ops) {
            std::cout << "....pending op: " << op;
        }
        _pending_ops.clear();
    }

    void assert_file_not_closed() {
        vassert(
          !_closed,
          "Op performed on closed file. StackTrace:\n{}",
          ss::current_backtrace());
    }

private:
    boost::intrusive::list<sanitizer_op, bi::constant_time_size<false>>
      _pending_ops;
    ss::file _file;
    std::optional<ss::saved_backtrace> _closed;
};
