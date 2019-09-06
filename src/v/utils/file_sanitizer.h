#pragma once

#include "seastarx.h"

#include <seastar/core/file.hh>
#include <seastar/util/backtrace.hh>
#include <seastar/util/log.hh>

#include <boost/intrusive/list.hpp>

#include <iostream>
#include <optional>
#include <unordered_map>
#include <utility>

namespace bi = boost::intrusive;

/// Classed used to debug bad file accesses, by wrapping the file handle:
/// auto fd = file(make_shared(file_io_sanitizer(original_fd)));
class file_io_sanitizer : public file_impl {
    struct op : public bi::list_base_hook<bi::link_mode<bi::auto_unlink>> {
        saved_backtrace bt;

        op()
          : bt(current_backtrace()) {
        }
    };

public:
    explicit file_io_sanitizer(file f)
      : _file(f) {
    }

    virtual future<size_t> write_dma(
      uint64_t pos,
      const void* buffer,
      size_t len,
      const io_priority_class& pc) override {
        check_closed();
        return with_op(get_file_impl(_file)->write_dma(pos, buffer, len, pc));
    }

    virtual future<size_t> write_dma(
      uint64_t pos,
      std::vector<iovec> iov,
      const io_priority_class& pc) override {
        check_closed();
        return with_op(get_file_impl(_file)->write_dma(pos, iov, pc));
    }

    virtual future<size_t> read_dma(
      uint64_t pos,
      void* buffer,
      size_t len,
      const io_priority_class& pc) override {
        check_closed();
        return with_op(get_file_impl(_file)->read_dma(pos, buffer, len, pc));
    }

    virtual future<size_t> read_dma(
      uint64_t pos,
      std::vector<iovec> iov,
      const io_priority_class& pc) override {
        check_closed();
        return with_op(get_file_impl(_file)->read_dma(pos, iov, pc));
    }

    virtual future<> flush(void) override {
        check_closed();
        if (!_pending_ops.empty()) {
            std::cout << "flush() called concurrently with other operations.\n";
            output_pending_ops();
        }
        return with_op(get_file_impl(_file)->flush());
    }

    virtual future<struct stat> stat(void) override {
        check_closed();
        return with_op(get_file_impl(_file)->stat());
    }

    virtual future<> truncate(uint64_t length) override {
        check_closed();
        return with_op(get_file_impl(_file)->truncate(length));
    }

    virtual future<> discard(uint64_t offset, uint64_t length) override {
        check_closed();
        return with_op(get_file_impl(_file)->discard(offset, length));
    }

    virtual future<> allocate(uint64_t position, uint64_t length) override {
        check_closed();
        return with_op(get_file_impl(_file)->allocate(position, length));
    }

    virtual future<uint64_t> size(void) override {
        check_closed();
        return with_op(get_file_impl(_file)->size());
    }

    virtual future<> close() override {
        if (_closed) {
            std::cout << "close() called again, from " << current_backtrace()
                      << std::endl;
        }
        if (!_pending_ops.empty()) {
            std::cout << "close() called concurrently with other operations.\n";
            output_pending_ops();
        }
        _closed = {current_backtrace()};
        return get_file_impl(_file)->close();
    }

    virtual std::unique_ptr<file_handle_impl> dup() override {
        check_closed();
        return get_file_impl(_file)->dup();
    }

    virtual subscription<directory_entry>
    list_directory(std::function<future<>(directory_entry de)> next) override {
        check_closed();
        return get_file_impl(_file)->list_directory(next);
    }

    virtual future<temporary_buffer<uint8_t>> dma_read_bulk(
      uint64_t offset,
      size_t range_size,
      const io_priority_class& pc) override {
        check_closed();
        return with_op(
          get_file_impl(_file)->dma_read_bulk(offset, range_size, pc));
    }

private:
    template<typename Future>
    Future with_op(Future&& f) {
        auto pending = std::make_unique<op>();
        _pending_ops.push_back(*pending);
        return std::forward<Future>(f).finally(
          [pending = std::move(pending)] {});
    }

    void output_pending_ops() {
        std::cout << "  called from " << current_backtrace();
        for (auto&& op : _pending_ops) {
            std::cout << "  pending op: " << op.bt;
        }
        _pending_ops.clear();
    }

    void check_closed() {
        if (_closed) {
            std::cout << "Op performed on closed file, from "
                         << current_backtrace() << std::endl;
        }
    }

private:
    boost::intrusive::list<op, bi::constant_time_size<false>> _pending_ops;
    file _file;
    std::optional<saved_backtrace> _closed;
};