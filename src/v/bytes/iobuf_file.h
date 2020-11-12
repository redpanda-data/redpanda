/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "bytes/iobuf.h"
#include "vlog.h"

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>

#include <fmt/ostream.h>
#include <sys/stat.h>

#include <numeric>
#include <stdexcept>

/// auto fd = file(make_shared(iobuf_file(buf)));
struct iobuf_file final : public ss::file_impl {
    explicit iobuf_file(iobuf& ref)
      : _ptr(&ref) {}
    explicit iobuf_file(iobuf* ref)
      : _ptr(ref) {}
    ~iobuf_file() override = default;
    iobuf_file(const iobuf_file&) = default;
    iobuf_file& operator=(const iobuf_file&) = default;
    iobuf_file(iobuf_file&& o) noexcept = default;
    iobuf_file& operator=(iobuf_file&& o) noexcept = default;

    ss::future<size_t> write_dma(
      uint64_t pos,
      const void* buffer,
      size_t len,
      const ss::io_priority_class&) final {
        vlog(logger().info, "write_dma({},{})", pos, len);
        if (_ptr->size_bytes() > pos) {
            _ptr->trim_back(_ptr->size_bytes() - pos);
        }
        _ptr->append((char*)buffer, len);
        return ss::make_ready_future<size_t>(len);
    }

    ss::future<size_t> write_dma(
      uint64_t pos,
      std::vector<iovec> iov,
      const ss::io_priority_class&) final {
        vlog(logger().info, "write_dma iov({})", pos);
        if (_ptr->size_bytes() > pos) {
            _ptr->trim_back(_ptr->size_bytes() - pos);
        }
        size_t acc = 0;
        for (auto& i : iov) {
            _ptr->append((const char*)i.iov_base, i.iov_len);
            acc += i.iov_len;
        }
        return ss::make_ready_future<size_t>(acc);
    }

    ss::future<size_t> read_dma(
      uint64_t pos,
      void* buffer,
      size_t len,
      const ss::io_priority_class&) final {
        vlog(logger().info, "read_dma({},{})", pos, len);
        auto b = _ptr->share(pos, len);
        iobuf::iterator_consumer it(b.cbegin(), b.cend());
        it.consume_to(b.size_bytes(), (char*)buffer);
        return ss::make_ready_future<size_t>(b.size_bytes());
    }

    ss::future<size_t> read_dma(
      uint64_t pos,
      std::vector<iovec> iov,
      const ss::io_priority_class&) final {
        vlog(logger().info, "read_dma with iov({})", pos);
        const size_t len = std::accumulate(
          iov.begin(), iov.end(), size_t(0), [](size_t acc, const iovec& v) {
              return acc + v.iov_len;
          });
        auto b = _ptr->share(pos, len);
        iobuf::iterator_consumer it(b.cbegin(), b.cend());
        for (auto& i : iov) {
            const size_t to_consume = std::min(
              b.size_bytes() - it.bytes_consumed(), i.iov_len);
            i.iov_len = to_consume;
            if (to_consume > 0) {
                it.consume_to(to_consume, (char*)i.iov_base);
            }
        }
        return ss::make_ready_future<size_t>(b.size_bytes());
    }

    ss::future<> flush() final {
        vlog(logger().info, "flush()");
        return ss::now();
    }

    ss::future<struct stat> stat() final {
        vlog(logger().info, "stat()");
        struct stat ret;
        std::memset(&ret, 0, sizeof(ret));
        ret.st_dev = (decltype(ret.st_dev))_ptr; // NOLINT
        ret.st_ino = (decltype(ret.st_ino))_ptr; // NOLINT
        ret.st_mode = __S_IFREG;
        ret.st_size = _ptr->size_bytes();
        ret.st_blocks = _ptr->size_bytes() / 512;
        return ss::make_ready_future<struct stat>(ret);
    }

    ss::future<> truncate(uint64_t pos) final {
        vlog(logger().info, "truncate({})", pos);
        if (pos == 0) {
            _ptr->clear();
        } else if (_ptr->size_bytes() > pos) {
            _ptr->trim_back(_ptr->size_bytes() - pos);
            const size_t new_pos = std::accumulate(
              _ptr->begin(),
              _ptr->end(),
              size_t(0),
              [](size_t acc, const iobuf::fragment& f) {
                  return acc + f.size();
              });
            vassert(
              new_pos == pos,
              "Could not truncate correctly. expected:{}, got:{}",
              pos,
              new_pos);
        }
        return ss::now();
    }

    ss::future<> discard(uint64_t offset, uint64_t length) final {
        vlog(logger().info, "discard({},{})", offset, length);
        return ss::make_exception_future(std::runtime_error(fmt::format(
          "iobuf backed file does not support discard. User asked to discard "
          "at offset:{} with len: {}",
          offset,
          length)));
    }

    ss::future<> allocate(uint64_t pos, uint64_t len) final {
        vlog(logger().info, "allocate({},{})", pos, len);
        return ss::now();
    }

    ss::future<uint64_t> size() final {
        vlog(logger().info, "size()");
        return ss::make_ready_future<uint64_t>(_ptr->size_bytes());
    }

    ss::future<> close() final {
        vlog(logger().info, "close()");
        return ss::now();
    }

    std::unique_ptr<ss::file_handle_impl> dup() final {
        vlog(logger().info, "dup()");
        struct iobuf_handle_impl final : ss::file_handle_impl {
            explicit iobuf_handle_impl(iobuf_file* f) noexcept
              : _f(f) {}
            ~iobuf_handle_impl() override = default;
            std::unique_ptr<ss::file_handle_impl> clone() const final {
                return std::make_unique<iobuf_handle_impl>(_f);
            }
            ss::shared_ptr<file_impl> to_file() && final {
                return ss::make_shared(iobuf_file(_f->_ptr));
            }
            iobuf_file* _f;
        };
        return std::make_unique<iobuf_handle_impl>(this);
    }

    ss::subscription<ss::directory_entry>
    list_directory(std::function<ss::future<>(ss::directory_entry)>) final {
        vlog(logger().info, "list_directory");
        throw std::runtime_error(
          "iobuf_file does not support list_directory() functions");
    }

    ss::future<ss::temporary_buffer<uint8_t>> dma_read_bulk(
      uint64_t pos, size_t len, const ss::io_priority_class&) final {
        vlog(
          logger().info, "dma_read_bulk: POS:{}, LEN:{} - {}", pos, len, *_ptr);
        auto b = _ptr->share(pos, len);
        ss::temporary_buffer<uint8_t> buf(b.size_bytes());
        iobuf::iterator_consumer it(b.cbegin(), b.cend());
        it.consume_to(b.size_bytes(), buf.get_write());
        return ss::make_ready_future<ss::temporary_buffer<uint8_t>>(
          std::move(buf));
    }
    static ss::logger& logger() {
        static ss::logger lgr{"iobuf_file"};
        return lgr;
    }
    iobuf* _ptr;
};
