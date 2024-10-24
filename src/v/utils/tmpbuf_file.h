#pragma once
#include "base/seastarx.h"
#include "base/units.h"
#include "base/vassert.h"
#include "base/vlog.h"
#include "bytes/iobuf.h"

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/later.hh>

#include <absl/container/btree_map.h>
#include <sys/stat.h>

#include <numeric>
#include <stdexcept>

/**
 * tmpbuf_file::store_t store;
 * auto fd = file(make_shared(tmpbuf_file(store)));
 */
class tmpbuf_file final : public ss::file_impl {
    static constexpr const size_t segment_size = 4_KiB;

public:
    struct store_t {
        absl::btree_map<size_t, ss::temporary_buffer<char>> data;
        size_t size{0};

        iobuf release_iobuf() && {
            iobuf ret;
            for (auto& e : data) {
                ret.append(std::move(e.second));
            }
            ret.trim_back(ret.size_bytes() - size);
            return ret;
        }

        iobuf share_iobuf() {
            iobuf ret;
            for (auto& e : data) {
                ret.append(e.second.share());
            }
            ret.trim_back(ret.size_bytes() - size);
            return ret;
        }
    };

    explicit tmpbuf_file(store_t& store)
      : _store(store) {}

    ss::future<size_t> write_dma(
      const uint64_t pos,
      const void* buffer,
      const size_t len,
      const ss::io_priority_class&) final {
        vlog(logger().info, "write_dma pos {} len {}", pos, len);
        auto written = write(pos, buffer, len);
        _store.size = std::max(_store.size, pos + written);
        return ss::make_ready_future<size_t>(written);
    }

    ss::future<size_t> write_dma(
      const uint64_t pos,
      const std::vector<iovec> iov,
      const ss::io_priority_class&) final {
        vlog(logger().info, "write_iov_dma ({:02}) begin", iov.size());
        size_t written = 0;
        for (auto& io : iov) {
            const auto off = pos + written;
            vlog(logger().info, "write_iov_dma pos {} len {}", off, io.iov_len);
            written += write(off, io.iov_base, io.iov_len);
        }
        _store.size = std::max(_store.size, pos + written);
        return ss::make_ready_future<size_t>(written);
    }

    ss::future<size_t> read_dma(
      const uint64_t pos,
      void* buffer,
      const size_t len,
      const ss::io_priority_class&) final {
        vlog(logger().info, "read_dma pos {} len {}", pos, len);
        auto ret = read(pos, buffer, len);
        return ss::make_ready_future<size_t>(ret);
    }

    ss::future<size_t> read_dma(
      const uint64_t pos,
      const std::vector<iovec> iov,
      const ss::io_priority_class&) final {
        vlog(logger().info, "read_iov_dma ({:02}) begin", iov.size());
        size_t bytes_read = 0;
        for (auto& io : iov) {
            const auto off = pos + bytes_read;
            vlog(logger().info, "read_iov_dma pos {} len {}", off, io.iov_len);
            bytes_read += read(off, io.iov_base, io.iov_len);
        }
        return ss::make_ready_future<size_t>(bytes_read);
    }

    ss::future<> flush() final {
        vlog(logger().info, "flush");
        return ss::now();
    }

    ss::future<struct stat> stat() final {
        vlog(logger().info, "stat");

        struct stat st; // NOLINT
        std::memset(&st, 0, sizeof(st));
        st.st_dev = 0; // NOLINT
        st.st_ino = (decltype(st.st_ino))(&_store);
        st.st_mode = S_IFREG;
        st.st_size = _store.size;
        st.st_blocks = capacity() / 512; // NOLINT

        return ss::make_ready_future<struct stat>(st);
    }

    ss::future<> truncate(const uint64_t size) final {
        vlog(logger().info, "truncate pos {} size {}", size, _store.size);

        if (size > _store.size) {
            auto pos = _store.size ? _store.size - 1 : 0;
            zero(pos, size - _store.size);
        } else if (size != _store.size) {
            auto it = _store.data.lower_bound(size);
            _store.data.erase(it, _store.data.end());
        }

        _store.size = size;

        return ss::now();
    }

    ss::future<> discard(uint64_t pos, uint64_t len) final {
        vlog(logger().info, "discard pos {} len {}", pos, len);
        // arrange for discards past eof to be noop/ignored to avoid extending
        // the capacity of the file while zeroing out the range.
        pos = std::min(pos, _store.size);
        len = std::min(len, _store.size - pos);
        zero(pos, len);
        return ss::now();
    }

    ss::future<> allocate(const uint64_t pos, const uint64_t len) final {
        vlog(logger().info, "allocate pos {} len {}", pos, len);
        // ignore allocations that increase capacity, but honor the zero range
        // flag that seastar adds to fallocate.
        return discard(pos, len);
    }

    ss::future<uint64_t> size() final {
        return ss::make_ready_future<uint64_t>(_store.size);
    }

    ss::future<> close() final { return ss::now(); }

    std::unique_ptr<ss::file_handle_impl> dup() final {
        return std::make_unique<tmpbuf_handle_impl>(this);
    }

    ss::subscription<ss::directory_entry>
    list_directory(std::function<ss::future<>(ss::directory_entry)>) final {
        throw std::system_error(
          std::error_code(ENOTDIR, std::system_category()));
    }

    ss::future<ss::temporary_buffer<uint8_t>> dma_read_bulk(
      const uint64_t pos,
      const size_t len,
      const ss::io_priority_class&) final {
        vlog(logger().info, "dma_read_bulk pos {} len {}", pos, len);
        ss::temporary_buffer<uint8_t> data(std::min(len, _store.size - pos));
        auto bytes_read = read(pos, data.get_write(), len);
        vassert(bytes_read == data.size(), "unexpected read size");
        return ss::make_ready_future<ss::temporary_buffer<uint8_t>>(
          std::move(data));
    }

private:
    class tmpbuf_handle_impl final : public ss::file_handle_impl {
    public:
        explicit tmpbuf_handle_impl(tmpbuf_file* f) noexcept
          : _f(f) {}

        std::unique_ptr<ss::file_handle_impl> clone() const final {
            return std::make_unique<tmpbuf_handle_impl>(_f);
        }

        ss::shared_ptr<file_impl> to_file() && final {
            return ss::make_shared(tmpbuf_file(_f->_store));
        }

    private:
        tmpbuf_file* _f;
    };

    size_t capacity() const { return _store.data.size() * segment_size; }

    void ensure_capacity(size_t size) {
        while (capacity() < size) {
            auto offset = capacity();
            _store.data.emplace(offset, segment_size);
        }
    }

    size_t read(uint64_t pos, const void* buffer, size_t len) {
        if (pos >= _store.size) {
            return 0;
        }

        auto it = _store.data.upper_bound(pos);
        --it;

        size_t bytes_read = 0;
        len = std::min(len, _store.size - pos);
        while (len) {
            vassert(it != _store.data.end(), "reading from invalid iterator");

            auto seg_pos = pos - it->first;
            auto seg_len = std::min(len, segment_size - seg_pos);

            std::memcpy(
              (char*)buffer + bytes_read,
              it->second.get_write() + seg_pos,
              seg_len);

            bytes_read += seg_len;
            pos += seg_len;
            len -= seg_len;

            ++it;
        }

        return bytes_read;
    }

    template<typename Func>
    size_t do_write(uint64_t pos, size_t len, Func copy_func) {
        if (len == 0) {
            // important to avoid a zero-length write at a position past eof
            // expanding capacity below via ensure_capacity.
            return 0;
        }

        // iterator to segment containing pos
        ensure_capacity(pos + len);
        auto it = _store.data.upper_bound(pos);
        --it;

        size_t written = 0;
        while (len) {
            vassert(it != _store.data.end(), "insufficient capacity");

            auto seg_pos = pos - it->first;
            auto seg_len = std::min(len, segment_size - seg_pos);

            // NOLINTNEXTLINE
            copy_func(it->second.get_write() + seg_pos, seg_len, written);

            written += seg_len;
            pos += seg_len;
            len -= seg_len;

            ++it;
        }

        return written;
    }

    // write data to a range
    size_t write(uint64_t pos, const void* buffer, size_t len) {
        return do_write(
          pos, len, [buffer](char* dest, size_t extent, size_t written) {
              std::memcpy(
                dest, (const char*)buffer + written, extent); // NOLINT
          });
    }

    // write zeros to a range
    size_t zero(uint64_t pos, size_t len) {
        return do_write(pos, len, [](char* dest, size_t extent, size_t) {
            std::memset(dest, 0, extent);
        });
    }

    static ss::logger& logger() {
        static ss::logger lgr{"tmpbuf_file"};
        return lgr;
    }

    store_t& _store;
};
