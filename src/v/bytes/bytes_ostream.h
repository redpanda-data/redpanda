#pragma once

#include "bytes/bytes.h"
#include "seastarx.h"
#include "utils/concepts-enabled.h"

#include <seastar/core/simple-stream.hh>
#include <seastar/core/unaligned.hh>

#include <boost/range/iterator_range.hpp>
#include <fmt/format.h>

#include <list>

class bytes_ostream {
public:
    class fragment {
    public:
        struct full {};
        struct empty {};
        fragment(temporary_buffer<char> buf, full)
          : _buf(std::move(buf))
          , _used_bytes(_buf.size()) {
        }
        fragment(temporary_buffer<char> buf, empty)
          : _buf(std::move(buf))
          , _used_bytes(0) {
        }
        fragment(fragment&& o) noexcept = default;
        fragment& operator=(fragment&& o) noexcept = default;
        ~fragment() = default;

        size_t write(const char* src, size_t len) {
            const size_t sz = std::min(len, available_bytes());
            std::copy_n(src, sz, get_current());
            _used_bytes += sz;
            return sz;
        }
        const char* get() const {
            return _buf.get();
        }
        /// \brief simply advances the pointer
        /// returning pointer to originating byte
        char* write_place_holder(size_t x) {
            char* tmp = get_current();
            _used_bytes += x;
            return tmp;
        }
        temporary_buffer<char>&& release() && {
            trim();
            return std::move(_buf);
        }
        size_t available_bytes() const {
            return _buf.size() - _used_bytes;
        }
        size_t size() const {
            return _used_bytes;
        }
        void trim() {
            if (_used_bytes == _buf.size()) {
                return;
            }
            _buf.trim(_used_bytes);
            /*
            // FIXME: #226

            size_t half = _buf.size() / 2;
            if (_used_bytes <= half) {
                // this is an important optimization. often times during RPC
                // serialization we append some small controll bytes, _right_
                // before we append a full new chain of iobufs

                temporary_buffer<char> tmp(_used_bytes);
                std::copy_n(_buf.get(), _used_bytes, tmp.get_write());
                _buf = std::move(tmp);
            } else {
                _buf.trim(_used_bytes);
            }
            */
        }
        bool is_empty() const {
            return _used_bytes == 0;
        }
        bool operator==(const fragment& o) const {
            return _buf == o._buf;
        }
        bool operator!=(const fragment& o) const {
            return !(*this == o);
        }

    private:
        char* get_current() {
            return _buf.get_write() + _used_bytes;
        }
        temporary_buffer<char> _buf;
        size_t _used_bytes;
    };
    static constexpr size_t max_chunk_size = 128 * 1024;
    static constexpr size_t default_chunk_size = 512;
    using value_type = char;
    using container_type = std::list<fragment>;
    using iterator = typename container_type::iterator;
    using const_iterator = typename container_type::const_iterator;

    // Figure out next chunk size.
    //   - must be enough for data_size
    //   - must be at least _fragment_capacity
    //   - try to double each time to prevent too many allocations
    //   - do not exceed max_chunk_size
    static size_t next_alloc_size(size_t current_capacity, size_t data_size) {
        auto next_size = current_capacity * 2;
        next_size = std::min(next_size, max_chunk_size);
        return std::max(next_size, data_size);
    }

    explicit bytes_ostream(size_t initial_chunk_size) noexcept
      : _fragment_capacity(initial_chunk_size) {
    }
    bytes_ostream() noexcept
      : bytes_ostream(default_chunk_size) {
    }

    bytes_ostream(bytes_ostream&& o) noexcept = default;
    bytes_ostream(const bytes_ostream& o) = delete;
    bytes_ostream& operator=(const bytes_ostream& o) = delete;
    bytes_ostream& operator=(bytes_ostream&& o) noexcept = default;

    [[gnu::always_inline]] value_type* write_place_holder(size_t size) {
        if (current_space_left() < size) {
            create_new_fragment(size);
        }
        _size += size;
        return _fragments.back().write_place_holder(size);
    }

    [[gnu::always_inline]] inline void write(bytes_view v) {
        if (v.empty()) {
            return;
        }
        write(reinterpret_cast<const value_type*>(v.begin()), v.size());
    }

    [[gnu::always_inline]] void write(temporary_buffer<char> b) {
        if (b.size() <= current_space_left()) {
            write(b.get(), b.size());
            return;
        }
        if (current_space_left() > 0) {
            if (_fragments.back().is_empty()) {
                _fragments.pop_back();
            } else {
                // happens when we are merge iobufs
                _fragments.back().trim();
                _fragment_capacity = default_chunk_size;
            }
        }
        _size += b.size();
        _fragments.emplace_back(std::move(b), fragment::full{});
    }

    [[gnu::always_inline]] void write(const char* ptr, size_t size) {
        if (size == 0) {
            return;
        }
        if (__builtin_expect(size < current_space_left(), true)) {
            _size += size;
            _fragments.back().write(ptr, size);
            return;
        }
        size_t i = 0;
        while (size > 0) {
            if (current_space_left() == 0) {
                create_new_fragment(size);
            }
            const size_t sz = _fragments.back().write(ptr + i, size);
            _size += sz;
            i += sz;
            size -= sz;
        }
    }

    // Returns the amount of bytes written so far
    size_t size_bytes() const {
        return _size;
    }

    bool empty() const {
        return _fragments.empty();
    }

    iterator begin() {
        return _fragments.begin();
    }
    iterator end() {
        return _fragments.end();
    }
    const_iterator begin() const {
        return _fragments.begin();
    }
    const_iterator end() const {
        return _fragments.end();
    }

    bool operator==(const bytes_ostream& other) const {
        if (
          _size != other._size
          || _fragments.size() != other._fragments.size()) {
            return false;
        }
        return _fragments == other._fragments;
    }

    bool operator!=(const bytes_ostream& other) const {
        return !(*this == other);
    }

    void clear() {
        _fragment_capacity = default_chunk_size;
        _fragments.clear();
        _size = 0;
    }

    container_type&& release() && {
        return std::move(_fragments);
    }

private:
    size_t current_space_left() const {
        if (_fragments.empty()) {
            return 0;
        }
        auto& b = _fragments.back();
        return b.available_bytes();
    }
    void create_new_fragment(size_t size) {
        if (__builtin_expect(current_space_left() != 0, false)) {
            throw std::runtime_error(fmt::format(
              "Must utilize full size, before adding new segment. bytes "
              "left:{}",
              current_space_left()));
        }
        _fragment_capacity = next_alloc_size(_fragment_capacity, size);
        _fragments.emplace_back(
          temporary_buffer<char>(_fragment_capacity), fragment::empty{});
    }

    size_t _fragment_capacity;
    container_type _fragments;
    size_t _size = 0;
};
