#pragma once

#include "bytes/bytes.h"
#include "seastarx.h"
#include "utils/concepts-enabled.h"

#include <seastar/core/simple-stream.hh>
#include <seastar/core/unaligned.hh>

#include <boost/range/iterator_range.hpp>

/**
 * Utility for writing data into a buffer when its final size is not known up
 * front.
 *
 * Internally the data is written into a vector of chunks allocated on-demand.
 * No resizing of previously written data happens.
 *
 */
class bytes_ostream {
public:
    class fragment {
    public:
        explicit fragment(temporary_buffer<char> buf)
          : _buf(std::move(buf)) {
            _len = _buf.size();
        }
        fragment(temporary_buffer<char> buf, size_t len)
          : _buf(std::move(buf))
          , _len(len) {
        }
        fragment(fragment&& o) noexcept
          : _buf(std::move(o._buf))
          , _len(o._len) {
        }
        fragment& operator=(fragment&& o) noexcept {
            if (this != &o) {
                this->~fragment();
                new (this) fragment(std::move(o));
            }
            return *this;
        }
        ~fragment() = default;
        size_t write(const char* src, size_t len) {
            const size_t sz = std::min(len, capacity() - size());
            std::memcpy(get_current(), src, sz);
            _len += sz;
            return sz;
        }
        const char* get() const {
            return _buf.get();
        }
        char* get_current() {
            return get_write() + size();
        }
        char* get_write() {
            return _buf.get_write();
        }
        /// \brief simply advances the pointer
        /// returning pointer to originating byte
        char* write_place_holder(size_t x) {
            char* tmp = get_current();
            _len += x;
            return tmp;
        }
        size_t size() const {
            return _len;
        }
        size_t capacity() const {
            return _buf.size();
        }
        bool is_full() const {
            return capacity() == size();
        }
        bool operator==(const fragment& o) const {
            return _buf == o._buf;
        }
        bool operator!=(const fragment& o) const {
            return !(*this == o);
        }

    private:
        temporary_buffer<char> _buf;
        size_t _len;
    };
    static constexpr size_t max_chunk_size = 128 * 1024;
    static constexpr size_t default_chunk_size = 512;
    using value_type = char;
    using vector_type = std::vector<fragment>;
    using iterator = typename vector_type::iterator;
    using const_iterator = typename vector_type::const_iterator;

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

    bytes_ostream(bytes_ostream&& o) noexcept
      : _fragment_capacity(o._fragment_capacity)
      , _fragments(std::move(o._fragments))
      , _size(o._size) {
    }

    bytes_ostream(const bytes_ostream& o) = delete;
    bytes_ostream& operator=(const bytes_ostream& o) = delete;

    bytes_ostream& operator=(bytes_ostream&& o) noexcept {
        if (this != &o) {
            this->~bytes_ostream();
            new (this) bytes_ostream(std::move(o));
        }
        return *this;
    }

    [[gnu::always_inline]] value_type* write_place_holder(size_t size) {
        if (current_space_left() < size) {
            const size_t sz = next_alloc_size(_fragment_capacity, size);
            if (sz <= max_chunk_size) {
                _fragment_capacity = sz;
            }
            _fragments.emplace_back(temporary_buffer<char>(sz), 0);
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
        _size += b.size();
        _fragments.emplace_back(std::move(b));
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
                _fragment_capacity = next_alloc_size(
                  _fragment_capacity, _fragment_capacity);
                _fragments.emplace_back(
                  temporary_buffer<char>(_fragment_capacity), 0);
            }
            const size_t sz = _fragments.back().write(
              ptr + i, std::min(size, current_space_left()));
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
        for (size_t i = 0; i < _fragments.size(); ++i) {
            if (_fragments[i] != other._fragments[i]) {
                return false;
            }
        }
        return true;
    }

    bool operator!=(const bytes_ostream& other) const {
        return !(*this == other);
    }

    void clear() {
        _fragments.clear();
        _size = 0;
    }

private:
    size_t current_space_left() {
        if (_fragments.empty()) {
            _fragments.emplace_back(
              temporary_buffer<char>(_fragment_capacity), 0);
        }
        auto& b = _fragments.back();
        return b.capacity() - b.size();
    }
    value_type* current_ptr() {
        return _fragments.back().get_current();
    }

    size_t _fragment_capacity;
    vector_type _fragments;
    size_t _size = 0;
};

