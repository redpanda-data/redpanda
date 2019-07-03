#pragma once

#include "bytes/bytes.h"
#include "utils/concepts-enabled.h"
#include "utils/fragment_range.h"

#include <seastar/core/simple-stream.hh>
#include <seastar/core/unaligned.hh>

#include <boost/range/iterator_range.hpp>

/**
 * Utility for writing data into a buffer when its final size is not known up
 * front.
 *
 * Internally the data is written into a chain of chunks allocated on-demand.
 * No resizing of previously written data happens.
 *
 * FIXME: Move to seastar.
 */
class bytes_ostream {
public:
    using size_type = bytes::size_type;
    using value_type = bytes::value_type;
    static constexpr size_type max_chunk_size() {
        return 128 * 1024;
    }
    using fragment_type = bytes_view;

private:
    static_assert(
      sizeof(value_type) == 1, "value_type is assumed to be one byte long");
    struct chunk {
        // FIXME: group fragment pointers to reduce pointer chasing when
        // packing.
        std::unique_ptr<chunk> next;
        ~chunk() {
            auto p = std::move(next);
            while (p) {
                // Avoid recursion when freeing chunks
                auto p_next = std::move(p->next);
                p = std::move(p_next);
            }
        }
        size_type offset; // Also means "size" after chunk is closed
        size_type size;
        value_type data[0];
        void operator delete(void* ptr) {
            free(ptr);
        }
    };

    static constexpr size_type default_chunk_size{512};

    inline size_type current_space_left() const {
        if (!_current) {
            return 0;
        }
        return _current->size - _current->offset;
    }

    // Figure out next chunk size.
    //   - must be enough for data_size
    //   - must be at least _initial_chunk_size
    //   - try to double each time to prevent too many allocations
    //   - do not exceed max_chunk_size
    size_type next_alloc_size(size_t data_size) const {
        auto next_size = _current ? _current->size * 2 : _initial_chunk_size;
        next_size = std::min(next_size, max_chunk_size());
        // FIXME: check for overflow?
        return std::max<size_type>(next_size, data_size + sizeof(chunk));
    }

    // Makes room for a contiguous region of given size.
    // The region is accounted for as already written.
    // size must not be zero.
    [[gnu::always_inline]] value_type* alloc(size_type size) {
        if (__builtin_expect(size <= current_space_left(), true)) {
            auto ret = _current->data + _current->offset;
            _current->offset += size;
            _size += size;
            return ret;
        } else {
            return alloc_new(size);
        }
    }

    [[gnu::noinline]] value_type* alloc_new(size_type size) {
        auto alloc_size = next_alloc_size(size);
        auto space = malloc(alloc_size);
        if (!space) {
            throw std::bad_alloc();
        }
        auto new_chunk = std::unique_ptr<chunk>(new (space) chunk());
        new_chunk->offset = size;
        new_chunk->size = alloc_size - sizeof(chunk);
        if (_current) {
            _current->next = std::move(new_chunk);
            _current = _current->next.get();
        } else {
            _begin = std::move(new_chunk);
            _current = _begin.get();
        }
        _size += size;
        return _current->data;
    }

public:
    class fragment_iterator
      : public std::iterator<std::input_iterator_tag, bytes_view> {
        chunk* _current = nullptr;

    public:
        fragment_iterator() = default;
        fragment_iterator(chunk* current)
          : _current(current) {
        }
        fragment_iterator(const fragment_iterator&) = default;
        fragment_iterator& operator=(const fragment_iterator&) = default;

        bytes_view operator*() const {
            return {_current->data, _current->offset};
        }

        bytes_view operator->() const {
            return *(*this);
        }

        fragment_iterator& operator++() {
            _current = _current->next.get();
            return *this;
        }

        fragment_iterator operator++(int) {
            fragment_iterator tmp(*this);
            ++(*this);
            return tmp;
        }

        bool operator==(const fragment_iterator& other) const {
            return _current == other._current;
        }

        bool operator!=(const fragment_iterator& other) const {
            return _current != other._current;
        }
    };

public:
    explicit bytes_ostream(size_t initial_chunk_size) noexcept
      : _begin()
      , _current(nullptr)
      , _size(0)
      , _initial_chunk_size(initial_chunk_size) {
    }

    bytes_ostream() noexcept
      : bytes_ostream(default_chunk_size) {
    }

    bytes_ostream(bytes_ostream&& o) noexcept
      : _begin(std::move(o._begin))
      , _current(o._current)
      , _size(o._size)
      , _initial_chunk_size(o._initial_chunk_size) {
        o._current = nullptr;
        o._size = 0;
    }

    bytes_ostream(const bytes_ostream& o)
      : _begin()
      , _current(nullptr)
      , _size(0)
      , _initial_chunk_size(o._initial_chunk_size) {
        append(o);
    }

    bytes_ostream& operator=(const bytes_ostream& o) {
        if (this != &o) {
            auto x = bytes_ostream(o);
            *this = std::move(x);
        }
        return *this;
    }

    bytes_ostream& operator=(bytes_ostream&& o) noexcept {
        if (this != &o) {
            this->~bytes_ostream();
            new (this) bytes_ostream(std::move(o));
        }
        return *this;
    }

    [[gnu::always_inline]] value_type* write_place_holder(size_type size) {
        return alloc(size);
    }

    [[gnu::always_inline]] inline void write_non_empty(bytes_view&& v) {
        auto this_size = std::min(v.size(), size_t(current_space_left()));
        if (__builtin_expect(this_size, true)) {
            memcpy(_current->data + _current->offset, v.begin(), this_size);
            _current->offset += this_size;
            _size += this_size;
            v.remove_prefix(this_size);
        }

        while (!v.empty()) {
            auto this_size = std::min(v.size(), size_t(max_chunk_size()));
            std::copy_n(v.begin(), this_size, alloc_new(this_size));
            v.remove_prefix(this_size);
        }
    }

    [[gnu::always_inline]] inline void write(bytes_view v) {
        if (v.empty()) {
            return;
        }
        write_non_empty(std::move(v));
    }

    [[gnu::always_inline]] void write(const char* ptr, size_t size) {
        write(bytes_view(reinterpret_cast<const signed char*>(ptr), size));
    }

    [[gnu::always_inline]] void write_non_empty(const char* ptr, size_t size) {
        write(bytes_view(reinterpret_cast<const signed char*>(ptr), size));
    }

    // Returns the amount of bytes written so far
    size_type size_bytes() const {
        return _size;
    }

    bool empty() const {
        return _size == 0;
    }

    bool is_linearized() const {
        return !_begin || !_begin->next;
    }

    // Call only when is_linearized()
    bytes_view view() const {
        assert(is_linearized());
        if (!_current) {
            return bytes_view();
        }

        return bytes_view(_current->data, _size);
    }

    // Makes the underlying storage contiguous and returns a view to it.
    // Useful if an interface can't accept fragments and must instead deal
    // with a contiguous block of memory (e.g., std::copy, C interfaces, etc.)
    // Invalidates all previously created placeholders.
    bytes_view linearize() {
        if (is_linearized()) {
            return view();
        }

        auto space = malloc(_size + sizeof(chunk));
        if (!space) {
            throw std::bad_alloc();
        }

        auto new_chunk = std::unique_ptr<chunk>(new (space) chunk());
        new_chunk->offset = _size;
        new_chunk->size = _size;

        auto dst = new_chunk->data;
        auto r = _begin.get();
        while (r) {
            auto next = r->next.get();
            dst = std::copy_n(r->data, r->offset, dst);
            r = next;
        }

        _current = new_chunk.get();
        _begin = std::move(new_chunk);
        return bytes_view(_current->data, _size);
    }

    void append(const bytes_ostream& o) {
        for (auto&& bv : o.fragments()) {
            write(std::move(bv));
        }
    }

    // Removes n bytes from the end of the bytes_ostream.
    // Beware of O(n) algorithm.
    void remove_suffix(size_t n) {
        _size -= n;
        auto left = _size;
        auto current = _begin.get();
        while (current) {
            if (current->offset >= left) {
                current->offset = left;
                _current = current;
                current->next.reset();
                return;
            }
            left -= current->offset;
            current = current->next.get();
        }
    }

    // begin() and end() form an input range to bytes_view representing
    // fragments. Any modification of this instance invalidates iterators.
    fragment_iterator begin() const {
        return {_begin.get()};
    }
    fragment_iterator end() const {
        return {nullptr};
    }

    boost::iterator_range<fragment_iterator> fragments() const {
        return {begin(), end()};
    }

    bool operator==(const bytes_ostream& other) const {
        auto as = fragments().begin();
        auto as_end = fragments().end();
        auto bs = other.fragments().begin();
        auto bs_end = other.fragments().end();

        auto a = *as++;
        auto b = *bs++;
        while (!a.empty() || !b.empty()) {
            auto now = std::min(a.size(), b.size());
            if (!std::equal(
                  a.begin(), a.begin() + now, b.begin(), b.begin() + now)) {
                return false;
            }
            a.remove_prefix(now);
            if (a.empty() && as != as_end) {
                a = *as++;
            }
            b.remove_prefix(now);
            if (b.empty() && bs != bs_end) {
                b = *bs++;
            }
        }
        return true;
    }

    bool operator!=(const bytes_ostream& other) const {
        return !(*this == other);
    }

    // Makes this instance empty.
    //
    // The first buffer is not deallocated, so callers may rely on the
    // fact that if they write less than the initial chunk size between
    // the clear() calls then writes will not involve any memory allocations,
    // except for the first write made on this instance.
    void clear() {
        if (_begin) {
            _begin->offset = 0;
            _size = 0;
            _current = _begin.get();
            _begin->next.reset();
        }
    }

private:
    std::unique_ptr<chunk> _begin;
    chunk* _current;
    size_type _size;
    size_type _initial_chunk_size = default_chunk_size;
};

CONCEPT(static_assert(FragmentRange<bytes_ostream>));
