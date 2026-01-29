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
#include "base/likely.h"
#include "base/seastarx.h"
#include "bytes/details/io_allocation_size.h"
#include "bytes/details/io_byte_iterator.h"
#include "bytes/details/io_fragment.h"
#include "bytes/details/io_iterator_consumer.h"
#include "bytes/details/io_placeholder.h"

#include <seastar/core/temporary_buffer.hh>

#include <array>
#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <string_view>
#include <type_traits>

/*
 * Our iobuf is a fragmented buffer modeled after
 * folly::iobufqueue.h - it supports prepend and append, but no
 * operations in the middle. It provides a forward iterator for
 * byte scanning and parsing. This is intended to be the workhorse
 * of our data path.
 * Noteworthy Operations:
 * Append/Prepend - O(1)
 * operator==, operator!=  - O(N)
 *
 * General sharing-mutation caveat:
 *
 * Operations such as share(), copy() and appending an iobuf or other compatible
 * buffer type to an iobuf may be zero-copy, in the sense that some or all of
 * the payload bytes may be shared between multiple iobufs (or between an iobuf
 * and a compatible buffer type like ss::temporary_buffer<>). The sharing occurs
 * at the fragment level.
 *
 * Be careful when any zero-copy operations are used as iobuf
 * does not perform copy-on-write, Therefore changes will be visible to all
 * iobufs that share the backing fragments.
 */
class iobuf {
    // Not a lightweight object.
    // 16 bytes for io_fragment_list
    // 8  bytes for _size_bytes
    // -----------------------
    //
    // 24 bytes total.

    // Each fragment:
    // 24  of ss::temporary_buffer<>
    // 16  for prev,next pointers
    // 8   for consumed capacity
    // -----------------------
    //
    // 48 bytes total

public:
    using fragment = details::io_fragment;
    using container = details::io_fragment_list;
    using iterator = typename container::iterator;
    using reverse_iterator = typename container::reverse_iterator;
    using const_iterator = typename container::const_iterator;
    using iterator_consumer = details::io_iterator_consumer;
    using byte_iterator = details::io_byte_iterator;
    using placeholder = details::io_placeholder;

    static iobuf from(std::string_view view) {
        iobuf i;
        i.append_str(view);
        return i;
    }

    iobuf() noexcept = default;
    ~iobuf() noexcept;
    iobuf(iobuf&& x) noexcept
      : _frags(std::move(x._frags))
      , _size(std::exchange(x._size, 0)) {}

    iobuf& operator=(iobuf&& x) noexcept {
        if (this != &x) {
            this->~iobuf();
            new (this) iobuf(std::move(x));
        }
        return *this;
    }
    iobuf(const iobuf&) = delete;
    iobuf& operator=(const iobuf&) = delete;

    /// override to pass in any container of temp bufs
    template<
      typename Range,
      typename = std::enable_if<
        std::is_same_v<typename Range::value_type, ss::temporary_buffer<char>>>>
    explicit iobuf(Range&& r) {
        static_assert(
          std::is_rvalue_reference_v<decltype(r)>,
          "Must be an rvalue. Use std::move()");
        for (auto& buf : r) {
            append(std::move(buf));
        }
    }

    /**
     * Returns a new iobuf of length len with the contents of this iobuf
     * starting at offset pos.
     *
     * This is a zero-copy operation: the returned iobuf will share a subset of
     * (depending on len and pos) the fragments of this iobuf. Currently no
     * linearization is performed: the sequence and sizes of fragments of the
     * copy will be the same as this iobuf, but callers should not rely on the
     * precise details.
     *
     * Since this call performs zero-copy operations, the sharing-mutation
     * caveat in the class comment applies.
     */
    iobuf share();
    iobuf share(size_t pos, size_t len);

    /**
     * Returns a copy of this iobuf. This is NOT a zero-copy operation: the
     * returned iobuf is the unique owner of all of its fragments, so any
     * mutations to the payload bytes of this iobuf do not affected the returned
     * value or vice-versa.
     *
     * Copying an iobuf is optimized for cases where the size of the resulting
     * iobuf will not be increased (e.g. via iobuf::append).
     */
    iobuf copy() const;

    /// makes a reservation with the internal storage. adds a layer of
    /// indirection instead of raw byte pointer to allow the
    /// details::io_fragments to internally compact buffers as long as they
    /// don't violate the reservation size here
    placeholder reserve(size_t reservation);

    /// only ensures that a segment of at least reservation is available
    /// as an empty details::io_fragment
    void reserve_memory(size_t reservation);

    /*
     * Append len bytes starting at src into this iobuf. This always makes
     * a copy of the source bytes.
     */
    void append(const char*, size_t);

    /*
     * Append len bytes starting at src into this iobuf. This always makes
     * a copy of the source bytes.
     */
    void append(const uint8_t*, size_t);

    /**
     * A helper to append a container of uint8_t or char to this iobuf. This
     * always makes a copy of the source bytes.
     */
    template<typename T, size_t S>
    void append(const std::array<T, S>& a) {
        static_assert(
          std::is_same_v<std::remove_const_t<T>, uint8_t>
          || std::is_same_v<std::remove_const_t<T>, char>);
        append(a.data(), a.size());
    }

    /**
     * A helper to append a string_view to this iobuf. This always makes a copy
     * of the source bytes.
     */
    void append_str(std::string_view str) { append(str.data(), str.size()); }

    /**
     * Appends the contents of the passed buffer to this one.
     *
     * This may copy the contents of the buffer into this one, creating new
     * fragments as necessary, or it may zero-copy link the source buffer into
     * this iobuf as a new fragment.
     *
     * The choice depends on the relative sizes
     * of the source buffer and this object and may change in the future.
     *
     * Since this call may perform zero-copy operations, the sharing-mutation
     * caveat in the class comment applies.
     */
    void append(ss::temporary_buffer<char>);

    /**
     * Appends the contents of the passed buffer to this one.
     *
     * This may copy the contents of the buffer into this one, creating new
     * fragments as necessary, or it may zero-copy link some of all of the
     * fragments in the source into this one.
     *
     * The choice depends on the relative sizes
     * of the source buffer and this object and may change in the future.
     *
     * Since this call may perform zero-copy operations, the sharing-mutation
     * caveat in the class comment applies.
     */
    void append(iobuf&&);

    /*
     * Appends the contents of the passed buffer to this one.
     *
     * This differs from append(iobuf) in that it always uses zero-copy: all
     * fragments from the source are appended as new fragments to this buffer
     * without needing to make a copy of their contents.
     *
     * Since this call performs zero-copy operations, the sharing-mutation
     * caveat in the class comment applies.
     */
    void append_fragments(iobuf);

    /**
     * Append a fragment to this iobuf. This takes ownership of the fragment
     * and is a zero-copy operation.
     */
    void append(std::unique_ptr<fragment>);

    /**
     * Share the last `size` bytes from the iobuf to create a new iobuf.
     *
     * This is an optimized version of:
     * `iobuf::share(iobuf.size_bytes() - size, size)`
     */
    iobuf tail(size_t size);

    /// prepends the _the buffer_ as iobuf::details::io_fragment::full{}
    void prepend(ss::temporary_buffer<char>);
    /// prepends the arg to this as iobuf::details::io_fragment::full{}
    void prepend(iobuf);
    /// used for iostreams
    void pop_front();
    void trim_front(size_t n);
    void pop_back();
    void trim_back(size_t n);
    void clear();
    size_t size_bytes() const;
    // The approximate amount of memory being used.
    size_t memory_usage() const { return sizeof(*this) + size_bytes(); }
    bool empty() const;
    /// compares that the _content_ is the same;
    /// ignores allocation strategy, and number of details::io_fragments
    /// it is a byte-per-byte comparator
    bool operator==(const iobuf&) const;
    bool operator<(const iobuf&) const;
    bool operator!=(const iobuf&) const;
    /// Does an unsigned byte-wise comparison between this iobuf and the other.
    std::strong_ordering operator<=>(const iobuf&) const;

    bool operator==(std::string_view) const;
    bool operator!=(std::string_view) const;
    std::strong_ordering operator<=>(std::string_view) const;

    iterator begin();
    iterator end();
    reverse_iterator rbegin();
    reverse_iterator rend();
    const_iterator begin() const;
    const_iterator end() const;
    const_iterator cbegin() const;
    const_iterator cend() const;

    std::string hexdump(size_t) const;

    // Linearize this iobuf to a string. Note that if the iobuf is over 128KiB
    // this method will throw as to not cause an oversized allocation.
    ss::sstring linearize_to_string() const;

private:
    void prepend(std::unique_ptr<fragment>);

    size_t available_bytes() const;
    void create_new_fragment(size_t);
    size_t last_allocation_size() const;

    bool try_copy_append(const char* ptr, size_t size);
    // the pointer passed to this function should be "owning".
    // iobuf will manage it's lifetime after its been passed.
    // this function is only used internally by other `iobuf::append`
    // functions in order to centralize some logic.
    void append(fragment*);

    container _frags;
    size_t _size{0};
    friend std::ostream& operator<<(std::ostream&, const iobuf&);
};

inline void iobuf::clear() {
    _frags.clear_and_dispose(&details::dispose_io_fragment);
    _size = 0;
}
inline iobuf::~iobuf() noexcept { clear(); }
inline iobuf::iterator iobuf::begin() { return _frags.begin(); }
inline iobuf::iterator iobuf::end() { return _frags.end(); }
inline iobuf::reverse_iterator iobuf::rbegin() { return _frags.rbegin(); }
inline iobuf::reverse_iterator iobuf::rend() { return _frags.rend(); }
inline iobuf::const_iterator iobuf::begin() const { return _frags.cbegin(); }
inline iobuf::const_iterator iobuf::end() const { return _frags.cend(); }
inline iobuf::const_iterator iobuf::cbegin() const { return _frags.cbegin(); }
inline iobuf::const_iterator iobuf::cend() const { return _frags.cend(); }

inline bool iobuf::operator!=(const iobuf& o) const { return !(*this == o); }
inline bool iobuf::operator!=(std::string_view o) const {
    return !(*this == o);
}
inline bool iobuf::empty() const { return _size == 0; }
inline size_t iobuf::size_bytes() const { return _size; }

inline size_t iobuf::available_bytes() const {
    if (_frags.empty()) {
        return 0;
    }
    return _frags.back().available_bytes();
}

inline size_t iobuf::last_allocation_size() const {
    return _frags.empty() ? details::io_allocation_size::default_chunk_size
                          : _frags.back().capacity();
}
inline void iobuf::append(fragment* f) {
    if (!_frags.empty()) {
        _frags.back().trim();
    }
    // NOTE: this _must_ be size and _not_ capacity
    _size += f->size();
    _frags.push_back(*f);
}
inline void iobuf::append(std::unique_ptr<fragment> f) { append(f.release()); }
inline void iobuf::prepend(std::unique_ptr<fragment> f) {
    _size += f->size();
    _frags.push_front(*f.release());
}

inline void iobuf::create_new_fragment(size_t sz) {
    auto chunk_max = std::max(sz, last_allocation_size());
    auto asz = details::io_allocation_size::next_allocation_size(chunk_max);
    append(std::make_unique<fragment>(asz));
}
/// only ensures that a segment of at least reservation is avaible
/// as an empty details::io_fragment
inline void iobuf::reserve_memory(size_t reservation) {
    if (auto b = available_bytes(); b < reservation) {
        if (b > 0) {
            _frags.back().trim();
        }
        create_new_fragment(reservation); // make space if not enough
    }
}

[[gnu::always_inline]] inline void
iobuf::prepend(ss::temporary_buffer<char> b) {
    if (unlikely(!b.size())) {
        return;
    }
    prepend(std::make_unique<fragment>(std::move(b)));
}
[[gnu::always_inline]] inline void iobuf::prepend(iobuf b) {
    while (!b._frags.empty()) {
        fragment* f = &b._frags.back();
        b._frags.pop_back();
        prepend(f->share());
        details::dispose_io_fragment(f);
    }
}
/// append src + len into storage
[[gnu::always_inline]] inline void
iobuf::append(const uint8_t* src, size_t len) {
    // NOLINTNEXTLINE
    append(reinterpret_cast<const char*>(src), len);
}

[[gnu::always_inline]] inline void iobuf::append(const char* ptr, size_t size) {
    if (unlikely(size == 0)) {
        return;
    }
    if (likely(size <= available_bytes())) {
        _size += _frags.back().append(ptr, size);
        return;
    }
    size_t i = 0;
    while (size > 0) {
        if (available_bytes() == 0) {
            create_new_fragment(size);
        }
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        const size_t sz = _frags.back().append(ptr + i, size);
        _size += sz;
        i += sz;
        size -= sz;
    }
}

// tries to copy the buffer into the iobuf if the heuristic described below is
// satisfied
[[gnu::always_inline]] inline bool
iobuf::try_copy_append(const char* ptr, size_t size) {
    const size_t last_asz = last_allocation_size();
    // The following is a heuristic to decide between copying and zero-copy
    // append of the source buffer. The rule we apply is if the buffer we are
    // appending is of the same size or smaller than last allocation we use
    // a copy. This effecitvely linearizes a series of smaller buffers when
    // they are appended to this one. However, if the incoming buffer is
    // already at (or above) the maximum fragment size, we use zero copy as
    // copying it provides almost no linearization benefits and appending
    // full-sized fragments is in practice a common operation when buffers
    // grow beyond the maximum fragment size.
    if (
      size <= last_asz && size < details::io_allocation_size::max_chunk_size) {
        append(ptr, size);
        return true;
    }

    return false;
}

/// appends the contents of buffer; might pack values into existing space
[[gnu::always_inline]] inline void iobuf::append(ss::temporary_buffer<char> b) {
    if (unlikely(!b.size())) {
        return;
    }

    if (try_copy_append(b.get(), b.size())) {
        return;
    }

    if (unlikely(available_bytes() > 0 && _frags.back().is_empty())) {
        pop_back();
    }
    append(std::make_unique<fragment>(std::move(b)));
}

/// appends the contents of buffer; might pack values into existing space
inline void iobuf::append(iobuf&& o) {
    while (!o._frags.empty()) {
        fragment* f = &o._frags.front();
        o._frags.pop_front();

        auto fsize = f->size();
        if (!fsize || try_copy_append(f->get(), fsize)) {
            details::dispose_io_fragment(f);
            continue;
        }

        if (unlikely(!_frags.empty() && _frags.back().is_empty())) {
            pop_back();
        }

        append(f);
    }
    o.clear();
}

inline void iobuf::append_fragments(iobuf o) {
    while (!o._frags.empty()) {
        fragment* f = &o._frags.front();
        o._frags.pop_front();
        append(std::make_unique<fragment>(f->share()));
        details::dispose_io_fragment(f);
    }
}
/// used for iostreams
inline void iobuf::pop_front() {
    fragment* f = &_frags.front();
    _size -= f->size();
    _frags.pop_front();
    details::dispose_io_fragment(f);
}
inline void iobuf::pop_back() {
    fragment* f = &_frags.back();
    _size -= f->size();
    _frags.pop_back();
    details::dispose_io_fragment(f);
}
inline void iobuf::trim_front(size_t n) {
    while (!_frags.empty()) {
        auto& f = _frags.front();
        if (f.size() > n) {
            _size -= n;
            f.trim_front(n);
            return;
        }
        n -= f.size();
        pop_front();
    }
}
inline void iobuf::trim_back(size_t n) {
    while (!_frags.empty()) {
        auto& f = _frags.back();
        if (f.size() > n) {
            _size -= n;
            f.trim(f.size() - n);
            return;
        }
        n -= f.size();
        pop_back();
    }
}

iobuf iobuf_copy(iobuf::iterator_consumer& in, size_t len);
