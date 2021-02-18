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
#include "bytes/details/io_allocation_size.h"
#include "bytes/details/io_byte_iterator.h"
#include "bytes/details/io_fragment.h"
#include "bytes/details/io_iterator_consumer.h"
#include "bytes/details/io_placeholder.h"
#include "bytes/details/out_of_range.h"
#include "likely.h"
#include "oncore.h"
#include "seastarx.h"
#include "utils/intrusive_list_helpers.h"
#include "vassert.h"

#include <seastar/core/iostream.hh>
#include <seastar/core/scattered_message.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/temporary_buffer.hh>

#include <boost/container_hash/hash.hpp> // hash_combine

#include <cstddef>
#include <iosfwd>
#include <string_view>
#include <type_traits>

/// our iobuf is a fragmented buffer. modeled after
/// folly::iobufqueue.h - it supports prepend and append, but no
/// operations in the middle. It provides a forward iterator for
/// byte scanning and parsing. This is intended to be the workhorse
/// of our data path.
/// Noteworthy Operations:
/// Append/Prepend - O(1)
/// operator==, operator!=  - O(N)
///
class iobuf {
    // Not a lightweight object.
    // 16 bytes for std::list
    // 8  bytes for _size_bytes
    // -----------------------
    //
    // 24 bytes total.

    // Each fragment:
    // 24  of ss::temporary_buffer<>
    // 16  for left,right pointers
    // 8   for consumed capacity
    // -----------------------
    //
    // 48 bytes total

public:
    using fragment = details::io_fragment;
    using container = uncounted_intrusive_list<fragment, &fragment::hook>;
    using iterator = typename container::iterator;
    using reverse_iterator = typename container::reverse_iterator;
    using const_iterator = typename container::const_iterator;
    using iterator_consumer = details::io_iterator_consumer;
    using byte_iterator = details::io_byte_iterator;
    using placeholder = details::io_placeholder;

    // NOLINTNEXTLINE
    iobuf() noexcept {
        // nothing allocates memory, but boost intrusive list is not marked as
        // noexcept
    }
    ~iobuf() noexcept;
    iobuf(iobuf&& x) noexcept
      : _frags(std::move(x._frags))
      , _size(x._size)
#ifndef NDEBUG
      , _verify_shard(std::move(x._verify_shard))
#endif
    {
        x._frags = container{};
        x._size = 0;
    }
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

    /// shares the underlying temporary buffers
    iobuf share(size_t pos, size_t len);

    /**
     * Copying an iobuf is optimized for cases where the size of the resulting
     * iobuf will not be increased (e.g. via iobuf::append).
     */
    iobuf copy() const;

    /// makes a reservation with the internal storage. adds a layer of
    /// indirection instead of raw byte pointer to allow the
    /// details::io_fragments to internally compact buffers as long as they
    /// don't violate the reservation size here
    placeholder reserve(size_t reservation);

    /// only ensures that a segment of at least reservation is avaible
    /// as an empty details::io_fragment
    void reserve_memory(size_t reservation);

    /// append src + len into storage
    void append(const char*, size_t);
    /// append src + len into storage
    void append(const uint8_t*, size_t);
    /// appends the contents of buffer; might pack values into existing space
    void append(ss::temporary_buffer<char>);
    /// appends the contents of buffer; might pack values into existing space
    void append(iobuf);
    /// \brief trims the back, and appends direct.
    void append_take_ownership(fragment*);
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
    bool empty() const;
    /// compares that the _content_ is the same;
    /// ignores allocation strategy, and number of details::io_fragments
    /// it is a byte-per-byte comparator
    bool operator==(const iobuf&) const;
    bool operator!=(const iobuf&) const;

    bool operator==(std::string_view) const;
    bool operator!=(std::string_view) const;

    iterator begin();
    iterator end();
    reverse_iterator rbegin();
    reverse_iterator rend();
    const_iterator begin() const;
    const_iterator end() const;
    const_iterator cbegin() const;
    const_iterator cend() const;

private:
    /// \brief trims the back, and appends direct.
    void prepend_take_ownership(fragment*);

    size_t available_bytes() const;
    void create_new_fragment(size_t);
    size_t last_allocation_size() const;

    container _frags;
    size_t _size{0};
    expression_in_debug_mode(oncore _verify_shard);
    friend std::ostream& operator<<(std::ostream&, const iobuf&);
};

inline void iobuf::clear() {
    _frags.clear_and_dispose([](fragment* f) {
        delete f; // NOLINT
    });
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
inline bool iobuf::empty() const { return _frags.empty(); }
inline size_t iobuf::size_bytes() const { return _size; }

inline size_t iobuf::available_bytes() const {
    oncore_debug_verify(_verify_shard);
    if (_frags.empty()) {
        return 0;
    }
    return _frags.back().available_bytes();
}

inline size_t iobuf::last_allocation_size() const {
    return _frags.empty() ? details::io_allocation_size::default_chunk_size
                          : _frags.back().capacity();
}
/// \brief trims the back, and appends direct.
inline void iobuf::append_take_ownership(fragment* f) {
    if (!_frags.empty()) {
        _frags.back().trim();
    }
    // NOTE: this _must_ be size and _not_ capacity
    _size += f->size();
    _frags.push_back(*f);
}
inline void iobuf::prepend_take_ownership(fragment* f) {
    _size += f->size();
    _frags.push_front(*f);
}

inline void iobuf::create_new_fragment(size_t sz) {
    oncore_debug_verify(_verify_shard);
    auto chunk_max = std::max(sz, last_allocation_size());
    auto asz = details::io_allocation_size::next_allocation_size(chunk_max);
    auto f = new fragment(ss::temporary_buffer<char>(asz), fragment::empty{});
    append_take_ownership(f);
}
inline iobuf::placeholder iobuf::reserve(size_t sz) {
    oncore_debug_verify(_verify_shard);
    vassert(sz, "zero length reservations are unsupported");
    reserve_memory(sz);
    _size += sz;
    auto it = std::prev(_frags.end());
    placeholder p(it, it->size(), sz);
    it->reserve(sz);
    return p;
}
/// only ensures that a segment of at least reservation is avaible
/// as an empty details::io_fragment
inline void iobuf::reserve_memory(size_t reservation) {
    oncore_debug_verify(_verify_shard);
    if (auto b = available_bytes(); b < reservation) {
        if (b > 0) {
            _frags.back().trim();
        }
        create_new_fragment(reservation); // make space if not enough
    }
}

[[gnu::always_inline]] void inline iobuf::prepend(
  ss::temporary_buffer<char> b) {
    if (unlikely(!b.size())) {
        return;
    }
    auto f = new fragment(std::move(b), fragment::full{});
    prepend_take_ownership(f);
}
[[gnu::always_inline]] void inline iobuf::prepend(iobuf b) {
    oncore_debug_verify(_verify_shard);
    while (!b._frags.empty()) {
        b._frags.pop_back_and_dispose([this](fragment* f) {
            prepend(f->share());
            delete f; // NOLINT
        });
    }
}
/// append src + len into storage
[[gnu::always_inline]] void inline iobuf::append(
  const uint8_t* src, size_t len) {
    // NOLINTNEXTLINE
    append(reinterpret_cast<const char*>(src), len);
}

[[gnu::always_inline]] void inline iobuf::append(const char* ptr, size_t size) {
    if (unlikely(!size)) {
        return;
    }
    oncore_debug_verify(_verify_shard);
    if (size == 0) {
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

/// appends the contents of buffer; might pack values into existing space
[[gnu::always_inline]] inline void iobuf::append(ss::temporary_buffer<char> b) {
    if (unlikely(!b.size())) {
        return;
    }
    oncore_debug_verify(_verify_shard);
    const size_t last_asz = last_allocation_size();
    if (b.size() <= available_bytes() || b.size() <= last_asz) {
        append(b.get(), b.size());
        return;
    }
    if (available_bytes() > 0) {
        if (_frags.back().is_empty()) {
            pop_back();
        } else {
            _frags.back().trim();
        }
    }
    // intrusive list manages the lifetime
    auto f = new fragment(std::move(b), fragment::full{});
    append_take_ownership(f);
}
/// appends the contents of buffer; might pack values into existing space
inline void iobuf::append(iobuf o) {
    oncore_debug_verify(_verify_shard);
    while (!o._frags.empty()) {
        o._frags.pop_front_and_dispose([this](fragment* f) {
            append(f->share());
            delete f; // NOLINT
        });
    }
}
/// used for iostreams
inline void iobuf::pop_front() {
    oncore_debug_verify(_verify_shard);
    _size -= _frags.front().size();
    _frags.pop_front_and_dispose([](fragment* f) {
        delete f; // NOLINT
    });
}
inline void iobuf::pop_back() {
    oncore_debug_verify(_verify_shard);
    _size -= _frags.back().size();
    _frags.pop_back_and_dispose([](fragment* f) {
        delete f; // NOLINT
    });
}
inline void iobuf::trim_front(size_t n) {
    oncore_debug_verify(_verify_shard);
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
    oncore_debug_verify(_verify_shard);
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

/// \brief wraps an iobuf so it can be used as an input stream data source
ss::input_stream<char> make_iobuf_input_stream(iobuf io);

/// \brief wraps the iobuf to be used as an output stream sink
ss::output_stream<char> make_iobuf_ref_output_stream(iobuf& io);

/// \brief exactly like input_stream<char>::read_exactly but returns iobuf
ss::future<iobuf> read_iobuf_exactly(ss::input_stream<char>& in, size_t n);

/// \brief keeps the iobuf in the deferred destructor of scattered_msg<char>
/// and wraps each details::io_fragment as a scattered_message<char>::static()
/// const char*
ss::scattered_message<char> iobuf_as_scattered(iobuf b);

ss::future<> write_iobuf_to_output_stream(iobuf, ss::output_stream<char>&);

iobuf iobuf_copy(iobuf::iterator_consumer& in, size_t len);

namespace std {
template<>
struct hash<::iobuf> {
    size_t operator()(const ::iobuf& b) const {
        size_t h = 0;
        for (auto& f : b) {
            boost::hash_combine(
              h, std::hash<std::string_view>{}({f.get(), f.size()}));
        }
        return h;
    }
};

} // namespace std
