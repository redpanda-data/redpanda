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

#include "base/seastarx.h"
#include "base/vassert.h"
#include "bytes/details/out_of_range.h"

#include <seastar/core/temporary_buffer.hh>

#include <iterator>
#include <utility>

namespace details {

class io_fragment {
public:
    /**
     * Initialize fragment from the provided temporary buffer.
     */
    explicit io_fragment(ss::temporary_buffer<char> buf)
      : _buf(std::move(buf))
      , _used_bytes(_buf.size()) {}

    /**
     * Initialize an empty fragment of a given size.
     */
    explicit io_fragment(size_t size)
      : _buf(size)
      , _used_bytes(0) {}

    io_fragment(io_fragment&& o) noexcept = delete;
    io_fragment& operator=(io_fragment&& o) noexcept = delete;
    io_fragment(const io_fragment& o) = delete;
    io_fragment& operator=(const io_fragment& o) = delete;
    ~io_fragment() noexcept {
        dassert(
          !_next && !_prev,
          "io_fragment cleanup failure and possible memory leak");
    }

    bool is_empty() const { return _used_bytes == 0; }
    size_t available_bytes() const { return _buf.size() - _used_bytes; }
    void reserve(size_t reservation) {
        check_out_of_range(reservation, available_bytes());
        _used_bytes += reservation;
    }
    size_t size() const { return _used_bytes; }
    size_t capacity() const { return _buf.size(); }

    const char* get() const {
        // required for the networking layer to conver to
        // scattered message without copying data
        return _buf.get();
    }
    size_t append(const char* src, size_t len) {
        const size_t sz = std::min(len, available_bytes());
        std::copy_n(src, sz, get_current());
        _used_bytes += sz;
        return sz;
    }
    ss::temporary_buffer<char> share() {
        // needed for output_stream<char> wrapper
        return _buf.share(0, _used_bytes);
    }
    ss::temporary_buffer<char> share(size_t pos, size_t len) {
        return _buf.share(pos, len);
    }

    // destructive move. most of the time share() will suffice.
    ss::temporary_buffer<char> unoptimized_release() && {
        _buf.trim(_used_bytes);
        return std::move(_buf);
    }
    /// destructive move. place special care when calling this method
    /// on a shared iobuf. most of the time you want share() instead of release
    ss::temporary_buffer<char> release() && {
        trim();
        return std::move(_buf);
    }
    void trim() {
        if (_used_bytes == _buf.size()) {
            return;
        }
        size_t half = _buf.size() / 2;
        if (_used_bytes <= half) {
            // this is an important optimization. often times during RPC
            // serialization we append some small controll bytes, _right_
            // before we append a full new chain of iobufs
            _buf = ss::temporary_buffer<char>(_buf.get(), _used_bytes);
        } else {
            _buf.trim(_used_bytes);
        }
    }
    void trim(size_t len) { _used_bytes = std::min(len, _used_bytes); }
    void trim_front(size_t pos);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    char* get_current() { return _buf.get_write() + _used_bytes; }
    char* get_write() { return _buf.get_write(); }

    explicit operator std::string_view() const { return {get(), size()}; }

private:
    friend class io_fragment_list;

    io_fragment* _next = nullptr;
    io_fragment* _prev = nullptr;

    ss::temporary_buffer<char> _buf;
    size_t _used_bytes;
};

/**
 * A double-ly linked (intrusive) list of `io_fragment`.
 *
 * We use a hand rolled list instead of boost::intrusive_list to optimize
 * certain codepaths, namely the move constructor was about 4x faster in the
 * hand rolled implementation over boost.
 *
 * We're also able to enhance the list with features like iterator invalidation
 * checking and the like.
 *
 * Note that this container does not own the memory for the io_fragments it
 * contains, iobuf is responsible for that.
 */
class io_fragment_list {
    template<bool Const, bool Forward>
    class iter {
    public:
        using iterator_category = std::forward_iterator_tag;
        using container_type = typename std::
          conditional_t<Const, const io_fragment_list, io_fragment_list>;
        using value_type =
          typename std::conditional_t<Const, const io_fragment, io_fragment>;
        using difference_type = std::ptrdiff_t;
        using pointer = value_type*;
        using reference = value_type&;

    private:
        iter([[maybe_unused]] container_type* container, pointer curr)
          : _current(curr)
#ifndef NDEBUG
          , _my_generation(container->_generation)
          , _container(container)
#endif
        {
        }

    public:
        iter() noexcept = default;
        // Support conversion to const_iterator
        // NOLINTNEXTLINE(hicpp-explicit-conversions)
        operator iter<true, Forward>() const {
            check_generation();
#ifndef NDEBUG
            return {_container, _current};
#else
            return {nullptr, _current};
#endif
        }

        reference operator*() const {
            check_generation();
            return *_current;
        }
        pointer operator->() const {
            check_generation();
            return _current;
        }
        iter& operator++() {
            check_generation();
            if constexpr (Forward) {
                _current = _current->_next;
            } else {
                _current = _current->_prev;
            }
            return *this;
        }
        iter operator++(int) {
            auto tmp = *this;
            ++*this;
            return tmp;
        }
        bool operator==(const iter& o) const {
            check_generation();
            return _current == o._current;
        }

    private:
        friend class io_fragment_list;

        inline void check_generation() const {
            dassert(
              _container->_generation == _my_generation,
              "Attempting to use an invalidated iterator. The corresponding "
              "iobuf has been mutated since this iterator was constructed.");
        }

        pointer _current = nullptr;
#ifndef NDEBUG
        size_t _my_generation = 0;
        container_type* _container = nullptr;
#endif
    };

public:
    io_fragment_list() noexcept = default;
    io_fragment_list(io_fragment_list&& o) noexcept
      : _head(std::exchange(o._head, nullptr))
      , _tail(std::exchange(o._tail, nullptr)) {
        o.update_generation();
    }
    io_fragment_list(const io_fragment_list&) = delete;
    io_fragment_list& operator=(io_fragment_list&&) = delete;
    io_fragment_list& operator=(const io_fragment_list&) = delete;
    ~io_fragment_list() noexcept {
        dassert(
          _head == nullptr && _tail == nullptr,
          "io_fragment_list cleanup failure and possible memory leak");
    };

    using iterator = iter<false, true>;
    using reverse_iterator = iter<false, false>;
    using const_iterator = iter<true, true>;
    using const_reverse_iterator = iter<true, false>;

    bool empty() const {
        check_consistency();
        return _head == nullptr;
    }

    bool is_single_fragment() const { return !empty() && _head == _tail; }

    io_fragment& front() {
        check_consistency();
        return *_head;
    }

    const io_fragment& front() const {
        check_consistency();
        return *_head;
    }

    void pop_front() {
        check_consistency();
        io_fragment* h = _head;
        _head = h->_next;
        h->_next = nullptr;
        if (_head != nullptr) {
            _head->_prev = nullptr;
        } else {
            _tail = nullptr;
        }
        update_generation();
    }

    void push_front(io_fragment& elem) {
        check_consistency();
        if (_head == nullptr) {
            _tail = &elem;
        } else {
            _head->_prev = &elem;
        }
        elem._next = _head;
        _head = &elem;
        update_generation();
    }

    io_fragment& back() {
        check_consistency();
        return *_tail;
    }
    const io_fragment& back() const {
        check_consistency();
        return *_tail;
    }

    void pop_back() {
        check_consistency();
        io_fragment* t = _tail;
        _tail = _tail->_prev;
        t->_prev = nullptr;
        if (_tail != nullptr) {
            _tail->_next = nullptr;
        } else {
            _head = nullptr;
        }
        update_generation();
    }

    void push_back(io_fragment& elem) {
        dassert(
          elem._prev == nullptr, "pushing back io_fragment in another iobuf");
        dassert(
          elem._next == nullptr, "pushing back io_fragment in another iobuf");
        check_consistency();
        if (_tail == nullptr) {
            _head = &elem;
        } else {
            _tail->_next = &elem;
        }
        elem._prev = _tail;
        _tail = &elem;
        update_generation();
    }

    template<typename Func>
    void clear_and_dispose(Func func) {
        check_consistency();
        io_fragment* current = _head;
        while (current != nullptr) {
            io_fragment* next = current->_next;
            if (next != nullptr) {
                dassert(
                  next->_prev == current, "io_fragment_list inconsistency");
                next->_prev = nullptr;
            }
            current->_next = nullptr;
            func(current);
            current = next;
        }
        _head = nullptr;
        _tail = nullptr;
        update_generation();
    }

    iterator begin() { return {this, _head}; }
    iterator end() { return {this, nullptr}; }
    reverse_iterator rbegin() { return {this, _tail}; }
    reverse_iterator rend() { return {this, nullptr}; }
    const_iterator cbegin() const { return {this, _head}; }
    const_iterator cend() const { return {this, nullptr}; }
    const_reverse_iterator crbegin() const { return {this, _tail}; }
    const_reverse_iterator crend() const { return {this, nullptr}; }

private:
    friend class iter<true, true>;
    friend class iter<false, true>;
    friend class iter<false, false>;
    friend class iter<true, false>;
    inline void update_generation() {
#ifndef NDEBUG
        ++_generation;
#endif
    }

    inline void check_consistency() const {
        // We don't check full consistency, just the head and tail elements, so
        // we don't make debug mode *that* much slower.
#ifndef NDEBUG
        dassert(
          (_head == nullptr) == (_tail == nullptr),
          "head and tail consistency");
        if (_head == nullptr) {
            return;
        }
        dassert(!_head->_prev, "head must not have prev");
        dassert(!_tail->_next, "tail must not have next");
        dassert(
          _head == _tail || _head->_next, "head must have next if size > 1");
        dassert(
          _head == _tail || _tail->_prev, "tail must have prev if size > 1");
        dassert(
          (_head->_next == _tail) == (_tail->_prev == _head),
          "If head points to tail, then tail should point to head (size == 2 "
          "consistency).");
#endif
    }

    io_fragment* _head = nullptr;
    io_fragment* _tail = nullptr;
#ifndef NDEBUG
    size_t _generation = 0;
#endif
};

inline void dispose_io_fragment(io_fragment* f) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wfree-nonheap-object"
    delete f; // NOLINT
#pragma GCC diagnostic pop
}

} // namespace details
