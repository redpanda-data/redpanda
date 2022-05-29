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

#include "bytes/details/io_byte_iterator.h"
#include "bytes/details/io_fragment.h"
#include "bytes/details/io_placeholder.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/future-util.hh>

#include <algorithm>

/*
 * It is common for an io_iterator_consumer to be initialized with the begin and
 * end iterator of an iobuf (e.g. see iobuf_parser). However, care must be taken
 * to maintain iterator validity. If the associated iobuf moves, then iterators
 * that point at container elements are not invalided, but the saved
 * iobuf::end() will be invalidated. The solution used in iobuf_parser is to
 * wrap the iobuf and iterator consumer in a heap allocated state structure
 * which can be moved without altering the saved iobuf::end() iterator.
 *
 * Alteratives:
 *
 * - Store std::distance(begin, end) instead of end. This would eliminate the
 *   issue, but requires o(n) operations in the general case. we could switch to
 *   a constant-size operation intrusive list, and special case of iterating
 *   over the entire iobuf and thus avoid std::distance in most cases.
 *
 * - Another option would be to add an interface for changing the end iterator
 *   that could be used by the moving context.
 *
 * - Both cases would need to be integrated transitively with io_byte_iterator
 *   since instances of this are built by io_iterator_consumer::end() and leak
 *   out the underlying stored iobuf::end() iterator.
 */
namespace details {
class io_iterator_consumer {
public:
    using io_const_iterator
      = uncounted_intrusive_list<io_fragment, &io_fragment::hook>::
        const_iterator;

    io_iterator_consumer(
      io_const_iterator const& begin, io_const_iterator const& end) noexcept
      : _frag(begin)
      , _frag_end(end) {
        if (_frag != _frag_end) {
            _frag_index = _frag->get();
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            _frag_index_end = _frag->get() + _frag->size();
        }
    }
    void skip(size_t n) {
        size_t c = consume(n, [](const char*, size_t /*max*/) {
            return ss::stop_iteration::no;
        });
        if (unlikely(c != n)) {
            details::throw_out_of_range(
              "Invalid skip(n). Expected:{}, but skipped:{}", n, c);
        }
    }
    template<typename Output>
    [[gnu::always_inline]] void consume_to(size_t n, Output out) {
        size_t c = consume(n, [&out](const char* src, size_t max) {
            std::copy_n(src, max, out);
            out += max;
            return ss::stop_iteration::no;
        });
        if (unlikely(c != n)) {
            details::throw_out_of_range(
              "Invalid consume_to(n, out), expected:{}, but consumed:{}", n, c);
        }
    }

    template<
      typename T,
      typename = std::enable_if_t<std::is_trivially_copyable_v<T>, T>>
    T consume_type() {
        constexpr size_t sz = sizeof(T);
        T obj;
        char* dst = reinterpret_cast<char*>(&obj); // NOLINT
        consume_to(sz, dst);
        return obj;
    }
    template<typename T, typename = std::enable_if_t<std::is_integral_v<T>, T>>
    T consume_be_type() {
        return ss::be_to_cpu(consume_type<T>());
    }
    [[gnu::always_inline]] void consume_to(size_t n, io_placeholder& ph) {
        size_t c = consume(n, [&ph](const char* src, size_t max) {
            ph.write(src, max);
            return ss::stop_iteration::no;
        });
        if (unlikely(c != n)) {
            details::throw_out_of_range(
              "Invalid consume_to(n, placeholder), expected:{}, but "
              "consumed:{}",
              n,
              c);
        }
    }

    template<typename Consumer>
    requires requires(Consumer c, const char* src, size_t max) {
        { c(src, max) } -> std::same_as<ss::stop_iteration>;
    }
    /// takes a Consumer object and iteraters over the chunks in oder, from
    /// the given buffer index position. Use a stop_iteration::yes for early
    /// exit;
    size_t consume(const size_t n, Consumer&& f) {
        size_t i = 0;
        while (i < n) {
            if (_frag == _frag_end) {
                return i;
            }
            const size_t bytes_left = segment_bytes_left();
            if (bytes_left == 0) {
                // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
                if (++_frag != _frag_end) {
                    _frag_index = _frag->get();
                    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
                    _frag_index_end = _frag->get() + _frag->size();
                }
                continue;
            }
            const size_t step = std::min(n - i, bytes_left);
            const ss::stop_iteration stop = f(_frag_index, step);
            i += step;
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            _frag_index += step;
            _bytes_consumed += step;
            if (stop == ss::stop_iteration::yes) {
                break;
            }
        }

        if (_frag_index == _frag_index_end) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            if (_frag != _frag_end && ++_frag != _frag_end) {
                _frag_index = _frag->get();
                // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
                _frag_index_end = _frag->get() + _frag->size();
            } else {
                _frag_index = nullptr;
                _frag_index_end = nullptr;
            }
        }

        return i;
    }
    size_t bytes_consumed() const { return _bytes_consumed; }
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    size_t segment_bytes_left() const { return _frag_index_end - _frag_index; }
    bool is_finished() const { return _frag == _frag_end; }

    /// starts a new iterator byte-for-byte starting at *this* index
    /// useful for varint decoding that need to peek ahead
    io_byte_iterator begin() const {
        return io_byte_iterator(_frag, _frag_end, _frag_index, _frag_index_end);
    }
    io_byte_iterator end() const {
        return io_byte_iterator(_frag_end, _frag_end, nullptr, nullptr);
    }

private:
    io_const_iterator _frag;
    io_const_iterator _frag_end;
    const char* _frag_index = nullptr;
    const char* _frag_index_end = nullptr;
    size_t _bytes_consumed{0};
};

} // namespace details
