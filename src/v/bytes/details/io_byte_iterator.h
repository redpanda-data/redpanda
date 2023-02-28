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

#include "bytes/details/io_fragment.h"

#include <iterator>

// See io_iterator_consumer for iterator validity notes.
namespace details {
class io_byte_iterator {
public:
    using io_const_iterator
      = uncounted_intrusive_list<io_fragment, &io_fragment::hook>::
        const_iterator;

    // iterator_traits
    using difference_type = void;
    using value_type = char;
    using pointer = const char*;
    using reference = const char&;
    using iterator_category = std::forward_iterator_tag;

    io_byte_iterator(
      const io_const_iterator& begin, const io_const_iterator& end) noexcept
      : _frag(begin)
      , _frag_end(end) {
        if (_frag != _frag_end) {
            _frag_index = _frag->get();
            // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            _frag_index_end = _frag->get() + _frag->size();
            // handle an empty fragment
            if (_frag_index == _frag_index_end) {
                next_fragment();
            }
        } else {
            _frag_index = nullptr;
            _frag_index_end = nullptr;
        }
    }
    io_byte_iterator(
      const io_const_iterator& begin,
      const io_const_iterator& end,
      const char* frag_index,
      const char* frag_index_end) noexcept
      : _frag(begin)
      , _frag_end(end)
      , _frag_index(frag_index)
      , _frag_index_end(frag_index_end) {}

    pointer get() const { return _frag_index; }
    reference operator*() const noexcept { return *_frag_index; }
    pointer operator->() const noexcept { return _frag_index; }
    /// true if pointing to the byte-value (not necessarily the same address)
    bool operator==(const io_byte_iterator& o) const noexcept {
        return _frag_index == o._frag_index;
    }
    bool operator!=(const io_byte_iterator& o) const noexcept {
        return !(*this == o);
    }
    io_byte_iterator& operator++() {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        if (++_frag_index == _frag_index_end) {
            next_fragment();
        }
        return *this;
    }

private:
    void next_fragment() {
        while (true) {
            ++_frag;
            if (_frag != _frag_end) {
                _frag_index = _frag->get();
                // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
                _frag_index_end = _frag->get() + _frag->size();
                // handle an empty fragment
                if (_frag_index == _frag_index_end) {
                    continue;
                }
                return;
            }
            _frag_index = nullptr;
            _frag_index_end = nullptr;
            return;
        }
    }

    io_const_iterator _frag;
    io_const_iterator _frag_end;
    const char* _frag_index = nullptr;
    const char* _frag_index_end = nullptr;
};

} // namespace details
