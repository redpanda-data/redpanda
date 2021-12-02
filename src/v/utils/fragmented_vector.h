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

#include "vassert.h"

#include <cstddef>
#include <vector>

/**
 * A very very simple fragmented vector that provides random access like a
 * vector, but does not store its data in contiguous memory.
 *
 * There is no reserve method because we allocate a full fragment at a time.
 * However, after you populate a vector you might want to call shrink to fit if
 * your fragment is large. A more advanced strategy could allocate capacity up
 * front which just requires a bit more accounting.
 *
 * The iterator implementation works for a few things like std::lower_bound,
 * upper_bound, distance, etc... see fragmented_vector_test.
 *
 * Note that the decision to allocate a full fragment at a time isn't
 * necessarily an optimization, but rather a restriction that simplifies the
 * implementation. If expand fragmented_vector to be more general purpose, we
 * should indeed make it more flexible. If we add a reserve method to allocate
 * all of the space at once we might benefit from the allocator helping with
 * giving us contiguous memory.
 */
template<typename T, size_t fragment_size = 8192>
class fragmented_vector {
    static_assert(
      (fragment_size & fragment_size - 1) == 0,
      "fragment size must be a power of 2");
    static_assert(fragment_size % sizeof(T) == 0);
    static constexpr size_t elems_per_frag = fragment_size / sizeof(T);
    static_assert(elems_per_frag >= 1);

public:
    using value_type = T;

    fragmented_vector() noexcept = default;
    fragmented_vector(const fragmented_vector&) noexcept = delete;
    fragmented_vector& operator=(const fragmented_vector&) noexcept = delete;
    fragmented_vector(fragmented_vector&&) noexcept = default;
    fragmented_vector& operator=(fragmented_vector&&) noexcept = default;
    ~fragmented_vector() noexcept = default;

    void push_back(T elem) {
        if (_size == _capacity) {
            std::vector<T> frag;
            frag.reserve(elems_per_frag);
            _frags.push_back(std::move(frag));
            _capacity += elems_per_frag;
        }
        _frags.back().push_back(elem);
        ++_size;
    }

    void pop_back() {
        vassert(_size > 0, "Cannot pop from empty container");
        _frags.back().pop_back();
        --_size;
        if (_frags.back().empty()) {
            _frags.pop_back();
            _capacity -= elems_per_frag;
        }
    }

    const T& operator[](size_t index) const {
        vassert(index < _size, "Index out of range {}/{}", index, _size);
        auto& frag = _frags.at(index / elems_per_frag);
        return frag.at(index % elems_per_frag);
    }

    const T& back() const { return _frags.back().back(); }
    bool empty() const noexcept { return _size == 0; }
    size_t size() const noexcept { return _size; }

    void shrink_to_fit() {
        if (!_frags.empty()) {
            _frags.back().shrink_to_fit();
        }
    }

    bool operator==(const fragmented_vector& o) const noexcept {
        return o._frags == _frags;
    }

    class const_iterator {
    public:
        using iterator_category = std::random_access_iterator_tag;
        using value_type = const T;
        using difference_type = std::ptrdiff_t;
        using pointer = const T*;
        using reference = const T&;

        reference operator*() const { return _vec->operator[](_index); }

        const_iterator& operator+=(ssize_t n) {
            _index += n;
            return *this;
        }

        const_iterator& operator++() {
            ++_index;
            return *this;
        }

        const_iterator& operator--() {
            --_index;
            return *this;
        }

        bool operator==(const const_iterator&) const = default;

        friend ssize_t
        operator-(const const_iterator& a, const const_iterator& b) {
            return a._index - b._index;
        }

    private:
        friend class fragmented_vector;

        const_iterator(
          const fragmented_vector<T, fragment_size>* vec, size_t index)
          : _index(index)
          , _vec(vec) {}

        size_t _index;
        const fragmented_vector<T, fragment_size>* _vec;
    };

    const_iterator begin() const { return const_iterator(this, 0); }
    const_iterator end() const { return const_iterator(this, _size); }

private:
    size_t _size{0};
    size_t _capacity{0};
    std::vector<std::vector<T>> _frags;
};
