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

#include "vassert.h"

#include <cstddef>
#include <stdexcept>
#include <type_traits>
#include <vector>

namespace test_details {
struct fragmented_vector_accessor;
}

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
template<typename T, size_t max_fragment_size = 8192>
class fragmented_vector {
    static constexpr size_t calc_elems_per_frag(size_t esize) {
        size_t max = max_fragment_size / esize;
        assert(max > 0);
        // round down to a power of two
        size_t pow2 = 1;
        while (pow2 * 2 <= max) {
            pow2 *= 2;
        }
        return pow2;
    }

    static constexpr size_t elems_per_frag = calc_elems_per_frag(sizeof(T));
    static constexpr size_t frag_bytes = elems_per_frag * sizeof(T);

    static_assert(
      (elems_per_frag & (elems_per_frag - 1)) == 0,
      "element count per fragment must be a power of 2");
    static_assert(elems_per_frag >= 1);

public:
    using value_type = T;

    static constexpr size_t max_frag_bytes = max_fragment_size;

    fragmented_vector() noexcept = default;
    fragmented_vector& operator=(const fragmented_vector&) noexcept = delete;
    fragmented_vector(fragmented_vector&&) noexcept = default;
    fragmented_vector& operator=(fragmented_vector&&) noexcept = default;
    ~fragmented_vector() noexcept = default;

    fragmented_vector copy() const noexcept { return *this; }

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

    T& operator[](size_t index) {
        return const_cast<T&>(std::as_const(*this)[index]);
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

    /**
     * Returns the approximate in-memory size of this vector in bytes.
     */
    size_t memory_size() const {
        return _frags.size() * (sizeof(_frags[0]) + elems_per_frag * sizeof(T));
    }

    /**
     * Returns the (maximum) number of elements in each fragment of this vector.
     */
    static size_t elements_per_fragment() { return elems_per_frag; }

    /**
     * Assign from a std::vector.
     */
    fragmented_vector& operator=(const std::vector<T>& rhs) noexcept {
        clear();

        for (auto& e : rhs) {
            push_back(e);
        }

        return *this;
    }

    void clear() {
        _frags.clear();
        // bug: this method does not update capacity
        _size = 0;
        _capacity = 0;
    }

    template<bool C>
    class iter {
    public:
        using iterator_category = std::random_access_iterator_tag;
        using value_type = typename std::conditional_t<C, const T, T>;
        using difference_type = std::ptrdiff_t;
        using pointer = value_type*;
        using reference = value_type&;

        iter() = default;

        reference operator*() const { return _vec->operator[](_index); }

        iter& operator+=(ssize_t n) {
            _index += n;
            return *this;
        }

        iter& operator-=(ssize_t n) {
            _index -= n;
            return *this;
        }

        iter& operator++() {
            ++_index;
            return *this;
        }

        iter& operator--() {
            --_index;
            return *this;
        }

        iter operator++(int) {
            auto tmp = *this;
            ++*this;
            return tmp;
        }

        iter operator--(int) {
            auto tmp = *this;
            --*this;
            return tmp;
        }

        iter operator+(difference_type offset) { return iter{*this} += offset; }
        iter operator-(difference_type offset) { return iter{*this} -= offset; }

        bool operator==(const iter&) const = default;
        auto operator<=>(const iter&) const = default;

        friend ssize_t operator-(const iter& a, const iter& b) {
            return a._index - b._index;
        }

    private:
        friend class fragmented_vector;
        using unqualified_vec_type = fragmented_vector<T, max_fragment_size>;
        using vec_type = std::
          conditional_t<C, const unqualified_vec_type, unqualified_vec_type>;

        iter(vec_type* vec, size_t index)
          : _index(index)
          , _vec(vec) {}

        size_t _index;
        vec_type* _vec;
    };

    using const_iterator = iter<true>;
    using iterator = iter<false>;

    iterator begin() { return iterator(this, 0); }
    iterator end() { return iterator(this, _size); }

    const_iterator begin() const { return const_iterator(this, 0); }
    const_iterator end() const { return const_iterator(this, _size); }

    const_iterator cbegin() const { return const_iterator(this, 0); }
    const_iterator cend() const { return const_iterator(this, _size); }

    friend test_details::fragmented_vector_accessor;

    friend std::ostream&
    operator<<(std::ostream& os, const fragmented_vector& v) {
        os << "[";
        for (auto& e : v) {
            os << e << ",";
        }
        os << "]";
        return os;
    }

private:
    fragmented_vector(const fragmented_vector&) noexcept = default;

    size_t _size{0};
    size_t _capacity{0};
    std::vector<std::vector<T>> _frags;
};

/**
 * An alias for a fragmented_vector using a "larger" fragment size.
 */
template<typename T>
using large_fragment_vector = fragmented_vector<T, 32 * 1024>;
