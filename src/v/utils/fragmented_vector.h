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

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/util/later.hh>

#include <cstddef>
#include <initializer_list>
#include <iterator>
#include <stdexcept>
#include <type_traits>
#include <utility>
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
    // calculate the maximum number of elements per fragment while
    // keeping the element count a power of two
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

    static_assert(
      (elems_per_frag & (elems_per_frag - 1)) == 0,
      "element count per fragment must be a power of 2");
    static_assert(elems_per_frag >= 1);

public:
    using this_type = fragmented_vector<T, max_fragment_size>;
    using value_type = T;
    using reference = T&;
    using const_reference = const T&;
    using size_type = size_t;

    /**
     * The maximum number of bytes per fragment as specified in
     * as part of the type. Note that for most types, the true
     * number of bytes in a full fragment may as low as half
     * of this amount (+1) since the number of elements is restricted
     * to a power of two.
     */
    static constexpr size_t max_frag_bytes = max_fragment_size;

    fragmented_vector() noexcept = default;
    fragmented_vector& operator=(const fragmented_vector&) noexcept = delete;
    fragmented_vector(fragmented_vector&& other) noexcept {
        *this = std::move(other);
    }

    /**
     * @brief Create a vector from a begin, end iterator pair.
     *
     * This has the same semantics as the corresponding std::vector
     * constructor.
     */
    template<typename Iter>
    requires std::input_iterator<Iter>
    fragmented_vector(Iter begin, Iter end)
      : fragmented_vector() {
        // Improvement: Write a more efficient implementation for
        // std::contiguous_iterator<Iter>
        for (auto it = begin; it != end; ++it) {
            push_back(*it);
        }
    }

    /**
     * @brief Construct a new vector using an initializer list
     *
     * In the same manner as the corresponding std::vector method.
     */
    fragmented_vector(std::initializer_list<value_type> elems)
      : fragmented_vector(elems.begin(), elems.end()) {}

    fragmented_vector& operator=(fragmented_vector&& other) noexcept {
        if (this != &other) {
            this->_size = other._size;
            this->_capacity = other._capacity;
            this->_frags = std::move(other._frags);
            // Move compatibility with std::vector that post move
            // the vector is empty().
            other._size = other._capacity = 0;
        }
        return *this;
    }
    ~fragmented_vector() noexcept = default;

    fragmented_vector copy() const noexcept { return *this; }

    void swap(fragmented_vector& other) noexcept {
        std::swap(_size, other._size);
        std::swap(_capacity, other._capacity);
        std::swap(_frags, other._frags);
    }

    template<class E = T>
    void push_back(E&& elem) {
        maybe_add_capacity();
        _frags.back().push_back(std::forward<E>(elem));
        ++_size;
    }

    template<class... Args>
    T& emplace_back(Args&&... args) {
        maybe_add_capacity();
        _frags.back().emplace_back(std::forward<Args>(args)...);
        ++_size;
        return _frags.back().back();
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

    /*
     * Replacement for `erase(some_it, end())` but more efficient than n
     * `pop_back()s`
     */
    void pop_back_n(size_t n) {
        vassert(
          _size >= n, "Cannot pop more than size() elements in container");

        if (_size == n) {
            clear();
            return;
        }

        _size -= n;

        while (n >= _frags.back().size()) {
            n -= _frags.back().size();
            _frags.pop_back();
            _capacity -= elems_per_frag;
        }

        for (size_t i = 0; i < n; ++i) {
            _frags.back().pop_back();
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

    const T& front() const { return _frags.front().front(); }
    const T& back() const { return _frags.back().back(); }
    T& front() { return _frags.front().front(); }
    T& back() { return _frags.back().back(); }
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
     * Remove all elements from the vector.
     *
     * Unlike std::vector, this also releases all the memory from
     * the vector (since this vector already the same pointer
     * and iterator stability guarantees that std::vector provides
     * based on non-reallocation and capacity()).
     */
    void clear() {
        // do the swap dance to actually clear the memory held by the vector
        std::vector<std::vector<T>>{}.swap(_frags);
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

        /**
         * Conversion operator allowing iterator to be converted to
         * const_iterator, as required by the general iterator contract.
         */
        operator iter<true>() const { // NOLINT(hicpp-explicit-conversions)
            iter<true> ret;
            ret._vec = _vec;
            ret._index = _index;
            return ret;
        }

        reference operator*() const { return _vec->operator[](_index); }
        pointer operator->() const { return &_vec->operator[](_index); }

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
        using vec_type = std::conditional_t<C, const this_type, this_type>;

        iter(vec_type* vec, size_t index)
          : _index(index)
          , _vec(vec) {}

        size_t _index{};
        vec_type* _vec{};
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

    /**
     * @brief Erases all elements from begin to the end of the vector.
     */
    void erase_to_end(const_iterator begin) { pop_back_n(cend() - begin); }

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
    void maybe_add_capacity() {
        if (_size == _capacity) {
            std::vector<T> frag;
            frag.reserve(elems_per_frag);
            _frags.push_back(std::move(frag));
            _capacity += elems_per_frag;
        }
    }

private:
    fragmented_vector(const fragmented_vector&) noexcept = default;

    template<typename TT, size_t SS>
    friend seastar::future<>
    fragmented_vector_fill_async(fragmented_vector<TT, SS>&, const TT&);

    template<typename TT, size_t SS>
    friend seastar::future<>
    fragmented_vector_clear_async(fragmented_vector<TT, SS>&);

    size_t _size{0};
    size_t _capacity{0};
    std::vector<std::vector<T>> _frags;
};

/**
 * An alias for a fragmented_vector using a larger fragment size, close
 * to the limit of the maximum contiguous allocation size.
 */
template<typename T>
using large_fragment_vector = fragmented_vector<T, 32 * 1024>;

/**
 * An alias for a fragmented_vector using a smaller fragment size.
 */
template<typename T>
using small_fragment_vector = fragmented_vector<T, 1024>;

/**
 * A futurized version of std::fill optimized for fragmented vector. It is
 * futurized to allow for large vectors to be filled without incurring reactor
 * stalls. It is optimized by circumventing the indexing indirection incurred by
 * using the fragmented vector interface directly.
 */
template<typename T, size_t S>
inline seastar::future<>
fragmented_vector_fill_async(fragmented_vector<T, S>& vec, const T& value) {
    auto remaining = vec._size;
    for (auto& frag : vec._frags) {
        const auto n = std::min(frag.size(), remaining);
        if (n == 0) {
            break;
        }
        std::fill_n(frag.begin(), n, value);
        remaining -= n;
        if (seastar::need_preempt()) {
            co_await seastar::yield();
        }
    }
    vassert(
      remaining == 0,
      "fragmented vector inconsistency filling remaining {} size {} cap {} "
      "nfrags {}",
      remaining,
      vec._size,
      vec._capacity,
      vec._frags.size());
}

/**
 * A futurized version of fragmented_vector::clear that allows clearing a large
 * vector without incurring a reactor stall.
 */
template<typename T, size_t S>
inline seastar::future<>
fragmented_vector_clear_async(fragmented_vector<T, S>& vec) {
    while (!vec._frags.empty()) {
        vec._frags.pop_back();
        if (seastar::need_preempt()) {
            co_await seastar::yield();
        }
    }
    vec.clear();
}
