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

#include "base/seastar_fwd.h"
#include "base/vassert.h"

#include <fmt/format.h>
#include <fmt/ostream.h>

#include <bit>
#include <cstddef>
#include <initializer_list>
#include <iterator>
#include <ostream>
#include <ranges>
#include <type_traits>
#include <utility>
#include <vector>

/**
 * A chunked vector is a container that provides random access like a
 * vector, but does not store its data in contiguous memory.
 *
 * Instead the allocations are broken up across many different individual
 * vectors, but the exposed view is of a single container.
 *
 * Additionally the allocation strategy is like a "normal" vector until the
 * first chunk is full, after which we will then only allocate full chunks.
 *
 * The iterator implementation works for a few things like std::lower_bound,
 * upper_bound, distance, etc... see chunked_vector_test.
 */
template<typename T>
class chunked_vector {
    static constexpr size_t max_allocation_size = 128UL * 1024;

    // calculate the maximum number of elements per fragment while
    // keeping the element count a power of two
    static consteval size_t calc_elems_per_frag() {
        size_t max = max_allocation_size / sizeof(T);
        return std::bit_floor(max);
    }

    /**
     * The maximum number of bytes per fragment as specified in
     * as part of the type. Note that for most types, the true
     * number of bytes in a full fragment may as low as half
     * of this amount (+1) since the number of elements is restricted
     * to a power of two.
     */
    static consteval size_t calc_max_frag_bytes() {
        return calc_elems_per_frag() * sizeof(T);
    }

public:
    using this_type = chunked_vector<T>;
    using backing_type = std::vector<std::vector<T>>;
    using value_type = T;
    using reference = std::conditional_t<std::is_same_v<T, bool>, bool, T&>;
    using const_reference
      = std::conditional_t<std::is_same_v<T, bool>, bool, const T&>;
    using size_type = size_t;
    using allocator_type = backing_type::allocator_type;
    using difference_type = backing_type::difference_type;
    using pointer = T*;
    using const_pointer = const T*;

    chunked_vector() noexcept = default;
    explicit chunked_vector(allocator_type alloc)
      : _frags(alloc) {}
    chunked_vector& operator=(const chunked_vector&) noexcept = delete;
    chunked_vector(chunked_vector&& other) noexcept {
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
    chunked_vector(Iter begin, Iter end)
      : chunked_vector() {
        if constexpr (std::random_access_iterator<Iter>) {
            reserve(std::distance(begin, end));
        }
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
    chunked_vector(std::initializer_list<value_type> elems)
      : chunked_vector(elems.begin(), elems.end()) {}

    /**
     * @brief Construct a new vector from a range
     *
     * This constructor will copy or move from the range depending on the value
     * category of the elements NOT the one of the range. I.e.
     * `chunked_vector(std::move(src))` will not necessarily invoke move
     * constructor on the elements of `src`. For example, an rvalue `std::span`
     * is a non-owning view, and moving from its elements would be a bug.
     * Similar for most of the standard library views.
     *
     * To ensure move semantics from the range elements, use
     * `std::views::as_rvalue`.
     *
     * https://en.cppreference.com/w/cpp/ranges/as_rvalue_view.html
     */
    template<typename Range>
    requires(std::ranges::range<Range>)
    // NOLINTNEXTLINE(cppcoreguidelines-missing-std-forward)
    chunked_vector(std::from_range_t, Range&& range)
      : chunked_vector() {
        if constexpr (std::ranges::sized_range<Range>) {
            reserve(std::ranges::size(range));
        }
        std::copy(
          std::ranges::begin(range),
          std::ranges::end(range),
          std::back_inserter(*this));
    }

    chunked_vector& operator=(chunked_vector&& other) noexcept {
        if (this != &other) {
            this->_size = other._size;
            this->_capacity = other._capacity;
            this->_frags = std::move(other._frags);
            // Move compatibility with std::vector that post move
            // the vector is empty().
            other._size = other._capacity = 0;
            other.update_generation();
            update_generation();
        }
        return *this;
    }
    ~chunked_vector() noexcept = default;

    chunked_vector copy() const noexcept { return *this; }

    auto get_allocator() const { return _frags.get_allocator(); }

    void swap(chunked_vector& other) noexcept {
        std::swap(_size, other._size);
        std::swap(_capacity, other._capacity);
        std::swap(_frags, other._frags);
        other.update_generation();
        update_generation();
    }

    template<class E = T>
    void push_back(E&& elem) {
        maybe_add_capacity();
        _frags.back().push_back(std::forward<E>(elem));
        ++_size;
        update_generation();
    }

    template<class... Args>
    T& emplace_back(Args&&... args) {
        maybe_add_capacity();
        T& emplaced = _frags.back().emplace_back(std::forward<Args>(args)...);
        ++_size;
        update_generation();
        return emplaced;
    }

    void pop_back() {
        vassert(_size > 0, "Cannot pop from empty container");
        _frags.back().pop_back();
        --_size;
        if (_frags.back().empty()) {
            _frags.pop_back();
            _capacity -= std::min(calc_elems_per_frag(), _capacity);
        }
        update_generation();
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
            _capacity -= calc_elems_per_frag();
        }

        for (size_t i = 0; i < n; ++i) {
            _frags.back().pop_back();
        }
        update_generation();
    }

    const_reference at(size_t index) const {
        static constexpr size_t elems_per_frag = calc_elems_per_frag();
        return _frags.at(index / elems_per_frag).at(index % elems_per_frag);
    }

    reference at(size_t index) {
        static constexpr size_t elems_per_frag = calc_elems_per_frag();
        return _frags.at(index / elems_per_frag).at(index % elems_per_frag);
    }

    const_reference operator[](size_t index) const {
        static constexpr size_t elems_per_frag = calc_elems_per_frag();
        return _frags[index / elems_per_frag][index % elems_per_frag];
    }

    reference operator[](size_t index) {
        static constexpr size_t elems_per_frag = calc_elems_per_frag();
        return _frags[index / elems_per_frag][index % elems_per_frag];
    }

    const_reference front() const { return _frags.front().front(); }
    const_reference back() const { return _frags.back().back(); }
    reference front() { return _frags.front().front(); }
    reference back() { return _frags.back().back(); }
    bool empty() const noexcept { return _size == 0; }
    size_t size() const noexcept { return _size; }
    size_t capacity() const noexcept { return _capacity; }

    /**
     * Requests the removal of unused capacity.
     *
     * If reallocation occurs, all iterators (including the end() iterator) and
     * all references to the elements are invalidated.
     */
    void shrink_to_fit() {
        // Calling shrink to fix then modifying the container could result
        // in allocations that overshoot our max_frag_bytes, except when
        // we're managing the dynamic size of the first fragment.
        if (_frags.size() == 1) {
            auto& front = _frags.front();
            front.shrink_to_fit();
            _capacity = front.capacity();
        }
        update_generation();
    }

    /**
     * Increase the capacity of the vector (the total number of elements that
     * the vector can hold without requiring reallocation) to a value that's
     * greater or equal to new_cap.
     *
     * This method has the same guarantees as `std::vector::reserve`, that being
     * calling reserve doesn't preserve pointer or iterator stability when
     * new_cap is larger than the current capacity. There maybe cases where this
     * class invalidates less than `std::vector`, but that should not be relied
     * upon.
     *
     * Performance wise if you know the intended size of this structure is
     * desired to call this to prevent costly reallocations when inserting
     * elements.
     */
    void reserve(size_t new_cap) {
        static constexpr size_t elems_per_frag = calc_elems_per_frag();
        if (new_cap > _capacity) {
            if (_frags.empty()) {
                auto& frag = _frags.emplace_back();
                frag.reserve(std::min(elems_per_frag, new_cap));
                _capacity = frag.capacity();
            } else if (_frags.size() == 1) {
                auto& frag = _frags.front();
                frag.reserve(std::min(elems_per_frag, new_cap));
                _capacity = frag.capacity();
            }
            // We only reserve the first fragment as all fragments after the
            // first are allocated at the maximum size, so we don't save
            // anything in terms of reallocs after fully allocating the
            // first fragment. In addition, due to cache locality, it's
            // better to delay the allocations of those other fragments
            // until they're going to be used.
        }
        update_generation();
    }

    bool operator==(const chunked_vector& o) const noexcept {
        return o._frags == _frags;
    }

    /**
     * Returns the approximate in-memory size of this vector in bytes.
     */
    size_t memory_size() const {
        return (_frags.size() * sizeof(_frags[0])) + (_capacity * sizeof(T));
    }

    /**
     * Returns the (maximum) number of elements in each fragment of this vector.
     */
    static constexpr size_t elements_per_fragment() {
        return calc_elems_per_frag();
    }

    static constexpr size_t max_frag_bytes() { return calc_max_frag_bytes(); }

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
        update_generation();
    }

    template<bool C>
    class iter {
    public:
        using iterator_category = std::random_access_iterator_tag;
        using iterator_concept = std::random_access_iterator_tag;
        using value_type = T;
        using difference_type = std::ptrdiff_t;
        using pointer = std::conditional_t<
          std::is_same_v<T, bool>,
          bool,
          std::conditional_t<C, const T*, T*>>;
        using reference = std::conditional_t<
          std::is_same_v<T, bool>,
          bool,
          std::conditional_t<C, const T&, T&>>;

        iter() = default;

        /**
         * Conversion operator allowing iterator to be converted to
         * const_iterator, as required by the general iterator contract.
         */
        operator iter<true>() const { // NOLINT(hicpp-explicit-conversions)
            check_generation();
            iter<true> ret;
            ret._vec = _vec;
            ret._index = _index;
#ifndef NDEBUG
            ret._my_generation = _my_generation;
#endif
            return ret;
        }

        reference operator*() const {
            check_generation();
            return _vec->operator[](_index);
        }
        pointer operator->() const {
            check_generation();
            return &_vec->operator[](_index);
        }

        iter& operator+=(ssize_t n) {
            check_generation();
            _index += n;
            return *this;
        }

        iter& operator-=(ssize_t n) {
            check_generation();
            _index -= n;
            return *this;
        }

        iter& operator++() {
            check_generation();
            ++_index;
            return *this;
        }

        iter& operator--() {
            check_generation();
            --_index;
            return *this;
        }

        iter operator++(int) {
            check_generation();
            auto tmp = *this;
            ++*this;
            return tmp;
        }

        iter operator--(int) {
            check_generation();
            auto tmp = *this;
            --*this;
            return tmp;
        }

        iter operator+(difference_type offset) const {
            check_generation();
            return iter{*this} += offset;
        }

        iter operator-(difference_type offset) const {
            check_generation();
            return iter{*this} -= offset;
        }

        reference operator[](difference_type offset) const {
            check_generation();
            return *(*this + offset);
        }

        bool operator==(const iter& o) const {
            check_generation();
            dassert(
              _vec == o._vec,
              "iterator compared with different chunked_vector");
            return std::tie(_index, _vec) == std::tie(o._index, o._vec);
        };
        auto operator<=>(const iter& o) const {
            check_generation();
            dassert(
              _vec == o._vec,
              "iterator compared with different chunked_vector");
            return std::tie(_index, _vec) <=> std::tie(o._index, o._vec);
        };

        friend ssize_t operator-(const iter& a, const iter& b) {
            return a._index - b._index;
        }

        friend iter operator+(const difference_type& offset, const iter& i) {
            return i + offset;
        }

    private:
        friend class chunked_vector;
        using vec_type = std::conditional_t<C, const this_type, this_type>;

        iter(vec_type* vec, size_t index)
          : _index(index)
#ifndef NDEBUG
          , _my_generation(vec->_generation)
#endif
          , _vec(vec) {
        }

        inline void check_generation() const {
            dassert(
              _vec->_generation == _my_generation,
              "Attempting to use an invalidated iterator. The corresponding "
              "chunked_vector container has been mutated since this "
              "iterator was constructed.");
        }

        size_t _index{};
#ifndef NDEBUG
        size_t _my_generation{};
#endif
        vec_type* _vec{};
    };

    using const_iterator = iter<true>;
    using iterator = iter<false>;

    using const_reverse_iterator = std::reverse_iterator<const_iterator>;
    using reverse_iterator = std::reverse_iterator<iterator>;

    iterator begin() { return iterator(this, 0); }
    iterator end() { return iterator(this, _size); }

    const_iterator begin() const { return const_iterator(this, 0); }
    const_iterator end() const { return const_iterator(this, _size); }

    const_iterator cbegin() const { return const_iterator(this, 0); }
    const_iterator cend() const { return const_iterator(this, _size); }

    reverse_iterator rbegin() { return reverse_iterator(end()); }
    reverse_iterator rend() { return reverse_iterator(begin()); }

    const_reverse_iterator rbegin() const {
        return const_reverse_iterator(end());
    }
    const_reverse_iterator rend() const {
        return const_reverse_iterator(begin());
    }

    const_reverse_iterator crbegin() const {
        return const_reverse_iterator(end());
    }
    const_reverse_iterator crend() const {
        return const_reverse_iterator(begin());
    }

    /**
     * @brief Erases all elements from begin to the end of the vector.
     */
    void erase_to_end(const_iterator begin) { pop_back_n(cend() - begin); }

    template<typename... Args>
    static chunked_vector single(Args&&... args) {
        chunked_vector v;
        v.emplace_back(std::forward<Args>(args)...);
        return v;
    }

    friend std::ostream&
    operator<<(std::ostream& os, const chunked_vector<T>& v) {
        fmt::print(os, "[{}]", fmt::join(v, ","));
        return os;
    }

private:
    [[gnu::always_inline]] void maybe_add_capacity() {
        if (_size == _capacity) [[unlikely]] {
            add_capacity();
        }
    }

    void add_capacity() {
        static constexpr size_t elems_per_frag = calc_elems_per_frag();
        static_assert(
          calc_max_frag_bytes() <= max_allocation_size,
          "max size of a fragment must be <= 128KiB");
        if (_frags.size() == 1 && _frags.back().capacity() < elems_per_frag) {
            auto& frag = _frags.back();
            auto new_cap = std::min(elems_per_frag, frag.capacity() * 2);
            frag.reserve(new_cap);
            _capacity = new_cap;
            return;
        } else if (_frags.empty()) {
            // At least one element or 32 bytes worth of elements for small
            // items.
            static constexpr size_t initial_cap = std::max(
              1UL, 32UL / sizeof(T));
            _capacity = initial_cap;
            _frags.emplace_back(_frags.get_allocator()).reserve(_capacity);
            return;
        }
        _frags.emplace_back(_frags.get_allocator()).reserve(elems_per_frag);
        _capacity += elems_per_frag;
    }

    inline void update_generation() {
#ifndef NDEBUG
        ++_generation;
#endif
    }

private:
    friend class chunked_vector_validator;
    chunked_vector(const chunked_vector&) noexcept = default;

    template<typename TT>
    friend seastar::future<void>
    chunked_vector_fill_async(chunked_vector<TT>&, const TT&);

    template<typename TT>
    friend seastar::future<void>
    chunked_vector_clear_async(chunked_vector<TT>&);

    size_t _size{0};
    size_t _capacity{0};
    backing_type _frags;
#ifndef NDEBUG
    // Add a generation number that is incremented on every mutation to catch
    // invalidated iterator accesses.
    size_t _generation{0};
#endif
};
