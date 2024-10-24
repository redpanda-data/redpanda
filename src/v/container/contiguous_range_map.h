/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "container/fragmented_vector.h"

/**
 * Contiguous range map is an associative sorted container backed by
 * chunked_vector designed to efficiently store objects indexed with contiguous
 * and limited range of integers starting at 0. The container provides a
 * wrapper around basic chunked vector that reassembles the API of a standard
 * map.
 *
 * The wrapper allows random inserts of elements to the map although this is
 * not supported by standard vector.
 *
 * Underlying container is resized every time its size is not sufficient to
 * account for emplaced key.
 *
 * The contiguous_range_map tolerates gap in the range of keys however
 * number and size of gaps is proportional to performance penalty hit when
 * incrementing or decrementing iterators.
 *
 * NOTE:
 * A map can store elements for key's which range starts from value greater than
 * 0 but it will cause significant memory loss. If we will need to store such a
 * ranges we may introduce non type template parameter indicating offset.
 */
template<std::integral KeyT, typename ValueT>
class contiguous_range_map {
public:
    using value_type = std::pair<const KeyT, ValueT>;
    using key_type = KeyT;
    using mapped_type = ValueT;

private:
    using underlying_t = chunked_vector<std::optional<value_type>>;

private:
    template<bool Const>
    class iter {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type =
          typename std::conditional_t<Const, const value_type, value_type>;

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
            ret._container = _container;
            ret._it = _it;
            return ret;
        }

        reference operator*() const { return _it->value(); }

        pointer operator->() const { return &(_it->value()); }

        iter& operator++() {
            ++_it;
            skip_to_next_present();
            return *this;
        }

        iter operator++(int) {
            auto tmp = *this;
            ++*this;
            return tmp;
        }

        bool operator==(const iter& o) const {
            return std::tie(_it, _container) == std::tie(o._it, o._container);
        };

        auto operator<=>(const iter& o) const {
            return std::tie(_it, _container) <=> std::tie(o._it, o._container);
        };

    private:
        void skip_to_next_present() {
            while (_it != _container->_values.end() && !_it->has_value()) {
                ++_it;
            }
        }

        friend class contiguous_range_map<KeyT, ValueT>;
        using parent_t = std::conditional_t<
          Const,
          const contiguous_range_map<KeyT, ValueT>,
          contiguous_range_map<KeyT, ValueT>>;

        iter(
          parent_t* map,
          typename parent_t::underlying_t::template iter<Const> it)
          : _it(it)
          , _container(map) {
            skip_to_next_present();
        }

        typename parent_t::underlying_t::template iter<Const> _it;
        parent_t* _container;
    };

public:
    using iterator = iter<false>;
    using const_iterator = iter<true>;

    contiguous_range_map() noexcept = default;
    contiguous_range_map& operator=(contiguous_range_map&& other) noexcept {
        if (this != &other) {
            this->_size = other._size;
            this->_values = std::move(other._values);
            other._size = 0;
        }
        return *this;
    }

    contiguous_range_map(contiguous_range_map&& other) noexcept {
        *this = std::move(other);
    }
    contiguous_range_map(const contiguous_range_map&) = delete;
    contiguous_range_map& operator=(const contiguous_range_map& other) = delete;
    ~contiguous_range_map() noexcept = default;

    /**
     * Determines if element comparing equal to given key exists in a map.
     */
    bool contains(KeyT key) const {
        return valid_key(key) && _values.size() > static_cast<size_t>(key)
               && _values[key].has_value();
    }

    /**
     * Returns an element comparing equal to given key. If an element for the
     * given key is not present the default element will be constructed.
     */
    mapped_type& operator[](KeyT key) { return emplace(key).first->second; }
    /**
     * Constructs element for given key in place.
     *
     * Returns a pair containing an iterator to the element (either existing or
     * newly created one) and a boolean indicating if element was inserted.
     *
     * Element constructor is not invoked if an element already exists in the
     * map.
     */
    template<typename... Args>
    std::pair<iterator, bool> emplace(KeyT key, Args&&... args) {
        if (!valid_key(key)) {
            throw std::invalid_argument(
              fmt::format("Invalid key {}, must be positive", key));
        }
        /**
         * Inserting last element directly with emplace_back
         */
        if (static_cast<size_t>(key) == _values.size()) {
            _values.emplace_back(
              std::make_pair(key, ValueT(std::forward<Args>(args)...)));
            _size++;
            return {iter<false>(this, _values.begin() + key), true};
        }
        while (static_cast<size_t>(key) >= _values.size()) {
            _values.emplace_back();
        }
        auto it = _values.begin() + key;
        if (it->has_value()) {
            return {iter<false>(this, it), false};
        }

        it->emplace(std::make_pair(key, ValueT(std::forward<Args>(args)...)));
        _size++;
        return std::make_pair(iter<false>(this, it), true);
    }

    /**
     * Resizes the underlying chunked vector to be ready to up to requested_size
     * of keys. Effectively the last key that the map would be able to accept
     * without resizing further is equal to `requested_size - 1`
     */
    void reserve(size_t requested_size) {
        _values.reserve(requested_size);
        while (_values.size() < requested_size) {
            _values.emplace_back();
        }
    }

    /**
     * Shrinks the underlying vector removing all empty slots from the back.
     */
    void shrink_to_fit() {
        while (!_values.back().has_value()) {
            _values.pop_back();
        }
    }
    /**
     * Returns the actual number of elements allocated in the underlying vector.
     */
    size_t capacity() const { return _values.size(); }

    /**
     * Returns true when map has no elements
     */
    bool empty() const { return _size == 0; }

    iterator begin() { return iter<false>(this, _values.begin()); }
    iterator end() { return iter<false>(this, _values.end()); }

    const_iterator begin() const { return iter<true>(this, _values.begin()); }
    const_iterator end() const { return iter<true>(this, _values.end()); }

    /**
     * Return number of valid elements in the map
     */
    size_t size() const { return _size; }

    /**
     * Returns iterator to an element with requested key or `end()` if the
     * element is not present in the map
     */
    iterator find(KeyT key) {
        if (valid_key(key) && static_cast<size_t>(key) < _values.size()) {
            auto it = _values.begin() + static_cast<size_t>(key);
            if (it->has_value()) {
                return iterator(this, it);
            }
        }
        return end();
    }
    /**
     * Returns iterator to an element with requested key or `end()` if the
     * element is not present in the map
     */
    const_iterator find(KeyT key) const {
        if (valid_key(key) && static_cast<size_t>(key) < _values.size()) {
            auto it = _values.begin() + static_cast<size_t>(key);
            if (it->has_value()) {
                return const_iterator(this, it);
            }
        }
        return end();
    }
    /**
     * Removes the element with given key from the map.
     * Invalid keys are ignored.
     *
     * NOTE: it does not shrink the underlying data structure.
     */
    void erase(KeyT key) {
        if (valid_key(key) && static_cast<size_t>(key) < _values.size()) {
            if (_values[static_cast<size_t>(key)].has_value()) {
                _values[static_cast<size_t>(key)].reset();
                --_size;
            }
        }
    }
    /**
     * Removes the element pointed by given iterator from the map.
     *
     * NOTE: it does not shrink the underlying data structure.
     */
    void erase(const_iterator it) {
        if (it._it->has_value()) {
            auto mutable_it = iterator(
              this, _values.begin() + it._it->value().first);
            mutable_it._it->reset();
            --_size;
        }
    }

    contiguous_range_map copy() const {
        contiguous_range_map<KeyT, ValueT> ret;
        ret._values = _values.copy();
        ret._size = _size;
        return ret;
    }

private:
    static bool valid_key(KeyT key) {
        // map keys must be positive.
        return key >= 0;
    }
    underlying_t _values;
    // number of valid entries
    size_t _size{0};
};
