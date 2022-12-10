/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/types.h"
#include "utils/delta_for.h"

#include <boost/iterator/iterator_categories.hpp>
#include <boost/iterator/iterator_facade.hpp>

#include <functional>
#include <iterator>

namespace cloud_storage {

template<class value_t, class decoder_t>
class segment_meta_frame_const_iterator
  : public boost::iterator_facade<
      segment_meta_frame_const_iterator<value_t, decoder_t>,
      value_t const,
      boost::iterators::forward_traversal_tag> {
    constexpr static uint32_t buffer_depth = details::FOR_buffer_depth;
    constexpr static uint32_t index_mask = buffer_depth - 1;

    friend class boost::iterator_core_access;
    using self_t = segment_meta_frame_const_iterator<value_t, decoder_t>;

public:
    /// Create iterator that points to the begining
    explicit segment_meta_frame_const_iterator(
      decoder_t decoder,
      const std::array<value_t, buffer_depth>& head,
      uint32_t size)
      : _head(head)
      , _decoder(std::move(decoder))
      , _size(size) {
        if (!_decoder->read(_read_buf)) {
            _read_buf = _head;
        }
    }

    /// Create iterator that points to the end
    segment_meta_frame_const_iterator() = default;

    uint32_t index() const { return _pos; }

private:
    const value_t& dereference() const {
        auto ix = _pos & index_mask;
        return _read_buf.at(ix);
    }

    void increment() {
        _pos++;
        if ((_pos & index_mask) == 0) {
            // Read next buffer from the decoder
            _read_buf = {};
            if (!_decoder->read(_read_buf)) {
                // If the decoder is empty we need to continue reading data from
                // the head of the column.
                _read_buf = _head;
            }
        }
    }

    bool equal(const self_t& other) const {
        if (other._pos == other._size) {
            // All 'end' iterators are equal
            return _pos == _size;
        }
        return other._pos == _pos && other._size == _size;
    }

    std::array<value_t, buffer_depth> _read_buf{};
    std::array<value_t, buffer_depth> _head{};
    std::optional<decoder_t> _decoder{std::nullopt};
    uint32_t _pos{0};
    uint32_t _size{0};
};

template<class value_t, class delta_t = details::delta_xor>
class segment_meta_column_frame {
    constexpr static uint32_t buffer_depth = details::FOR_buffer_depth;
    constexpr static uint32_t index_mask = buffer_depth - 1;
    using encoder_t = deltafor_encoder<value_t, delta_t>;
    using decoder_t = deltafor_decoder<value_t, delta_t>;

    struct index_value {
        size_t ix;
        value_t value;
    };

public:
    using const_iterator
      = segment_meta_frame_const_iterator<value_t, decoder_t>;

    using delta_alg = delta_t;

    explicit segment_meta_column_frame(delta_alg d)
      : _delta_alg(std::move(d)) {}

    void append(value_t value) {
        auto ix = index_mask & _size++;
        _head.at(ix) = value;
        if ((_size & index_mask) == 0) {
            if (!_tail.has_value()) {
                _tail.emplace(_head.at(0), _delta_alg);
            }
            _tail->add(_head);
        }
    }

    const_iterator at(size_t index) const {
        if (index >= _size) {
            return end();
        }
        auto it = begin();
        std::advance(it, index);
        return it;
    }

    size_t size() const { return _size; }

    const_iterator begin() const {
        if (_tail.has_value()) {
            decoder_t decoder(
              _tail->get_initial_value(),
              _tail->get_row_count(),
              _tail->share(),
              _delta_alg);
            return const_iterator(std::move(decoder), _head, _size);
        } else if (_size != 0) {
            // special case, data is only stored in the buffer
            // not in the compressed column
            decoder_t decoder(0, 0, iobuf(), _delta_alg);
            return const_iterator(std::move(decoder), _head, _size);
        }
        return end();
    }

    const_iterator end() const { return const_iterator(); }

    const_iterator find(value_t value) const {
        return pred_search<std::equal_to<value_t>>(value);
    }

    const_iterator upper_bound(value_t value) const {
        return pred_search<std::greater<value_t>>(value);
    }

    const_iterator lower_bound(value_t value) const {
        return pred_search<std::greater_equal<value_t>>(value);
    }

    std::optional<value_t> last_value() const {
        if (_size == 0) {
            return std::nullopt;
        }
        auto ix = (_size - 1) & index_mask;
        return _head.at(ix);
    }

    bool contains(value_t value) const { return find(value) != end(); }

    void prefix_truncate(value_t new_start) {
        segment_meta_column_frame<value_t, delta_t> tmp(_delta_alg);
        for (auto it = find(new_start); it != end(); ++it) {
            tmp.append(*it);
        }
        _head = tmp._head;
        _tail = std::move(tmp._tail);
        _size = tmp._size;
    }

private:
    template<class PredT>
    const_iterator pred_search(value_t value) const {
        PredT pred;
        for (auto it = begin(); it != end(); ++it) {
            if (pred(*it, value)) {
                return it;
            }
        }
        return end();
    }

    std::array<value_t, buffer_depth> _head{};
    mutable std::optional<encoder_t> _tail{std::nullopt};
    const delta_t _delta_alg;
    size_t _size{0};
};

/// Column iterator
///
/// The core idea here is that the iterator stores an immutable
/// snapshot of the data so it's safe to use it in the asynchronous
/// and concurrent environment. The list of iterators to all frames
/// is stored in the vector. The iterators are referencing the immutable
/// copy of the column (the underlying iobuf is shared, the write buffer
/// is copied).
template<class value_t, class delta_t>
class segment_meta_column_const_iterator
  : public boost::iterator_facade<
      segment_meta_column_const_iterator<value_t, delta_t>,
      value_t const,
      boost::iterators::forward_traversal_tag> {
    friend class boost::iterator_core_access;

public:
    using frame_t = segment_meta_column_frame<value_t, delta_t>;
    using frame_iter_t = typename frame_t::const_iterator;

private:
    using outer_iter_t = typename std::list<frame_iter_t>::iterator;
    using iter_list_t = std::list<frame_iter_t>;
    using self_t = segment_meta_column_const_iterator<value_t, delta_t>;

    template<class container_t>
    static iter_list_t make_snapshot(const container_t& src) {
        iter_list_t snap;
        for (const auto& f : src) {
            auto i = f.begin();
            snap.push_back(std::move(i));
        }
        return snap;
    }

    template<class iter_t>
    static iter_list_t make_snapshot_at(iter_t begin, iter_t end, size_t ix) {
        iter_list_t snap;
        auto intr = begin->at(ix);
        snap.push_back(std::move(intr));
        std::advance(begin, 1);
        for (auto it = begin; it != end; ++it) {
            auto i = it->begin();
            snap.push_back(std::move(i));
        }
        return snap;
    }

    const value_t& dereference() const {
        vassert(_inner_end != _inner_it, "Can't dereference iterator");
        return *_inner_it;
    }

    void increment() {
        ++_inner_it;
        if (_inner_it == _inner_end) {
            ++_outer_it;
            _inner_it = _outer_it == _snapshot.end() ? frame_iter_t()
                                                     : std::move(*_outer_it);
        }
    }

    bool equal(const auto& other) const {
        // Invariant: _inner_it is never equal to _inner_end
        // unless the iterator points to the end.
        bool end_this = _inner_end == _inner_it;
        bool end_other = other._inner_end == other._inner_it;
        if (end_this && end_other) {
            // All 'end' iterators are equal
            return true;
        }
        return _outer_it == other._outer_it && _inner_it == other._inner_it;
        return true;
    }

public:
    /// Create iterator that points to the begining of the column
    template<class container_t>
    explicit segment_meta_column_const_iterator(const container_t& src)
      : _snapshot(make_snapshot(src))
      , _outer_it(_snapshot.begin())
      , _inner_it(
          _snapshot.empty() ? frame_iter_t{} : std::move(_snapshot.front())) {}

    /// Create iterator that points to the middle of the column
    template<class iterator_t>
    explicit segment_meta_column_const_iterator(
      iterator_t begin, iterator_t end, size_t intra_frame_ix)
      : _snapshot(make_snapshot_at(begin, end, intra_frame_ix))
      , _outer_it(_snapshot.begin())
      , _inner_it(
          _snapshot.empty() ? frame_iter_t{} : std::move(_snapshot.front())) {}

    /// Create iterator that points to the end of any column
    segment_meta_column_const_iterator()
      : _outer_it(_snapshot.end()) {}

private:
    iter_list_t _snapshot;
    outer_iter_t _outer_it;
    frame_iter_t _inner_it{};
    // We don't need to store 'end' iterator for
    // every 'begin' iterator we store in the '_snapshot'
    // because all 'end' iterators are equal.
    frame_iter_t _inner_end{};
};

/// Column that represents a signle field
///
/// There are two specializations of this template. One for delta_xor
/// algorithm and another one for delta_delta. The latter one is guaranteed
/// to be used with monotonic sequences which makes some serach optimizations
/// possible.
template<class value_t, class delta_t>
class segment_meta_column;

/// Column implementatoin
///
/// Contains a list of frames. The frames are not overlapping with each other.
/// The iterator is used to scan both all frames seamlessly.
/// This specialization is for xor-delta algorithm. It doesn't allow skipping
/// frames so all search operations require full scan. Random access by index
/// can skip frames since indexes are monotonic.
template<class value_t>
class segment_meta_column<value_t, details::delta_xor> {
    using frame_t = segment_meta_column_frame<value_t, details::delta_xor>;
    using decoder_t = deltafor_decoder<value_t, details::delta_xor>;
    static constexpr size_t max_frame_size = 0x1000;

public:
    using const_iterator
      = segment_meta_column_const_iterator<value_t, details::delta_xor>;

    using delta_alg = details::delta_xor;

    explicit segment_meta_column(delta_alg d)
      : _delta_alg(d) {}

    void append(value_t value) {
        if (_frames.empty() || _frames.back().size() == max_frame_size) {
            _frames.emplace_back(_delta_alg);
        }
        _frames.back().append(value);
    }

    const_iterator at(size_t index) const {
        auto ix_outer = index / max_frame_size;
        auto ix_inner = index % max_frame_size;
        auto it = _frames.begin();
        std::advance(it, ix_outer);
        return const_iterator(it, _frames.end(), ix_inner);
    }

    size_t size() const {
        if (_frames.empty()) {
            return 0;
        }
        return max_frame_size * (_frames.size() - 1) + _frames.back().size();
    }

    const_iterator begin() const { return const_iterator(_frames); }

    const_iterator end() const { return const_iterator(); }

    const_iterator find(value_t value) const {
        return pred_search<std::equal_to<value_t>>(value);
    }

    const_iterator upper_bound(value_t value) const {
        return pred_search<std::greater<value_t>>(value);
    }

    const_iterator lower_bound(value_t value) const {
        return pred_search<std::greater_equal<value_t>>(value);
    }

    std::optional<value_t> last_value() const {
        if (size() == 0) {
            return std::nullopt;
        }
        return _frames.back().last_value();
    }

    bool contains(value_t value) const { return find(value) != end(); }

    /// Prefix truncate the column
    ///
    /// \param new_start is value from which the column should start
    /// \note the method can only be used for monotonic sequences
    void prefix_truncate(value_t new_start) {
        auto st = _frames.begin();
        for (auto it = _frames.begin(); it != _frames.end(); it++) {
            if (it->last_value() < new_start) {
                st = it;
            } else {
                it->prefix_truncate(new_start);
                break;
            }
        }
        _frames.erase(_frames.begin(), st);
    }

private:
    template<class PredT>
    const_iterator pred_search(value_t value) const {
        PredT pred;
        for (auto it = begin(); it != end(); ++it) {
            if (pred(*it, value)) {
                return it;
            }
        }
        return end();
    }

    std::list<frame_t> _frames;
    const details::delta_xor _delta_alg;
};

class segment_meta_cstore {
    class impl;

public:
    using const_iterator = int;
    using const_reverse_iterator = int;

    const_iterator begin();
    const_iterator end();

    std::optional<segment_meta> last_segment();

    const_iterator find(model::offset) const;
    bool contains(model::offset) const;
    bool empty() const;
    const_iterator upper_bound(model::offset) const;
    const_iterator lower_bound(model::offset) const;

    std::pair<const_iterator, bool>
      insert(std::pair<model::offset, segment_meta>);

private:
    std::unique_ptr<impl> _impl;
};

} // namespace cloud_storage