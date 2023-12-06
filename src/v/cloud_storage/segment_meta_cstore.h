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

#include <absl/container/btree_map.h>
#include <boost/iterator/iterator_categories.hpp>
#include <boost/iterator/iterator_facade.hpp>

#include <functional>
#include <iterator>
#include <memory>
#include <tuple>
#include <variant>

namespace cloud_storage {

// each frame contains up to #cstore_max_frame_size elements, compressed with
// deltafor
constexpr size_t cstore_max_frame_size = 0x400;
// every #cstore_sampling_rate element inserted, the byte position in the
// compressed buffer is recorded to speed up random access
constexpr size_t cstore_sampling_rate = 8;

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
    /// Create iterator that points to the beginning
    explicit segment_meta_frame_const_iterator(
      decoder_t decoder,
      const std::array<value_t, buffer_depth>& head,
      uint32_t size,
      uint32_t pos = 0,
      value_t frame_initial_value = {})
      : _head(head)
      , _decoder(std::move(decoder))
      , _pos(pos)
      , _size(size)
      , _frame_initial(frame_initial_value) {
        if (!_decoder->read(_read_buf)) {
            _read_buf = _head;
        }
    }

    /// Create iterator that points to the end
    segment_meta_frame_const_iterator() = default;

    uint32_t index() const { return _pos; }

    value_t get_frame_initial_value() const noexcept { return _frame_initial; }

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
    value_t _frame_initial;
};

struct share_frame_t {};
inline constexpr auto share_frame = share_frame_t{};

template<class value_t, auto delta_alg_instance = details::delta_xor{}>
class segment_meta_column_frame
  : public serde::envelope<
      segment_meta_column_frame<value_t, delta_alg_instance>,
      serde::version<0>,
      serde::compat_version<0>> {
public:
    using delta_alg = std::remove_cvref_t<decltype(delta_alg_instance)>;

private:
    constexpr static uint32_t buffer_depth = details::FOR_buffer_depth;
    constexpr static uint32_t index_mask = buffer_depth - 1;
    using encoder_t
      = deltafor_encoder<value_t, delta_alg, true, delta_alg_instance>;
    using decoder_t = deltafor_decoder<value_t, delta_alg>;

    struct index_value {
        size_t ix;
        value_t value;
    };

    using self_t = segment_meta_column_frame<value_t, delta_alg_instance>;

public:
    segment_meta_column_frame() = default;

    // constructor that will share the underlying buffer of src
    segment_meta_column_frame(
      share_frame_t, segment_meta_column_frame& src) noexcept
      : _head{src._head}
      , _tail{[&] {
          if (src._tail.has_value()) {
              // invoke share constructor by passing a pointer to encoder_t
              return std::optional<encoder_t>{
                std::in_place, &(src._tail.value())};
          }
          return std::optional<encoder_t>{};
      }()}
      , _size{src._size}
      , _last_row{src._last_row} {}

    using const_iterator
      = segment_meta_frame_const_iterator<value_t, decoder_t>;

    using hint_t = deltafor_stream_pos_t<value_t>;

    void append(value_t value) {
        auto ix = index_mask & _size++;
        _head.at(ix) = value;
        if ((_size & index_mask) == 0) {
            if (!_tail.has_value()) {
                _tail.emplace(_head.at(0));
            }
            _last_row = _tail->get_position();
            _tail->add(_head);
        }
    }

    struct frame_tx_t {
        typename encoder_t::tx_state inner;
        self_t& self;

        frame_tx_t(typename encoder_t::tx_state&& s, self_t& self)
          : inner(std::move(s))
          , self(self) {}

        void add(const std::array<value_t, buffer_depth>& row) {
            inner.add(row);
        }
        void commit() && noexcept { self._tail->tx_commit(std::move(inner)); }
    };

    // Transactional append operation
    //
    // The method returns an optional with the state of the
    // transaction. If the append operation doesn't require
    // memory allocation the optional is null. Otherwise it
    // will contain a transaction which need to be commited.
    // The actual append operation can allocate memory and
    // can throw but 'commit' method of the transaction
    // doesn't allocate and doesn't throw.
    std::optional<frame_tx_t> append_tx(value_t value) {
        std::optional<frame_tx_t> tx;
        auto ix = index_mask & _size++;
        _head.at(ix) = value;
        if ((_size & index_mask) == 0) {
            if (!_tail.has_value()) {
                _tail.emplace(_head.at(0));
            }
            _last_row = _tail->get_position();
            tx.emplace(_tail->tx_start(), *this);
            tx->add(_head);
        }
        return tx;
    }

    /// Get stream position object which can be used as a "hint" to speed up
    /// index lookup operations.
    std::optional<hint_t> get_current_stream_pos() const { return _last_row; }

    const_iterator at_index(size_t index) const {
        if (index >= _size) {
            return end();
        }
        auto it = begin();
        std::advance(it, index);
        return it;
    }

    /// Get element by index, use 'hint' to speedup the operation.
    ///
    /// The 'hint' has to correspond to any index lower or equal to
    /// 'index'. The 'hint' values has to be stored externally.
    const_iterator at_index(size_t index, const hint_t& hint) const {
        if (index >= _size) {
            return end();
        }
        decoder_t decoder(
          _tail->get_initial_value(),
          _tail->get_row_count(),
          _tail->share(),
          delta_alg_instance);
        decoder.skip(hint);
        auto curr_ix = hint.num_rows * details::FOR_buffer_depth;
        auto it = const_iterator(
          std::move(decoder),
          _head,
          _size,
          curr_ix,
          _tail->get_initial_value());
        std::advance(it, index - curr_ix);
        return it;
    }

    bool is_applicable(const hint_t& hint) const {
        if (!_tail) {
            return false;
        }

        if constexpr (std::
                        is_same_v<delta_alg, details::delta_delta<value_t>>) {
            // check possible only with a monotonic frame
            if (!(_tail->get_initial_value() <= hint.initial
                  && hint.initial <= _tail->get_last_value())) {
                // ensure the value is in range
                return false;
            }
        }

        if (_tail->get_position().offset < hint.offset) {
            // ensure enough data in the buffer to apply the hint
            return false;
        }

        // has an encoded buffer, (if monotonic) data is in range, there are
        // enough bytes in the encoded buffer
        return true;
    }

    size_t size() const { return _size; }

    size_t mem_use() const {
        if (!_tail.has_value()) {
            return sizeof(*this);
        }
        return sizeof(*this) + _tail.value().mem_use();
    }

    /// Return first element of the frame or nullopt if frame is empty
    auto get_initial_value() const noexcept {
        if (_tail.has_value()) {
            return std::make_optional(_tail->get_initial_value());
        }
        return std::nullopt;
    }

    const_iterator begin() const {
        if (_tail.has_value()) {
            decoder_t decoder(
              _tail->get_initial_value(),
              _tail->get_row_count(),
              _tail->share(),
              delta_alg_instance);
            return const_iterator(
              std::move(decoder), _head, _size, 0, _tail->get_initial_value());
        } else if (_size != 0) {
            // special case, data is only stored in the buffer
            // not in the compressed column
            decoder_t decoder(0, 0, iobuf(), delta_alg_instance);
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

    /// Prefix truncate the frame. Index ix_exclusive is a new
    /// start of the frame.
    void prefix_truncate_ix(uint32_t ix_exclusive) {
        self_t tmp{};
        for (auto it = at_index(ix_exclusive); it != end(); ++it) {
            tmp.append(*it);
        }
        _head = tmp._head;
        _tail = std::move(tmp._tail);
        _size = tmp._size;
        _last_row = tmp._last_row;
    }

    auto serde_fields() { return std::tie(_head, _tail, _size, _last_row); }

    /// Returns a frame that shares the underlying iobuf
    /// The method itself is not unsafe, but the returned object can modify the
    /// original object, so users need to think about the use case
    auto unsafe_alias() const -> self_t {
        auto tmp = self_t{};
        tmp._head = _head;
        if (_tail.has_value()) {
            tmp._tail = _tail->unsafe_alias();
        }
        tmp._size = _size;
        tmp._last_row = _last_row;
        return tmp;
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
    std::optional<encoder_t> _tail{std::nullopt};
    size_t _size{0};
    std::optional<hint_t> _last_row{std::nullopt};
};

/// Column iterator
///
/// The core idea here is that the iterator stores an immutable
/// snapshot of the data so it's safe to use it in the asynchronous
/// and concurrent environment. The list of iterators to all frames
/// is stored in the vector. The iterators are referencing the immutable
/// copy of the column (the underlying iobuf is shared, the write buffer
/// is copied).
template<class value_t, auto delta_alg>
class segment_meta_column_const_iterator
  : public boost::iterator_facade<
      segment_meta_column_const_iterator<value_t, delta_alg>,
      value_t const,
      boost::iterators::forward_traversal_tag> {
    friend class boost::iterator_core_access;

public:
    using frame_t = segment_meta_column_frame<value_t, delta_alg>;
    using frame_iter_t = typename frame_t::const_iterator;
    using hint_t = typename frame_t::hint_t;

private:
    using outer_iter_t = typename std::list<frame_iter_t>::iterator;
    using iter_list_t = std::list<frame_iter_t>;
    using self_t = segment_meta_column_const_iterator<value_t, delta_alg>;

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
        auto intr = begin->at_index(ix);
        snap.push_back(std::move(intr));
        std::advance(begin, 1);
        for (auto it = begin; it != end; ++it) {
            auto i = it->begin();
            snap.push_back(std::move(i));
        }
        return snap;
    }

    template<class iter_t>
    static iter_list_t
    make_snapshot_at(iter_t begin, iter_t end, size_t ix, const hint_t& row) {
        iter_list_t snap;
        auto intr = begin->at_index(ix, row);
        snap.push_back(std::move(intr));
        std::advance(begin, 1);
        for (auto it = begin; it != end; ++it) {
            auto i = it->begin();
            snap.push_back(std::move(i));
        }
        return snap;
    }

    const value_t& dereference() const {
        vassert(!is_end(), "Can't dereference iterator");
        return *_inner_it;
    }

    void increment() {
#ifndef NDEBUG
        vassert(!is_end(), "can't increment iterator");
#endif
        ++_ix_column;
        ++_inner_it;
        if (_inner_it == _inner_end) {
            ++_outer_it;
            _inner_it = _outer_it == _snapshot.end() ? frame_iter_t()
                                                     : std::move(*_outer_it);
        }
    }

    bool equal(const auto& other) const {
        ssize_t idx = is_end() ? -1 : index();
        ssize_t oth_idx = other.is_end() ? -1 : other.index();
        return idx == oth_idx;
    }

public:
    /// Create iterator that points to the beginning of the column
    template<class container_t>
    explicit segment_meta_column_const_iterator(const container_t& src)
      : _snapshot(make_snapshot(src))
      , _outer_it(_snapshot.begin())
      , _inner_it(
          _snapshot.empty() ? frame_iter_t{} : std::move(_snapshot.front())) {}

    /// Create iterator that points to the middle of the column
    template<class iterator_t>
    explicit segment_meta_column_const_iterator(
      iterator_t begin,
      iterator_t end,
      uint32_t intra_frame_ix,
      uint32_t column_ix)
      : _snapshot(make_snapshot_at(begin, end, intra_frame_ix))
      , _outer_it(_snapshot.begin())
      , _inner_it(
          _snapshot.empty() ? frame_iter_t{} : std::move(_snapshot.front()))
      , _ix_column(column_ix) {}

    template<class iterator_t>
    explicit segment_meta_column_const_iterator(
      iterator_t begin,
      iterator_t end,
      uint32_t intra_frame_ix,
      const hint_t& row,
      uint32_t column_ix)
      : _snapshot(make_snapshot_at(begin, end, intra_frame_ix, row))
      , _outer_it(_snapshot.begin())
      , _inner_it(
          _snapshot.empty() ? frame_iter_t{} : std::move(_snapshot.front()))
      , _ix_column(column_ix) {}

    /// Create iterator that points to the end of any column
    segment_meta_column_const_iterator()
      : _outer_it(_snapshot.end()) {}

    // Current index
    size_t index() const { return _ix_column; }

    bool is_end() const {
        // Invariant: _inner_it is never equal to _inner_end
        // unless the iterator points to the end.
        return _inner_it == _inner_end;
    }

    auto get_frame_initial_value() const noexcept {
        return _outer_it->get_frame_initial_value();
    }

private:
    iter_list_t _snapshot;
    outer_iter_t _outer_it;
    frame_iter_t _inner_it{};
    // We don't need to store 'end' iterator for
    // every 'begin' iterator we store in the '_snapshot'
    // because all 'end' iterators are equal.
    frame_iter_t _inner_end{};
    // Current position inside the column
    size_t _ix_column{0};
};

/// Column that represents a single field
///
/// There are two specializations of this template. One for delta_xor
/// algorithm and another one for delta_delta. The latter one is guaranteed
/// to be used with monotonic sequences which makes some search
/// optimizations possible.
template<class value_t, class delta_t>
class segment_meta_column;

// to get rid of value_t and delta_alg
template<class value_t, auto delta_alg, class Derived>
class segment_meta_column_impl
  : public serde::envelope<
      segment_meta_column_impl<value_t, delta_alg, Derived>,
      serde::version<0>,
      serde::compat_version<0>> {
    using delta_t = std::remove_cvref_t<decltype(delta_alg)>;
    using frame_t = segment_meta_column_frame<value_t, delta_alg>;
    using decoder_t = deltafor_decoder<value_t, delta_t>;

    using self_t = segment_meta_column_impl<value_t, delta_alg, Derived>;

    // this friendship is used to access frame_t and _frames
    friend class column_store;

    // constructor that will share all the frames from the source range
    segment_meta_column_impl(
      share_frame_t, auto&& frame_iterator, auto&& frame_iterator_end) {
        for (; frame_iterator != frame_iterator_end; ++frame_iterator) {
            // this target the constructor that will share the underlying buffer
            _frames.emplace_back(share_frame, *frame_iterator);
        }
    }

    // crtp helper
    auto to_underlying() const -> decltype(auto) {
        return *static_cast<Derived const*>(this);
    }

public:
    /// Position in the column, can be used by
    /// index lookup operations
    using hint_t = typename frame_t::hint_t;

    static constexpr size_t max_frame_size = cstore_max_frame_size;
    using const_iterator
      = segment_meta_column_const_iterator<value_t, delta_alg>;

    segment_meta_column_impl() = default;

    segment_meta_column_impl(segment_meta_column_impl&&) = default;
    segment_meta_column_impl& operator=(segment_meta_column_impl&&) = default;

    void append(value_t value) {
        if (_frames.empty() || _frames.back().size() == max_frame_size) {
            _frames.push_back({});
        }
        _frames.back().append(value);
    }

    /// Return frame that contains value with index
    std::reference_wrapper<const frame_t>
    get_frame_by_element_index(size_t ix) {
        for (auto it = _frames.begin(); it != _frames.end(); ++it) {
            if (it->size() <= ix) {
                ix -= it->size();
            } else {
                return *it;
            }
        }
        vassert(false, "Invalid index {}", ix);
    }

    /// Get stream position object which can be used as a "hint" to speed up
    /// index lookup operations.
    std::optional<hint_t> get_current_stream_pos() const {
        if (_frames.empty()) {
            return std::nullopt;
        }
        return _frames.back().get_current_stream_pos();
    }

    struct column_tx_t {
        using frame_tx_t = typename frame_t::frame_tx_t;
        using frame_list_t = std::list<frame_t>;
        std::variant<frame_tx_t, frame_list_t> inner;
        self_t& self;

        column_tx_t(
          frame_tx_t inner,
          segment_meta_column_impl<value_t, delta_alg, Derived>& self)
          : inner(std::move(inner))
          , self(self) {}

        column_tx_t(
          frame_t frame,
          segment_meta_column_impl<value_t, delta_alg, Derived>& self)
          : inner(std::list<frame_t>())
          , self(self) {
            std::get<frame_list_t>(inner).push_back(std::move(frame));
        }

        void commit() && noexcept {
            if (std::holds_alternative<frame_tx_t>(inner)) {
                std::move(std::get<frame_tx_t>(inner)).commit();
            } else {
                self._frames.splice(
                  self._frames.end(), std::get<frame_list_t>(inner));
            }
        }
    };

    // Transactional append operation
    //
    // The method returns an optional with the state of the
    // transaction. If the append operation doesn't require
    // memory allocation the optional is null. Otherwise it
    // will contain a transaction which need to be commited.
    // The actual append operation can allocate memory and
    // can throw but 'commit' method of the transaction
    // doesn't allocate and doesn't throw.
    //
    // Transaction can be aborted by destroying the tx object.
    // Only one transaction can be active at a time. The caller
    // of the method is supposed to check the returned value
    // and to call 'commit' if it's not 'nullopt'.
    std::optional<column_tx_t> append_tx(value_t value) {
        std::optional<column_tx_t> tx;
        if (_frames.empty() || _frames.back().size() == max_frame_size) {
            frame_t tmp{};
            tmp.append(value);
            tx.emplace(std::move(tmp), *this);
            return tx;
        }
        auto inner = _frames.back().append_tx(value);
        if (!inner.has_value()) {
            // transactional op is already committed
            return tx;
        }
        tx.emplace(std::move(*inner), *this);
        return tx;
    }

    const_iterator at_index(size_t index) const {
        auto inner = index;
        for (auto it = _frames.begin(); it != _frames.end(); ++it) {
            if (it->size() <= inner) {
                inner -= it->size();
            } else {
                return const_iterator(it, _frames.end(), inner, index);
            }
        }
        return end();
    }

    /// Get element by index, use 'hint' to speedup the operation.
    ///
    /// The 'hint' has to correspond to any index lower or equal to
    /// 'index'. The 'hint' values has to be stored externally.
    const_iterator at_index(size_t index, const hint_t& hint) const {
        auto inner = index;
        for (auto it = _frames.begin(); it != _frames.end(); ++it) {
            if (it->size() <= inner) {
                inner -= it->size();
            } else {
                return const_iterator(it, _frames.end(), inner, hint, index);
            }
        }
        return end();
    }

    size_t size() const {
        if (_frames.empty()) {
            return 0;
        }
        size_t total = 0;
        for (const auto& f : _frames) {
            total += f.size();
        }
        return total;
    }

    bool empty() const {
        // short circuit at the first non empty frame
        return std::ranges::all_of(
          _frames, [](auto& f) { return f.size() == 0; });
    }

    size_t mem_use() const {
        size_t total = 0;
        for (const auto& p : _frames) {
            total += p.mem_use();
        }
        return sizeof(*this) + total;
    }

    const_iterator begin() const { return const_iterator(_frames); }

    const_iterator end() const { return const_iterator(); }

    const_iterator find(value_t value) const {
        return to_underlying().pred_search(value, std::equal_to<>{});
    }

    const_iterator upper_bound(value_t value) const {
        return to_underlying().pred_search(value, std::greater<>{});
    }

    const_iterator lower_bound(value_t value) const {
        return to_underlying().pred_search(value, std::greater_equal<>{});
    }

    std::optional<value_t> last_value() const {
        if (size() == 0) {
            return std::nullopt;
        }
        return _frames.back().last_value();
    }

    bool contains(value_t value) const { return find(value) != end(); }

    /// Prefix truncate column. Value at the position 'ix_exclusive' will
    /// become a new start of the column.
    void prefix_truncate_ix(uint32_t ix_exclusive) {
        for (auto it = _frames.begin(); it != _frames.end(); it++) {
            if (it->size() <= ix_exclusive) {
                ix_exclusive -= it->size();
            } else {
                it->prefix_truncate_ix(ix_exclusive);
                _frames.erase(_frames.begin(), it);
                break;
            }
        }
    }

    void serde_write(iobuf& out) {
        // std::list is not part of the serde-enabled types, save is as
        // size,elements
        auto const frames_size = _frames.size();
        if (unlikely(
              frames_size > std::numeric_limits<serde::serde_size_t>::max())) {
            throw serde::serde_exception(fmt_with_ctx(
              ssx::sformat,
              "serde: {}::_frames size {} exceeds serde_size_t",
              serde::type_str<self_t>(),
              frames_size));
        }
        serde::write(out, static_cast<serde::serde_size_t>(frames_size));
        for (auto& e : _frames) {
            serde::write(out, std::move(e));
        }
    }

    void serde_read(iobuf_parser& in, serde::header const& h) {
        // std::list is not part of the serde-enabled types, retrieve size and
        // push_back the elements
        if (unlikely(in.bytes_left() < h._bytes_left_limit)) {
            throw serde::serde_exception(fmt_with_ctx(
              ssx::sformat,
              "field spill over in {}, field type {}: envelope_end={}, "
              "in.bytes_left()={}",
              serde::type_str<self_t>(),
              serde::type_str<std::remove_reference_t<decltype(_frames)>>(),
              h._bytes_left_limit,
              in.bytes_left()));
        }
        auto const frames_size = serde::read_nested<serde::serde_size_t>(
          in, h._bytes_left_limit);
        for (auto i = 0U; i < frames_size; ++i) {
            _frames.push_back(
              serde::read_nested<frame_t>(in, h._bytes_left_limit));
        }
    }

    /// Returns a column that shares the underlying iobuf
    /// The method itself is not unsafe, but the returned object can modify the
    /// original object, so users need to think about the use case
    auto unsafe_alias() const -> Derived {
        auto tmp = Derived{};
        for (auto& e : _frames) {
            tmp._frames.push_back(e.unsafe_alias());
        }
        return tmp;
    }

protected:
    auto get_frame_iterator_by_element_index(size_t ix) const {
        return std::find_if(
          _frames.begin(), _frames.end(), [ix](frame_t const& f) mutable {
              if (f.size() > ix) {
                  return true;
              }
              ix -= f.size();
              return false;
          });
    }

    std::list<frame_t> _frames;
};

/// Segment metadata column.
///
/// Contains a list of frames. The frames are not overlapping with each
/// other. The iterator is used to scan both all frames seamlessly. This
/// specialization is for xor-delta algorithm. It doesn't allow skipping
/// frames so all search operations require full scan. Random access by
/// index can skip frames since indexes are monotonic.
template<class value_t>
class segment_meta_column<value_t, details::delta_xor>
  : public segment_meta_column_impl<
      value_t,
      details::delta_xor{},
      segment_meta_column<value_t, details::delta_xor>> {
    using base_t = segment_meta_column_impl<
      value_t,
      details::delta_xor{},
      segment_meta_column<value_t, details::delta_xor>>;

public:
    using delta_alg = details::delta_xor;
    using base_t::base_t;
    using typename base_t::const_iterator;

    const_iterator pred_search(
      value_t value, std::regular_invocable<value_t, value_t> auto pred) const {
        for (auto it = this->begin(); it != this->end(); ++it) {
            if (pred(*it, value)) {
                return it;
            }
        }
        return this->end();
    }
};

/// Segment metadata column for monotonic sequences.
///
/// Optimized for quick append/at/find operations.
/// Find/lower_bound/upper_bound operations are complited within
/// single digit microsecond intervals even with millions of elements
/// in the column. The actual decoding is only performed for a single frame.
/// The access by index is also fast (same order of magnitued as search).
template<class value_t>
class segment_meta_column<value_t, details::delta_delta<value_t>>
  : public segment_meta_column_impl<
      value_t,
      details::delta_delta<value_t>{},
      segment_meta_column<value_t, details::delta_delta<value_t>>> {
    using base_t = segment_meta_column_impl<
      value_t,
      details::delta_delta<value_t>{},
      segment_meta_column<value_t, details::delta_delta<value_t>>>;

public:
    using delta_alg = details::delta_delta<value_t>;
    using base_t::base_t;
    using typename base_t::const_iterator;

    const_iterator pred_search(
      value_t value, std::regular_invocable<value_t, value_t> auto pred) const {
        auto it = this->_frames.begin();
        size_t index = 0;
        for (; it != this->_frames.end(); ++it) {
            // The code is only used with equal/greater/greater_equal
            // predicates to implement find/lower_bound/upper_bound. Because
            // of that we can hardcode the '>=' operation here.
            if (it->last_value().has_value() && *it->last_value() >= value) {
                break;
            }
            index += it->size();
        }
        if (it != this->_frames.end()) {
            auto start = const_iterator(it, this->_frames.end(), 0, index);
            for (; start != this->end(); ++start) {
                if (pred(*start, value)) {
                    return start;
                }
            }
        }
        return this->end();
    }
};

/// Column-store iterator
///
/// The iterator materializes segment_meta structs when
/// it's dereferenced. The implementation can be configured
/// to touch only specific fields of the struct.
class segment_meta_materializing_iterator
  : public boost::iterator_facade<
      segment_meta_materializing_iterator,
      const segment_meta,
      boost::iterators::forward_traversal_tag> {
public:
    class impl;

    segment_meta_materializing_iterator() = default;
    segment_meta_materializing_iterator(
      const segment_meta_materializing_iterator&)
      = delete;
    segment_meta_materializing_iterator&
    operator=(const segment_meta_materializing_iterator&)
      = delete;
    segment_meta_materializing_iterator(segment_meta_materializing_iterator&&);
    segment_meta_materializing_iterator&
    operator=(segment_meta_materializing_iterator&&);
    explicit segment_meta_materializing_iterator(std::unique_ptr<impl>);

    ~segment_meta_materializing_iterator();

    /**
     * @return the index into the segment_meta_cstore for this iterator
     */
    size_t index() const;

    bool is_end() const;

private:
    friend class boost::iterator_core_access;
    const segment_meta& dereference() const;

    void increment();

    bool equal(const segment_meta_materializing_iterator& other) const;

    std::unique_ptr<impl> _impl;
};

/// Columnar storage for segment metadata.
/// The object stores segment_meta values using
/// a dedicated column for every field in the segment_meta
/// struct
class segment_meta_cstore {
    class impl;

public:
    using const_iterator = segment_meta_materializing_iterator;

    using int64_delta_alg = details::delta_delta<int64_t>;
    using int64_xor_alg = details::delta_xor;
    using counter_col_t = segment_meta_column<int64_t, int64_delta_alg>;
    using gauge_col_t = segment_meta_column<int64_t, int64_xor_alg>;

    segment_meta_cstore();
    segment_meta_cstore(segment_meta_cstore&&) noexcept;
    segment_meta_cstore& operator=(segment_meta_cstore&&) noexcept;
    ~segment_meta_cstore();

    bool operator==(segment_meta_cstore const& oth) const;
    /// Return iterator
    const_iterator begin() const;
    const_iterator end() const;

    /// Return last segment's metadata (or nullopt if empty)
    std::optional<segment_meta> last_segment() const;

    /// Find element and return its iterator
    const_iterator find(model::offset) const;

    /// Check if the offset is present
    bool contains(model::offset) const;

    /// Return true if data structure is empty
    bool empty() const;

    /// Return size of the collection
    size_t size() const;

    /// Upper/lower bound search operations
    const_iterator upper_bound(model::offset) const;
    const_iterator lower_bound(model::offset) const;
    const_iterator at_index(size_t ix) const;
    const_iterator prev(const_iterator const& it) const;

    void insert(const segment_meta&);

    class [[nodiscard]] append_tx {
    public:
        explicit append_tx(segment_meta_cstore&, const segment_meta&);
        ~append_tx();
        append_tx(const append_tx&) = delete;
        append_tx(append_tx&&) = delete;
        append_tx& operator=(const append_tx&) = delete;
        append_tx& operator=(append_tx&&) = delete;

        void rollback() noexcept;

    private:
        segment_meta _meta;
        segment_meta_cstore& _parent;
    };

    /// Add element to the c-store without flushing the
    /// write buffer. The new element can't replace any
    /// existing element in the write buffer.
    append_tx append(const segment_meta&);

    std::pair<size_t, size_t> inflated_actual_size() const;

    /// Removes all values up to the offset. The provided offset is
    /// a new start offset.
    void prefix_truncate(model::offset);

    void from_iobuf(iobuf in);

    iobuf to_iobuf() const;

    void flush_write_buffer();

    // Access individual columns
    const gauge_col_t& get_size_bytes_column() const;
    const counter_col_t& get_base_offset_column() const;
    const gauge_col_t& get_committed_offset_column() const;
    const gauge_col_t& get_delta_offset_end_column() const;
    const gauge_col_t& get_base_timestamp_column() const;
    const gauge_col_t& get_max_timestamp_column() const;
    const gauge_col_t& get_delta_offset_column() const;
    const gauge_col_t& get_segment_term_column() const;
    const gauge_col_t& get_archiver_term_column() const;

private:
    std::unique_ptr<impl> _impl;
};

} // namespace cloud_storage
