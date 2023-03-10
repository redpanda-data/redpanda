/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/segment_meta_cstore.h"

#include "cloud_storage/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timestamp.h"
#include "utils/delta_for.h"

#include <bitset>
#include <functional>
#include <tuple>

namespace cloud_storage {

using int64_delta_alg = details::delta_delta<int64_t>;
using int64_xor_alg = details::delta_xor;
// Column for monotonically increasing data
using counter_col_t = segment_meta_column<int64_t, int64_delta_alg>;
// Column for varying data
using gauge_col_t = segment_meta_column<int64_t, int64_xor_alg>;

/// Sampling rate of the indexer inside the column store, if
/// sampling_rate == 1 every row is indexed, 2 - every second row, etc
/// The value 8 with max_frame_size set to 64 will give us 8 hints per
/// frame (frame has 64 rows). There is no measurable difference between
/// value 8 and smaller values.
static constexpr uint32_t sampling_rate = 8;

// Sampling rate should be proportional to max_frame_size so we will
// sample first row of every frame.
static_assert(
  gauge_col_t::max_frame_size % sampling_rate == 0, "Invalid sampling rate");

enum class segment_meta_ix {
    is_compacted,
    size_bytes,
    base_offset,
    committed_offset,
    base_timestamp,
    max_timestamp,
    delta_offset,
    ntp_revision,
    archiver_term,
    segment_term,
    delta_offset_end,
    sname_format,
    metadata_size_hint,
};

namespace details {
template<class T>
void commit_one(T&& tx) {
    if (tx.has_value()) {
        std::move(*tx).commit();
    }
}

template<class... Args>
void commit_all_impl(Args&&... args) {
    (commit_one(args), ...);
}

template<class... Args>
void commit_all(std::tuple<Args...>&& tup) {
    std::apply(commit_all_impl<Args...>, std::move(tup));
}

template<class... Args>
void maybe_increment_impl(Args&... args) {
    (++args, ...);
}

// Increment tuple of iterators
template<class... Args>
void increment_all(std::tuple<Args...>& tup) {
    std::apply(maybe_increment_impl<Args...>, tup);
}

/// The iters tuple is a tuple of std::optional<iterator-type>. The method
/// checks if the optional referenced by index is not none and dereferences.
template<segment_meta_ix ix, class T, class... Args>
void value_or(const std::tuple<Args...>& iters, T& res) {
    const auto& it = std::get<static_cast<size_t>(ix)>(iters);
    res = T(*it);
}

/// zip first_tuple with second_tuple, calls map_fn on each pair of elements,
/// collect results in a tuple if map_fn produces a result
template<typename Fn>
auto tuple_map(Fn&& map_fn, auto&& first_tuple, auto&& second_tuple) {
    return std::apply(
      [&]<typename... Ts>(Ts&&... first_param) {
          return std::apply(
            [&]<typename... Us>(Us&&... second_param) {
                if constexpr (std::is_void_v<std::invoke_result_t<
                                Fn&&,
                                decltype(std::get<0>(first_tuple)),
                                decltype(std::get<0>(second_tuple))>>) {
                    (std::invoke(
                       map_fn,
                       std::forward<Ts>(first_param),
                       std::forward<Us>(second_param)),
                     ...);
                } else {
                    return std::tuple{std::invoke(
                      map_fn,
                      std::forward<Ts>(first_param),
                      std::forward<Us>(second_param))...};
                }
            },
            second_tuple);
      },
      first_tuple);
}

} // namespace details

/// The aggregated columnar storage for segment_meta.
/// The data structure contains several columns, one per
/// field in the segment_meta struct. All columns have the same
/// number of elements at any point in time.
class column_store
  : public serde::
      envelope<column_store, serde::version<0>, serde::compat_version<0>> {
    using hint_t = deltafor_stream_pos_t<int64_t>;
    using hint_vec_t = std::array<hint_t, 13>;

    static constexpr size_t hint_array_size = sizeof(hint_vec_t); // (192 bytes)

    // The data type is used to store a map of 'hint' objects which
    // are stream pos objects. We're optimizing memory usage by not
    // storing index of the element as part of the hint_array_t because
    // all elements of the hint_array_t has the same index and it's the
    // same as the key.
    // The nullopt values act as a dividers between different frames.
    // Without them it will be possible to fetch hint that corresponds
    // to previous frame using lower_bound method. This will lead to
    // assertion. To prevent this we need to insert a nullopt when the
    // frame starts.

    using greater = std::greater<int64_t>;
    using hint_map_t
      = absl::btree_map<int64_t, std::optional<hint_vec_t>, greater>;

    auto columns() {
        return std::tie(
          _is_compacted,
          _size_bytes,
          _base_offset,
          _committed_offset,
          _base_timestamp,
          _max_timestamp,
          _delta_offset,
          _ntp_revision,
          _archiver_term,
          _segment_term,
          _delta_offset_end,
          _sname_format,
          _metadata_size_hint);
    }

    auto columns() const {
        return std::tie(
          _is_compacted,
          _size_bytes,
          _base_offset,
          _committed_offset,
          _base_timestamp,
          _max_timestamp,
          _delta_offset,
          _ntp_revision,
          _archiver_term,
          _segment_term,
          _delta_offset_end,
          _sname_format,
          _metadata_size_hint);
    }

    // helper used for serde_write/read and insert_entries
    auto member_fields() { return std::tuple_cat(columns(), std::tie(_hints)); }

    // projections from segment_meta to value_t used by the columns. the order
    // is the same of columns()
    constexpr static auto segment_meta_accessors = std::tuple{
      [](segment_meta const& s) {
          return static_cast<int64_t>(s.is_compacted);
      },
      [](segment_meta const& s) { return static_cast<int64_t>(s.size_bytes); },
      [](segment_meta const& s) { return s.base_offset(); },
      [](segment_meta const& s) { return s.committed_offset(); },
      [](segment_meta const& s) { return s.base_timestamp(); },
      [](segment_meta const& s) { return s.max_timestamp(); },
      [](segment_meta const& s) { return s.delta_offset(); },
      [](segment_meta const& s) { return s.ntp_revision(); },
      [](segment_meta const& s) { return s.archiver_term(); },
      [](segment_meta const& s) { return s.segment_term(); },
      [](segment_meta const& s) { return s.delta_offset_end(); },
      [](segment_meta const& s) {
          return static_cast<std::underlying_type_t<segment_name_format>>(
            s.sname_format);
      },
      [](segment_meta const& s) {
          return static_cast<int64_t>(s.metadata_size_hint);
      },
    };

    static_assert(
      reflection::arity<segment_meta>()
        == std::tuple_size_v<decltype(segment_meta_accessors)>,
      "segment_meta has a field that is not in segment_meta_accessors. check "
      "also that the members of column_store match the members of "
      "segment_meta");
    /**
     * private constructor used to create a store that share the underlying
     * buffers with another store
     * @param cols_iterators tuple<<begin,end>...> of
     * std::list<frame_t>::iterators for each column
     * @param hints_begin start of hints map
     * @param hints_end end of hints map
     */
    column_store(
      share_frame_t,
      auto cols_iterators,
      hint_map_t::const_iterator hints_begin,
      hint_map_t::const_iterator hints_end)
      : _hints{hints_begin, hints_end} {
        details::tuple_map(
          [](auto& col, auto& it_pair) {
              col = {share_frame, std::get<0>(it_pair), std::get<1>(it_pair)};
          },
          columns(),
          cols_iterators);
    }

public:
    using iterators_t = std::tuple<
      gauge_col_t::const_iterator,
      gauge_col_t::const_iterator,
      counter_col_t::const_iterator,
      gauge_col_t::const_iterator,
      gauge_col_t::const_iterator,
      gauge_col_t::const_iterator,
      counter_col_t::const_iterator,
      counter_col_t::const_iterator,
      gauge_col_t::const_iterator,
      counter_col_t::const_iterator,
      counter_col_t::const_iterator,
      counter_col_t::const_iterator,
      gauge_col_t::const_iterator>;

    column_store() = default;

    /// Add element to the store. The operation is transactional.
    void append(const segment_meta& meta) {
        auto ix = _base_offset.size();
        // C++ guarantees that all parameters of the function are
        // computed before the function is called. Here, every 'append_tx'
        // call returns a transaction that has to be committed. The 'append_tx'
        // is a transactional append operation and it's 'commit' method is
        // guaranteed to not throw exceptions. Because of that all 'append_tx'
        // calls will be completed before the 'commit_all' function will be
        // called. The 'commit_all' calls 'commit' method on all transactions,
        // committing them. If any 'append_tx' call will throw no column will be
        // updated and all transactions will be aborted. Because of that the
        // update is all or nothing even in presence of bad_alloc exceptions.
        details::commit_all(details::tuple_map(
          [&](auto& col, auto accessor) {
              return col.append_tx(std::invoke(accessor, meta));
          },
          columns(),
          segment_meta_accessors));

        if (
          ix
            % static_cast<uint32_t>(::details::FOR_buffer_depth * sampling_rate)
          == 0) {
            // At the beginning of every row we need to collect
            // a set of hints to speed up the subsequent random
            // reads.
            auto base_offset_hint = _base_offset.get_current_stream_pos();
            // Invariant: it's guaranteed that if we will get nullopt from
            // one column we will get nullopt from other columns. The opposite
            // is also true.
            if (base_offset_hint.has_value()) {
                auto tup = hint_vec_t{
                  *_is_compacted.get_current_stream_pos(),
                  *_size_bytes.get_current_stream_pos(),
                  *base_offset_hint,
                  *_committed_offset.get_current_stream_pos(),
                  *_base_timestamp.get_current_stream_pos(),
                  *_max_timestamp.get_current_stream_pos(),
                  *_delta_offset.get_current_stream_pos(),
                  *_ntp_revision.get_current_stream_pos(),
                  *_archiver_term.get_current_stream_pos(),
                  *_segment_term.get_current_stream_pos(),
                  *_delta_offset_end.get_current_stream_pos(),
                  *_sname_format.get_current_stream_pos(),
                  *_metadata_size_hint.get_current_stream_pos()};
                _hints.insert(std::make_pair(meta.base_offset(), tup));
            } else {
                _hints.insert(std::make_pair(meta.base_offset(), std::nullopt));
            }
        }
    }

    /**
     * reconstruct *this by replacing local segment_meta-s with replacement
     * segment_metas replacements must sorted and be [base_offset,
     * commit_offset] aligned to local segments
     * @param offset_seg_it a [base_offset, segment_meta] iterator
     * @param offset_seg_end end sentinel for offset_seg_it
     * @return a list of segment_meta that where evicted during the process
     */
    auto insert_entries(
      absl::btree_map<model::offset, segment_meta>::const_iterator
        offset_seg_it,
      absl::btree_map<model::offset, segment_meta>::const_iterator
        offset_seg_end) -> fragmented_vector<segment_meta> {
        if (offset_seg_it == offset_seg_end) {
            return {};
        }

        // construct a column_store with the frames and hints that are before
        // the replacements
        auto first_replacement_index
          = _base_offset.find(offset_seg_it->first()).index();
        // tuple of [begin, end] iterators to std::list<frame_t>
        auto to_clone_frames = std::apply(
          [&](auto&... col) {
              return std::tuple{std::tuple{
                col._frames.begin(),
                col.get_frame_iterator_by_element_index(
                  first_replacement_index)}...};
          },
          columns());

        // extract the end iterator for hints, to cover only the frames that
        // will be cloned
        auto end_hints = [&] {
            auto frame_it = _base_offset.get_frame_iterator_by_element_index(
              first_replacement_index);
            if (frame_it == _base_offset._frames.begin()) {
                // no hint will be saved
                return _hints.begin();
            }
            --frame_it; // go back to last frame that will be cloned
            auto frame_max_offset = frame_it->last_value();
            return _hints.upper_bound(
              frame_max_offset.value_or(model::offset::min()()));
        }();

        // this column_store is initialized with the run of segments that are
        // not changed
        auto replacement_store = column_store{
          share_frame, std::move(to_clone_frames), _hints.begin(), end_hints};

        auto unchanged_committed_offset
          = replacement_store.last_committed_offset().value_or(
            model::offset::min());
        vassert(
          unchanged_committed_offset < offset_seg_it->first(),
          "committed_offset of unchanged elements must be strictly less than "
          "replacements base_offset, by design");

        // iterator pointing to first segment not cloned into replacement_store
        auto old_segments_it = upper_bound(unchanged_committed_offset());
        auto old_segments_end = end();

        auto replaced_segments = fragmented_vector<segment_meta>{};
        // merge replacements and old segments into new store
        while (old_segments_it != old_segments_end
               && offset_seg_it != offset_seg_end) {
            auto [replacement_base_offset, replacement_meta] = *offset_seg_it;
            ++offset_seg_it;

            auto old_seg = dereference(old_segments_it);
            // append old segments with committed offset smaller than
            // replacement
            while (old_seg.committed_offset < replacement_base_offset) {
                replacement_store.append(old_seg);
                details::increment_all(old_segments_it);
                if (old_segments_it == old_segments_end) {
                    break;
                }
                old_seg = dereference(old_segments_it);
            }
            // old_segment_it points to first segment to replace

            // append replacement segment instead of the old one
            replacement_store.append(replacement_meta);

            // skip over segments superseded by replacement_meta
            while (old_segments_it != old_segments_end) {
                auto to_replace = dereference(
                  old_segments_it); // this could potentially be cached for the
                                    // next iteration of the outer loop
                if (
                  to_replace.base_offset > replacement_meta.committed_offset) {
                    break;
                }
                replaced_segments.push_back(to_replace);
                details::increment_all(old_segments_it);
            }
        }

        // drain remaining segments
        if (old_segments_it == old_segments_end) {
            for (; offset_seg_it != offset_seg_end; ++offset_seg_it) {
                replacement_store.append(offset_seg_it->second);
            }
        } else {
            // offset_seg_it==offset_seg_end
            for (; old_segments_it != old_segments_end;
                 details::increment_all(old_segments_it)) {
                replacement_store.append(dereference(old_segments_it));
            }
        }

        // perform update of *this by stealing the fields from replacement
        auto current_data = member_fields();
        auto replacement_data = replacement_store.member_fields();
        std::swap(current_data, replacement_data);
        return replaced_segments;
    }

    static segment_meta dereference(const iterators_t& it) {
        segment_meta meta = {};
        details::value_or<segment_meta_ix::is_compacted>(it, meta.is_compacted);
        details::value_or<segment_meta_ix::size_bytes>(it, meta.size_bytes);
        details::value_or<segment_meta_ix::base_offset>(it, meta.base_offset);
        details::value_or<segment_meta_ix::committed_offset>(
          it, meta.committed_offset);
        details::value_or<segment_meta_ix::base_timestamp>(
          it, meta.base_timestamp);
        details::value_or<segment_meta_ix::max_timestamp>(
          it, meta.max_timestamp);
        details::value_or<segment_meta_ix::delta_offset>(it, meta.delta_offset);
        details::value_or<segment_meta_ix::ntp_revision>(it, meta.ntp_revision);
        details::value_or<segment_meta_ix::archiver_term>(
          it, meta.archiver_term);
        details::value_or<segment_meta_ix::segment_term>(it, meta.segment_term);
        details::value_or<segment_meta_ix::delta_offset_end>(
          it, meta.delta_offset_end);
        details::value_or<segment_meta_ix::sname_format>(it, meta.sname_format);
        details::value_or<segment_meta_ix::metadata_size_hint>(
          it, meta.metadata_size_hint);
        return meta;
    }

    /// Return last element in the sequence or nullopt
    std::optional<segment_meta> last_segment() const {
        if (_base_offset.size() == 0) {
            return std::nullopt;
        }
        segment_meta meta = {
          .is_compacted = static_cast<bool>(*_is_compacted.last_value()),
          .size_bytes = static_cast<size_t>(*_size_bytes.last_value()),
          .base_offset = model::offset(*_base_offset.last_value()),
          .committed_offset = model::offset(*_committed_offset.last_value()),
          .base_timestamp = model::timestamp(*_base_timestamp.last_value()),
          .max_timestamp = model::timestamp(*_max_timestamp.last_value()),
          .delta_offset = model::offset_delta(*_delta_offset.last_value()),
          .ntp_revision = model::initial_revision_id(
            *_ntp_revision.last_value()),
          .archiver_term = model::term_id(*_archiver_term.last_value()),
          .segment_term = model::term_id(*_segment_term.last_value()),
          .delta_offset_end = model::offset_delta(
            *_delta_offset_end.last_value()),
          .sname_format = static_cast<segment_name_format>(
            *_sname_format.last_value()),
          .metadata_size_hint = static_cast<uint64_t>(
            *_metadata_size_hint.last_value()),
        };
        return meta;
    }

    auto last_committed_offset() const -> std::optional<model::offset> {
        if (_base_offset.size() == 0) {
            return std::nullopt;
        }
        return model::offset(*_committed_offset.last_value());
    }

    /// Return iterator to the end of the sequence
    auto begin() const -> iterators_t {
        // Individual iterators can be accessed using
        // segment_meta_ix values as tuple indexes.
        return std::apply(
          [](auto&&... col) { return iterators_t(col.begin()...); }, columns());
    }

    /// Return iterator to the first element after the end of the sequence
    auto end() const -> iterators_t {
        return std::apply(
          [](auto&&... col) { return iterators_t(col.end()...); }, columns());
    }

    template<segment_meta_ix col_index, class col_t>
    auto at_with_hint(
      const col_t& c, uint32_t ix, const std::optional<hint_vec_t>& h) const {
        vassert(h.has_value(), "Invalid access at index {}", ix);
        return c.at_index(
          ix, std::get<static_cast<size_t>(col_index)>(h.value()));
    }

    /// Materialize 'segment_meta' struct from column iterator
    ///
    ///
    auto materialize(counter_col_t::const_iterator base_offset_iter) const
      -> iterators_t {
        if (base_offset_iter == _base_offset.end()) {
            return end();
        }
        auto bo = *base_offset_iter;
        auto ix = base_offset_iter.index();
        auto hint_it = _hints.lower_bound(bo);
        if (hint_it == _hints.end() || hint_it->second == std::nullopt) {
            return iterators_t(
              _is_compacted.at_index(ix),
              _size_bytes.at_index(ix),
              std::move(base_offset_iter),
              _committed_offset.at_index(ix),
              _base_timestamp.at_index(ix),
              _max_timestamp.at_index(ix),
              _delta_offset.at_index(ix),
              _ntp_revision.at_index(ix),
              _archiver_term.at_index(ix),
              _segment_term.at_index(ix),
              _delta_offset_end.at_index(ix),
              _sname_format.at_index(ix),
              _metadata_size_hint.at_index(ix));
        }

        auto hint = hint_it->second;
        return iterators_t(
          at_with_hint<segment_meta_ix::is_compacted>(_is_compacted, ix, hint),
          at_with_hint<segment_meta_ix::size_bytes>(_size_bytes, ix, hint),
          std::move(base_offset_iter),
          at_with_hint<segment_meta_ix::committed_offset>(
            _committed_offset, ix, hint),
          at_with_hint<segment_meta_ix::base_timestamp>(
            _base_timestamp, ix, hint),
          at_with_hint<segment_meta_ix::max_timestamp>(
            _max_timestamp, ix, hint),
          at_with_hint<segment_meta_ix::delta_offset>(_delta_offset, ix, hint),
          at_with_hint<segment_meta_ix::ntp_revision>(_ntp_revision, ix, hint),
          at_with_hint<segment_meta_ix::archiver_term>(
            _archiver_term, ix, hint),
          at_with_hint<segment_meta_ix::segment_term>(_segment_term, ix, hint),
          at_with_hint<segment_meta_ix::delta_offset_end>(
            _delta_offset_end, ix, hint),
          at_with_hint<segment_meta_ix::sname_format>(_sname_format, ix, hint),
          at_with_hint<segment_meta_ix::metadata_size_hint>(
            _metadata_size_hint, ix, hint));
    }

    /// Search by base_offset
    auto find(int64_t bo) const {
        auto it = _base_offset.find(bo);
        return materialize(std::move(it));
    }

    /// Search by base_offset
    auto lower_bound(int64_t bo) const {
        auto it = _base_offset.lower_bound(bo);
        return materialize(std::move(it));
    }

    /// Search by base_offset
    auto upper_bound(int64_t bo) const -> iterators_t {
        auto it = _base_offset.upper_bound(bo);
        return materialize(std::move(it));
    }

    /// Search by index
    auto at_index(size_t ix) const {
        auto it = _base_offset.at_index(ix);
        return materialize(std::move(it));
    }

    void clear() {
        // replace *this with an empty instance
        auto replacement = column_store{};
        auto current_fields = member_fields();
        auto new_fields = replacement.member_fields();
        std::swap(current_fields, new_fields);
    }
    void prefix_truncate(int64_t bo) {
        auto lb = _base_offset.lower_bound(bo);
        if (lb == _base_offset.end()) {
            clear();
            return;
        }
        auto ix = lb.index();
        // We need to remove hints that belong to the first frame.
        // This frame was truncated and therefore the hints that
        // belong to it are no longer valid.
        const auto& frame = _base_offset.get_frame_by_element_index(ix).get();
        auto frame_max_offset = frame.last_value();
        auto it = _hints.upper_bound(frame_max_offset.value_or(bo));

        // Truncate columns
        std::apply(
          [ix](auto&&... col) { (col.prefix_truncate_ix(ix), ...); },
          columns());

        // The elements are ordered by base offset from large to small
        // so the hints that belong to removed and truncated frames are
        // at the end.
        _hints.erase(it, _hints.end());
    }

    /// Return two values: inflated size (size without compression) followed
    /// by the actual size that takes compression into account.
    std::pair<size_t, size_t> inflated_actual_size() const {
        auto inflated_size = _base_offset.size() * sizeof(segment_meta);
        auto actual_size = std::apply(
          [](auto&&... col) { return (col.mem_use() + ...); }, columns());
        auto index_size = static_cast<size_t>(
          _hints.size() * sizeof(hint_map_t::value_type)
          * 1.4); // The size of the _hints is an estimate based on absl docs
        return std::make_pair(inflated_size, actual_size + index_size);
    }

    size_t size() const { return _base_offset.size(); }

    bool contains(int64_t o) const {
        auto it = _base_offset.find(o);
        return it != _base_offset.end();
    }

    bool empty() const { return size() == 0; }

    void serde_write(iobuf& out) {
        // hint_map_t (absl::btree_map) is not serde-enabled, it's serialized
        // manually as size,[(key,value)...]
        auto field_writer = [&out]<typename FieldType>(FieldType& f) {
            if constexpr (std::same_as<FieldType, hint_map_t>) {
                if (unlikely(
                      f.size()
                      > std::numeric_limits<serde::serde_size_t>::max())) {
                    throw serde::serde_exception(fmt_with_ctx(
                      ssx::sformat,
                      "serde: {} size {} exceeds serde_size_t",
                      serde::type_str<column_store>(),
                      f.size()));
                }
                serde::write(out, static_cast<serde::serde_size_t>(f.size()));
                for (auto& [k, v] : f) {
                    serde::write(out, k);
                    serde::write(out, std::move(v));
                }
            } else {
                serde::write(out, std::move(f));
            }
        };
        std::apply(
          [&](auto&... field) { (field_writer(field), ...); }, member_fields());
    }

    void serde_read(iobuf_parser& in, serde::header const& h) {
        // hint_map_t (absl::btree_map) is not serde-enabled, read it as
        // size,[(key,value)...]
        auto field_reader = [&]<typename FieldType>(FieldType& f) {
            if (h._bytes_left_limit == in.bytes_left()) {
                return false;
            }
            if (unlikely(in.bytes_left() < h._bytes_left_limit)) {
                throw serde::serde_exception(fmt_with_ctx(
                  ssx::sformat,
                  "field spill over in {}, field type {}: envelope_end={}, "
                  "in.bytes_left()={}",
                  serde::type_str<column_store>(),
                  serde::type_str<FieldType>(),
                  h._bytes_left_limit,
                  in.bytes_left()));
            }
            if constexpr (std::same_as<hint_map_t, FieldType>) {
                const auto size = serde::read_nested<serde::serde_size_t>(
                  in, h._bytes_left_limit);
                for (auto i = 0U; i < size; ++i) {
                    auto key
                      = serde::read_nested<typename hint_map_t::key_type>(
                        in, h._bytes_left_limit);
                    auto value
                      = serde::read_nested<typename hint_map_t::mapped_type>(
                        in, h._bytes_left_limit);
                    f.emplace(std::move(key), std::move(value));
                }
            } else {
                f = serde::read_nested<FieldType>(in, h._bytes_left_limit);
            }
            return true;
        };
        std::apply(
          [&](auto&... field) { (void)(field_reader(field) && ...); },
          member_fields());
    }

private:
    gauge_col_t _is_compacted{};
    gauge_col_t _size_bytes{};
    counter_col_t _base_offset{};
    gauge_col_t _committed_offset{};
    gauge_col_t _base_timestamp{};
    gauge_col_t _max_timestamp{};
    counter_col_t _delta_offset{};
    counter_col_t _ntp_revision{};
    /// The archiver term is not strictly monotonic in manifests
    /// generated by old redpanda versions
    gauge_col_t _archiver_term{};
    counter_col_t _segment_term{};
    counter_col_t _delta_offset_end{};
    counter_col_t _sname_format{};
    gauge_col_t _metadata_size_hint{};

    hint_map_t _hints{};
};

/// Materializing iterator implementation
class segment_meta_materializing_iterator::impl {
public:
    explicit impl(column_store::iterators_t iters)
      : _iters(std::move(iters))
      , _curr(std::nullopt) {}

    const segment_meta& dereference() const {
        if (!_curr.has_value()) {
            _curr = column_store::dereference(_iters);
        }
        return _curr.value();
    }

    void increment() {
        _curr = std::nullopt;
        details::increment_all(_iters);
    }

    bool equal(const impl& other) const { return _iters == other._iters; }

private:
    column_store::iterators_t _iters;
    mutable std::optional<segment_meta> _curr;
};

segment_meta_materializing_iterator::segment_meta_materializing_iterator(
  std::unique_ptr<impl> i)
  : _impl(std::move(i)) {}

segment_meta_materializing_iterator::~segment_meta_materializing_iterator() {}

const segment_meta& segment_meta_materializing_iterator::dereference() const {
    return _impl->dereference();
}

void segment_meta_materializing_iterator::increment() { _impl->increment(); }

bool segment_meta_materializing_iterator::equal(
  const segment_meta_materializing_iterator& other) const {
    return _impl->equal(*other._impl);
}

/// Column store implementation
class segment_meta_cstore::impl
  : public serde::envelope<
      segment_meta_cstore::impl,
      serde::version<0>,
      serde::compat_version<0>> {
    // TODO tunable?
    constexpr static auto max_buffer_entries = 1024u;

public:
    void append(const segment_meta& meta) { _col.append(meta); }

    std::unique_ptr<segment_meta_materializing_iterator::impl> begin() const {
        flush_write_buffer();
        return std::make_unique<segment_meta_materializing_iterator::impl>(
          _col.begin());
    }

    std::unique_ptr<segment_meta_materializing_iterator::impl> end() const {
        flush_write_buffer();
        return std::make_unique<segment_meta_materializing_iterator::impl>(
          _col.end());
    }

    std::unique_ptr<segment_meta_materializing_iterator::impl>
    find(model::offset o) const {
        flush_write_buffer();
        return std::make_unique<segment_meta_materializing_iterator::impl>(
          _col.find(o()));
    }

    std::unique_ptr<segment_meta_materializing_iterator::impl>
    lower_bound(model::offset o) const {
        flush_write_buffer();
        return std::make_unique<segment_meta_materializing_iterator::impl>(
          _col.lower_bound(o()));
    }

    std::unique_ptr<segment_meta_materializing_iterator::impl>
    upper_bound(model::offset o) const {
        flush_write_buffer();
        return std::make_unique<segment_meta_materializing_iterator::impl>(
          _col.upper_bound(o()));
    }

    std::optional<segment_meta> last_segment() const {
        if (!_write_buffer.empty()) {
            auto last_committed_offset = _col.last_committed_offset().value_or(
              model::offset::min());

            auto& buffer_last_seg = _write_buffer.crbegin()->second;
            if (likely(
                  buffer_last_seg.committed_offset >= last_committed_offset)) {
                // return last one in _write_buffer
                return buffer_last_seg;
            }
            // fallthrough
        }
        return _col.last_segment();
    }

    void insert(const segment_meta& m) {
        auto [m_it, _] = _write_buffer.insert_or_assign(m.base_offset, m);
        // the new segment_meta could be a replacement for subsequent entries in
        // the buffer, so do a pass to clean them
        if (_write_buffer.size() > 1) {
            auto not_replaced_segment = std::find_if(
              std::next(m_it), _write_buffer.end(), [&](auto& kv) {
                  return kv.first > m.committed_offset;
              });
            // if(next(m_it) == not_replaced_segment) there is nothing to erase,
            // _write_buffer.erase would do nothing
            _write_buffer.erase(std::next(m_it), not_replaced_segment);
        }

        if (_write_buffer.size() > max_buffer_entries) {
            flush_write_buffer();
        }
    }

    auto inflated_actual_size() const {
        // TODO how to deal with write_buffer? is it ok to return an approx
        // value by not flushing?
        return _col.inflated_actual_size();
    }

    size_t size() const {
        flush_write_buffer();
        return _col.size();
    }

    bool empty() const { return _write_buffer.empty() && _col.empty(); }

    bool contains(model::offset o) {
        return _write_buffer.contains(o) || _col.contains(o());
    }

    void prefix_truncate(model::offset new_start_offset) {
        auto new_begin = _write_buffer.lower_bound(new_start_offset);
        _write_buffer.erase(_write_buffer.begin(), new_begin);
        _col.prefix_truncate(new_start_offset());
    }

    std::unique_ptr<segment_meta_materializing_iterator::impl>
    at_index(size_t ix) const {
        flush_write_buffer();
        return std::make_unique<segment_meta_materializing_iterator::impl>(
          _col.at_index(ix));
    }

    void serde_write(iobuf& out) {
        flush_write_buffer();
        serde::write(out, std::exchange(_col, {}));
    }
    void serde_read(iobuf_parser& in, serde::header const& h) {
        if (h._bytes_left_limit == in.bytes_left()) {
            return;
        }
        if (unlikely(in.bytes_left() < h._bytes_left_limit)) {
            throw serde::serde_exception(fmt_with_ctx(
              ssx::sformat,
              "field spill over in {}, field type {}: envelope_end={}, "
              "in.bytes_left()={}",
              serde::type_str<segment_meta_cstore::impl>(),
              serde::type_str<column_store>(),
              h._bytes_left_limit,
              in.bytes_left()));
        }

        _write_buffer.clear();
        _col = serde::read_nested<column_store>(in, h._bytes_left_limit);
    }

private:
    void flush_write_buffer() const {
        if (_write_buffer.empty()) {
            return;
        }
        _col.insert_entries(_write_buffer.begin(), _write_buffer.end());
        _write_buffer.clear();
    }

    mutable absl::btree_map<model::offset, segment_meta> _write_buffer{};
    mutable column_store _col{};
};

segment_meta_cstore::segment_meta_cstore()
  : _impl(std::make_unique<impl>()) {}

segment_meta_cstore::~segment_meta_cstore() {}

segment_meta_cstore::const_iterator segment_meta_cstore::begin() const {
    return const_iterator(_impl->begin());
}

std::pair<size_t, size_t> segment_meta_cstore::inflated_actual_size() const {
    return _impl->inflated_actual_size();
}

segment_meta_cstore::const_iterator segment_meta_cstore::end() const {
    return const_iterator(_impl->end());
}

std::optional<segment_meta> segment_meta_cstore::last_segment() const {
    return _impl->last_segment();
}

segment_meta_cstore::const_iterator
segment_meta_cstore::find(model::offset o) const {
    return const_iterator(_impl->find(o));
}

bool segment_meta_cstore::contains(model::offset o) const {
    return _impl->contains(o);
}

bool segment_meta_cstore::empty() const { return _impl->empty(); }

size_t segment_meta_cstore::size() const { return _impl->size(); }

segment_meta_cstore::const_iterator
segment_meta_cstore::upper_bound(model::offset o) const {
    return const_iterator(_impl->upper_bound(o));
}

segment_meta_cstore::const_iterator
segment_meta_cstore::lower_bound(model::offset o) const {
    return const_iterator(_impl->lower_bound(o));
}

void segment_meta_cstore::insert(const segment_meta& s) { _impl->insert(s); }

void segment_meta_cstore::prefix_truncate(model::offset new_start_offset) {
    return _impl->prefix_truncate(new_start_offset);
}

segment_meta_cstore::const_iterator
segment_meta_cstore::at_index(size_t ix) const {
    return const_iterator(_impl->at_index(ix));
}

void segment_meta_cstore::from_iobuf(iobuf in) {
    // NOTE: this process is not optimal memory-wise, but it's simple and
    // correct. It would require some rewrite on serde to accept a type& out
    // parameter.
    *_impl = serde::from_iobuf<segment_meta_cstore::impl>(std::move(in));
}

iobuf segment_meta_cstore::to_iobuf() {
    return serde::to_iobuf(std::exchange(*_impl, {}));
}
} // namespace cloud_storage
