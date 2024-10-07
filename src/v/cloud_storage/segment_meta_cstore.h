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
inline constexpr size_t cstore_sampling_rate = 8;

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
    using counter_col_t
      = deltafor_column<int64_t, int64_delta_alg, cstore_max_frame_size>;
    using gauge_col_t
      = deltafor_column<int64_t, int64_xor_alg, cstore_max_frame_size>;

    segment_meta_cstore();
    segment_meta_cstore(segment_meta_cstore&&) noexcept;
    segment_meta_cstore& operator=(segment_meta_cstore&&) noexcept;
    ~segment_meta_cstore();

    bool operator==(const segment_meta_cstore& oth) const;
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
    const_iterator prev(const const_iterator& it) const;

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
