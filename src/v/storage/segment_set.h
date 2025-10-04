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

#include "features/fwd.h"
#include "storage/batch_cache.h"
#include "storage/file_sanitizer_types.h"
#include "storage/fs_utils.h"
#include "storage/fwd.h"
#include "storage/segment.h"

#include <seastar/core/sharded.hh>

#include <deque>

namespace storage {
/*
 * A container for log_segment_reader's. Usage:
 *
 * segment_set l;
 * l.add(some_log_segment);
 * ...
 * l.add(another_log_segment);
 * ...
 * for (auto seg : l) {
 *   // Do something with the segment
 * }
 */
class segment_set {
public:
    // type _must_ offer stable segment addresses
    // for readers and writers taking refs.
    using type = ss::lw_shared_ptr<segment>;

    using underlying_t = std::deque<type>;
    using const_iterator = underlying_t::const_iterator;
    using iterator = underlying_t::iterator;
    using reverse_iterator = underlying_t::reverse_iterator;
    using const_reverse_iterator = underlying_t::const_reverse_iterator;

    explicit segment_set(underlying_t);
    ~segment_set() noexcept;
    segment_set(segment_set&&) noexcept = default;
    segment_set& operator=(segment_set&& o) noexcept = default;
    segment_set& operator=(const segment_set&) = delete;

    size_t size() const { return _handles.size(); }

    bool empty() const { return _handles.empty(); }

    /// must be monotonically increasing in base offset
    void add(ss::lw_shared_ptr<segment>);

    void pop_back();
    void pop_front();
    void erase(iterator begin, iterator end);

    underlying_t release() && { return std::move(_handles); }
    type& back() { return _handles.back(); }
    const type& back() const { return _handles.back(); }
    const type& front() const { return _handles.front(); }
    type& operator[](size_t i) { return _handles[i]; }
    const type& operator[](size_t i) const { return _handles[i]; }

    iterator lower_bound(model::offset o);
    const_iterator lower_bound(model::offset o) const;
    iterator upper_bound(model::term_id o);
    const_iterator upper_bound(model::term_id o) const;

    const_iterator cbegin() const { return _handles.cbegin(); }
    const_iterator cend() const { return _handles.cend(); }
    iterator begin() { return _handles.begin(); }
    iterator end() { return _handles.end(); }
    const_iterator begin() const { return _handles.begin(); }
    const_iterator end() const { return _handles.end(); }

    reverse_iterator rbegin() { return _handles.rbegin(); }
    reverse_iterator rend() { return _handles.rend(); }
    const_reverse_iterator rbegin() const { return _handles.rbegin(); }
    const_reverse_iterator rend() const { return _handles.rend(); }

    segment_set copy() const noexcept { return *this; }

private:
    segment_set(const segment_set&) noexcept = default;

    underlying_t _handles;

    friend std::ostream& operator<<(std::ostream&, const segment_set&);
};

ss::future<segment_set> recover_segments(
  partition_path path,
  bool is_compaction_enabled,
  std::function<std::optional<batch_cache_index>()> batch_cache_factory,
  ss::abort_source& as,
  size_t read_buf_size,
  unsigned read_readahead_count,
  std::optional<ss::sstring> last_clean_segment,
  storage_resources&,
  ss::sharded<features::feature_table>& feature_table,
  std::optional<ntp_sanitizer_config> ntp_sanitizer_config);

// Attempts to create a contiguous & non-overlapping `segment_set` from those
// provided after recovery is performed. `segs` are first sorted in ascending
// order w/r/t base offset and descending w/r/t dirty offset. I.e, sorting
// `segment`s `[[0,100],[0,1000],[0,200],[1001,2000],[1001,2001]]` would result
// in the ordering `[[0,1000],[0,200],[0,100],[1001,2001],[1001,2000]]`. From
// there, a greedy selection is performed over the set to create a contiguous &
// non-overlapping `segment_set` by removing redundant `segment`s. If the
// acquired range is non-contiguous or has altered the offset span,
// `std::nullopt` is returned, indicating the desired range could not be
// created. Otherwise, the range is returned.
// This process deals with the scenario in which redundant `segment`s left over
// on disk by an adjacent merge compaction (i.e were scheduled for removal, but
// were not removed due to e.g. a broker crash) will not be included in the
// `log` post reboot.
ss::future<std::optional<segment_set>>
maybe_create_contiguous_segment_set(segment_set::underlying_t segs);

} // namespace storage
