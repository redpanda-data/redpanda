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

#include "storage/batch_cache.h"
#include "storage/compacted_index_writer.h"
#include "storage/file_sanitizer_types.h"
#include "storage/fs_utils.h"
#include "storage/fwd.h"
#include "storage/segment_appender.h"
#include "storage/segment_index.h"
#include "storage/segment_reader.h"
#include "storage/types.h"
#include "storage/version.h"

#include <seastar/core/file.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/sharded.hh>

#include <exception>
#include <optional>

namespace storage {
struct segment_closed_exception final : std::exception {
    const char* what() const noexcept final {
        return "segment_closed exception";
    }
};

class segment {
public:
    using generation_id = named_type<uint64_t, struct segment_gen_tag>;
    class offset_tracker {
    public:
        using committed_offset_t
          = named_type<model::offset, struct committed_offset_tag>;
        using stable_offset_t
          = named_type<model::offset, struct stable_offset_tag>;
        using dirty_offset_t
          = named_type<model::offset, struct dirty_offset_tag>;

        offset_tracker(model::term_id t, model::offset base)
          : _term(t)
          , _base_offset(base)
          , _committed_offset(model::prev_offset(base))
          , _stable_offset(model::prev_offset(base))
          , _dirty_offset(model::prev_offset(base)) {}

        template<typename... Ts>
        void set_offsets(Ts... ts) {
            (set_offset_impl(ts), ...);
            vassert(
              _committed_offset <= _stable_offset
                && _stable_offset <= _dirty_offset,
              "Must maintain offset invariant: committed ({}) <= stable ({}) "
              "<= dirty ({})",
              _committed_offset,
              _stable_offset,
              _dirty_offset);
        }

        template<typename T>
        void set_offset(T t) {
            set_offsets(t);
        }

        model::term_id get_term() const { return _term; }
        model::offset get_base_offset() const { return _base_offset; }
        model::offset get_committed_offset() const { return _committed_offset; }
        model::offset get_stable_offset() const { return _stable_offset; }
        model::offset get_dirty_offset() const { return _dirty_offset; }

    private:
        template<typename T>
        void set_offset_impl(T tagged_offset) {
            if constexpr (std::is_same_v<T, committed_offset_t>) {
                _committed_offset = tagged_offset();
            } else if constexpr (std::is_same_v<T, stable_offset_t>) {
                _stable_offset = tagged_offset();
            } else if constexpr (std::is_same_v<T, dirty_offset_t>) {
                _dirty_offset = tagged_offset();
            } else {
                static_assert(always_false_v<T>, "Invalid offset type");
            }
        }

        model::term_id _term;
        model::offset _base_offset;

        /// \brief These offsets are the `batch.last_offset()` and not
        /// `batch.base_offset()` which might be confusing at first,
        /// but allow us to keep track of the actual last logical offset

        // Offset of last message fsynced to disk.
        model::offset _committed_offset;
        // Offset of last message written to disk, may not yet have been
        // fsynced.
        model::offset _stable_offset;
        // Offset of last message written to this log, may not yet be stable.
        model::offset _dirty_offset;

        friend std::ostream& operator<<(std::ostream&, const offset_tracker&);
    };
    enum class bitflags : uint32_t {
        none = 0,
        is_compacted_segment = 1,
        finished_self_compaction = 1U << 1U,
        mark_tombstone = 1U << 2U,
        closed = 1U << 3U,
        finished_windowed_compaction = 1U << 4U,
    };

public:
    segment(
      offset_tracker tracker,
      segment_reader_ptr,
      segment_index,
      segment_appender_ptr,
      std::optional<compacted_index_writer>,
      std::optional<batch_cache_index>,
      storage_resources&,
      generation_id = generation_id{}) noexcept;
    ~segment() noexcept = default;
    segment(segment&&) noexcept = default;
    // rwlock does not have move-assignment
    segment& operator=(segment&&) noexcept = delete;
    segment(const segment&) = delete;
    segment& operator=(const segment&) = delete;

    ss::future<> close();
    ss::future<> flush();
    ss::future<> release_appender(readers_cache*);
    ss::future<> truncate(
      model::offset, size_t physical, model::timestamp new_max_timestamp);

    /// main write interface
    /// auto indexes record_batch
    ///
    /// We recommend using the const-ref method below over the r-value since we
    /// do not need to take ownership of the batch itself
    ///
    /// Appending does not offer read-your-writes consistency in the general
    /// case. It only buffers data in-memory and no guarantees are made about
    /// when the data will be available fo reading.
    ///
    /// If you need to read the data you either need to wait for the stable
    /// offset to cover `append_result::last_offset`^1 or flush the segment.
    ///
    /// In case batch_cache_index is provided, the batch will be available for
    /// will be available there immediately after and at least until it is
    /// written to disk. The batch cache can be used to read data immediately
    /// after it is appended.^2
    ///
    /// ^1: This is what `log_segment_batch_reader` does
    ///     https://github.com/redpanda-data/redpanda/blob/b4f54b1dc71c1f6750a319f6ef0efa6192bb95d7/src/v/storage/log_reader.cc#L246-L254
    /// ^2: This is what `log_reader` does via `log_segment_batch_reader`
    ///     https://github.com/redpanda-data/redpanda/blob/b4f54b1dc71c1f6750a319f6ef0efa6192bb95d7/src/v/storage/log_reader.cc#L224-L230
    ss::future<append_result> append(model::record_batch&&);

    /// See the overload for r-value batches above for documentation.
    ss::future<append_result> append(const model::record_batch&);
    ss::future<append_result> do_append(const model::record_batch&);
    ss::future<bool> materialize_index();

    /// main read interface
    ss::future<segment_reader_handle>
      offset_data_stream(model::offset, ss::io_priority_class);

    const offset_tracker& offsets() const { return _tracker; }
    bool empty() const;
    size_t size_bytes() const;
    void tombstone();
    bool is_tombstone() const;
    bool has_outstanding_locks() const;
    bool is_closed() const;
    bool has_compaction_index() const;
    void mark_as_compacted_segment();
    void unmark_as_compacted_segment();
    bool is_compacted_segment() const;
    void mark_as_finished_self_compaction();
    bool finished_self_compaction() const;
    void mark_as_finished_windowed_compaction();
    bool finished_windowed_compaction() const;
    /// \brief used for compaction, to reset the tracker from index
    void force_set_commit_offset_from_index();

    /// \brief Returns whether the underlying segment has data records that
    /// might be removed if compaction were to run. May return false positives,
    /// e.g. if the underlying index was written in a version with insufficient
    /// metadata.
    bool may_have_compactible_records() const;

    // low level api's are discouraged and might be deprecated
    // please use higher level API's when possible
    segment_reader& reader();
    segment_reader_ptr release_segment_reader();
    void swap_reader(segment_reader_ptr);
    size_t file_size() const { return _reader->file_size(); }
    const ss::sstring filename() const { return _reader->filename(); }
    const segment_full_path& path() const { return _reader->path(); }
    segment_index& index();
    const segment_index& index() const;
    segment_appender_ptr release_appender();
    segment_appender& appender();
    const segment_appender& appender() const;
    bool has_appender() const;
    compacted_index_writer& compaction_index();
    const compacted_index_writer& compaction_index() const;
    // We currently use `max_collectible_offset` to control both
    // deletion/eviction, and compaction.
    bool has_compactible_offsets(const compaction_config& cfg) const;

    void release_batch_cache_index() { _cache.reset(); }
    /** Cache methods */
    std::optional<std::reference_wrapper<batch_cache_index>> cache();
    std::optional<std::reference_wrapper<const batch_cache_index>>
    cache() const;
    bool has_cache() const;
    batch_cache_index::read_result cache_get(
      model::offset offset,
      model::offset max_offset,
      std::optional<model::record_batch_type> type_filter,
      std::optional<model::timestamp> first_ts,
      size_t max_bytes,
      bool skip_lru_promote);
    void cache_put(
      const model::record_batch& batch, batch_cache::is_dirty_entry dirty);

    ss::future<ss::rwlock::holder> read_lock(
      ss::semaphore::time_point timeout = ss::semaphore::time_point::max());

    ss::future<ss::rwlock::holder> write_lock(
      ss::semaphore::time_point timeout = ss::semaphore::time_point::max());

    /*
     * return an estimate of how much data on disk is associated with this
     * segment (e.g. the data file, indices, etc...).
     */
    ss::future<usage> persistent_size();

    /*
     * return the number of bytes removed from disk.
     */
    ss::future<size_t> remove_persistent_state();

    generation_id get_generation_id() const { return _generation_id; }
    void advance_generation() { _generation_id++; }

    /**
     * Timestamp of the first data batch written to this segment.
     * Note that this isn't the first timestamp of a data batch in the log,
     * and is only set if the segment was appended to while this segment was
     * active. I.e. a closed segment following a restart wouldn't have this
     * value set, because we only intend on using this in the context of an
     * active segment.
     */
    constexpr std::optional<ss::lowres_clock::time_point>
    first_write_ts() const {
        return _first_write;
    }

    void clear_cached_disk_usage();

    /// Fallback timestamp method, for use if the timestamps in the index
    /// appear to be invalid (e.g. too far in the future)
    ss::future<model::timestamp> get_file_timestamp() const;

private:
    void set_close();
    void cache_truncate(model::offset offset);
    void check_segment_not_closed(const char* msg);
    ss::future<> do_truncate(
      model::offset prev_last_offset,
      size_t physical,
      model::timestamp new_max_timestamp);
    ss::future<> do_close();
    ss::future<> do_flush();
    ss::future<> do_release_appender(
      segment_appender_ptr,
      std::optional<batch_cache_index>,
      std::optional<compacted_index_writer>);
    ss::future<> compaction_index_batch(const model::record_batch&);
    ss::future<> do_compaction_index_batch(const model::record_batch&);
    void release_appender_in_background(readers_cache* readers_cache);

    ss::future<size_t> remove_persistent_state(std::filesystem::path);

    struct appender_callbacks : segment_appender::callbacks {
        explicit appender_callbacks(segment* segment)
          : _segment(segment) {}

        void committed_physical_offset(size_t offset) final {
            _segment->advance_stable_offset(offset);
        }

        segment* _segment;
    };

    storage_resources& _resources;

    appender_callbacks _appender_callbacks;

    void advance_stable_offset(size_t offset);
    /**
     * Generation id is incremented every time the destructive operation is
     * executed on the segment, it is used when atomically swapping the staging
     * compacted segment content with current segment content.
     *
     * Generation is advanced when:
     * - segment is truncated
     * - batches are appended to the segment
     * - segment appender is flushed
     * - segment is compacted
     */
    generation_id _generation_id;

    offset_tracker _tracker;
    segment_reader_ptr _reader;
    segment_index _idx;
    bitflags _flags{bitflags::none};
    segment_appender_ptr _appender;
    std::optional<size_t> _data_disk_usage_size;

    // compaction index size should be cleared whenever the size might change
    // (e.g. after compaction). when cleared it will reset the next time the
    // size of the compaction index is needed (e.g. estimating total seg size).
    std::optional<size_t> _compaction_index_size;
    std::optional<compacted_index_writer> _compaction_index;

    std::optional<batch_cache_index> _cache;
    ss::rwlock _destructive_ops;
    ss::gate _gate;

    absl::btree_map<size_t, model::offset> _inflight;

    // Timestamp from server time of first data written to this segment,
    // field is set when a raft_data batch is appended.
    // Used to implement segment.ms rolling
    std::optional<ss::lowres_clock::time_point> _first_write;

    friend std::ostream& operator<<(std::ostream&, const segment&);
};

/**
 * \brief Create a segment reader for the specified file.
 *
 * Returns an exceptional future if the segment cannot be opened.
 * This may occur due to many reasons such as a file system error, or
 * because the segment is corrupt or is stored in an unsupported
 * format.
 *
 * Returns a ready future containing a nullptr value if the specified
 * file is not a segment file.
 *
 * Returns an open segment if the segment was successfully opened.
 * Including a valid index and recovery for the index if one does not
 * exist
 */
ss::future<ss::lw_shared_ptr<segment>> open_segment(
  segment_full_path path,
  std::optional<batch_cache_index> batch_cache,
  size_t buf_size,
  unsigned read_ahead,
  storage_resources&,
  ss::sharded<features::feature_table>& feature_table,
  std::optional<ntp_sanitizer_config> ntp_sanitizer_config);

ss::future<ss::lw_shared_ptr<segment>> make_segment(
  const ntp_config& ntpc,
  model::offset base_offset,
  model::term_id term,
  ss::io_priority_class pc,
  record_version_type version,
  size_t buf_size,
  unsigned read_ahead,
  std::optional<batch_cache_index> batch_cache,
  storage_resources&,
  ss::sharded<features::feature_table>& feature_table,
  std::optional<ntp_sanitizer_config> ntp_sanitizer_config,
  size_t segment_size_hint);

// bitflags operators
[[gnu::always_inline]] inline segment::bitflags
operator|(segment::bitflags a, segment::bitflags b) {
    return segment::bitflags(
      std::underlying_type_t<segment::bitflags>(a)
      | std::underlying_type_t<segment::bitflags>(b));
}

[[gnu::always_inline]] inline void
operator|=(segment::bitflags& a, segment::bitflags b) {
    a = (a | b);
}

[[gnu::always_inline]] inline segment::bitflags operator~(segment::bitflags a) {
    return segment::bitflags(~std::underlying_type_t<segment::bitflags>(a));
}

[[gnu::always_inline]] inline segment::bitflags
operator&(segment::bitflags a, segment::bitflags b) {
    return segment::bitflags(
      std::underlying_type_t<segment::bitflags>(a)
      & std::underlying_type_t<segment::bitflags>(b));
}

[[gnu::always_inline]] inline void
operator&=(segment::bitflags& a, segment::bitflags b) {
    a = (a & b);
}

inline bool segment::empty() const {
    if (_appender) {
        return _appender->file_byte_offset() == 0;
    }
    return _reader->empty();
}
inline size_t segment::size_bytes() const {
    if (_appender) {
        return _appender->file_byte_offset();
    }
    return _reader->file_size();
}
inline bool segment::has_compaction_index() const {
    return _compaction_index != std::nullopt;
}

inline bool
segment::has_compactible_offsets(const compaction_config& cfg) const {
    // since we don't support partially-compacted segments, a segment must
    // end before the max compactible offset to be eligible for compaction.
    return _tracker.get_stable_offset() <= cfg.max_collectible_offset;
}

inline void segment::mark_as_compacted_segment() {
    _flags |= bitflags::is_compacted_segment;
}
inline void segment::unmark_as_compacted_segment() {
    _flags &= ~bitflags::is_compacted_segment;
}
inline bool segment::is_compacted_segment() const {
    return (_flags & bitflags::is_compacted_segment)
           == bitflags::is_compacted_segment;
}
inline void segment::mark_as_finished_self_compaction() {
    _flags |= bitflags::finished_self_compaction;
}
inline bool segment::finished_self_compaction() const {
    return (_flags & bitflags::finished_self_compaction)
           == bitflags::finished_self_compaction;
}
inline void segment::mark_as_finished_windowed_compaction() {
    _flags |= bitflags::finished_windowed_compaction;
}
inline bool segment::finished_windowed_compaction() const {
    return (_flags & bitflags::finished_windowed_compaction)
           == bitflags::finished_windowed_compaction;
}
inline std::optional<std::reference_wrapper<batch_cache_index>>
segment::cache() {
    using ret_t = std::optional<std::reference_wrapper<batch_cache_index>>;
    return _cache.has_value() ? ret_t(std::ref(*_cache)) : ret_t(std::nullopt);
}
inline std::optional<std::reference_wrapper<const batch_cache_index>>
segment::cache() const {
    using ret_t
      = std::optional<std::reference_wrapper<const batch_cache_index>>;
    return _cache.has_value() ? ret_t(std::cref(*_cache)) : ret_t(std::nullopt);
}
inline bool segment::has_cache() const { return _cache != std::nullopt; }
inline batch_cache_index::read_result segment::cache_get(
  model::offset offset,
  model::offset max_offset,
  std::optional<model::record_batch_type> type_filter,
  std::optional<model::timestamp> first_ts,
  size_t max_bytes,
  bool skip_lru_promote) {
    if (likely(bool(_cache))) {
        return _cache->read(
          offset,
          max_offset,
          type_filter,
          first_ts,
          max_bytes,
          skip_lru_promote);
    }
    return batch_cache_index::read_result{
      .next_batch = offset,
    };
}
inline void segment::cache_put(
  const model::record_batch& batch, batch_cache::is_dirty_entry dirty) {
    if (likely(bool(_cache))) {
        _cache->put(batch, dirty);
    }
}
inline ss::future<ss::rwlock::holder>
segment::read_lock(ss::semaphore::time_point timeout) {
    return _destructive_ops.hold_read_lock(timeout);
}
inline ss::future<ss::rwlock::holder>
segment::write_lock(ss::semaphore::time_point timeout) {
    return _destructive_ops.hold_write_lock(timeout);
}
inline void segment::tombstone() { _flags |= bitflags::mark_tombstone; }
inline bool segment::has_outstanding_locks() const {
    return _destructive_ops.locked();
}
inline bool segment::is_closed() const {
    return (_flags & bitflags::closed) == bitflags::closed;
}
inline segment_reader& segment::reader() { return *_reader; }
inline void segment::swap_reader(segment_reader_ptr new_reader) {
    std::swap(new_reader, _reader);
}
inline segment_reader_ptr segment::release_segment_reader() {
    return std::move(_reader);
}
inline segment_index& segment::index() { return _idx; }
inline const segment_index& segment::index() const { return _idx; }
inline segment_appender_ptr segment::release_appender() {
    return std::move(_appender);
}
inline segment_appender& segment::appender() { return *_appender; }
inline const segment_appender& segment::appender() const { return *_appender; }
inline bool segment::has_appender() const { return !!_appender; }
inline compacted_index_writer& segment::compaction_index() {
    return *_compaction_index;
}
inline const compacted_index_writer& segment::compaction_index() const {
    return *_compaction_index;
}
inline void segment::set_close() { _flags |= bitflags::closed; }
inline bool segment::is_tombstone() const {
    return (_flags & bitflags::mark_tombstone) == bitflags::mark_tombstone;
}
/// \brief used for compaction, to reset the tracker from index
inline void segment::force_set_commit_offset_from_index() {
    _tracker.set_offsets(
      offset_tracker::committed_offset_t{_idx.max_offset()},
      offset_tracker::stable_offset_t{_idx.max_offset()},
      offset_tracker::dirty_offset_t{_idx.max_offset()});
}

} // namespace storage
