#pragma once

#include "storage/batch_cache.h"
#include "storage/compacted_index_writer.h"
#include "storage/segment_appender.h"
#include "storage/segment_index.h"
#include "storage/segment_reader.h"
#include "storage/types.h"
#include "storage/version.h"

#include <seastar/core/file.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/rwlock.hh>

#include <exception>
#include <optional>

namespace storage {
struct segment_closed_exception final : std::exception {
    const char* what() const noexcept final {
        return "segment_closed exception";
    }
};

constexpr size_t default_segment_readahead_size = 128 * 1024;

class segment {
public:
    struct offset_tracker {
        offset_tracker(model::term_id t, model::offset base)
          : term(t)
          , base_offset(base)
          , committed_offset(base)
          , dirty_offset(base) {}
        model::term_id term;
        model::offset base_offset;

        /// \brief These offsets are the `batch.last_offset()` and not
        /// `batch.base_offset()` which might be confusing at first,
        /// but allow us to keep track of the actual last logical offset
        model::offset committed_offset;
        model::offset dirty_offset;
        friend std::ostream& operator<<(std::ostream&, const offset_tracker&);
    };
    enum class bitflags : uint32_t {
        none = 0,
        is_compacted_segment = 1,
        finished_self_compaction = 1U << 1U,
        mark_tombstone = 1U << 2U,
        closed = 1U << 3U,
    };

public:
    segment(
      offset_tracker tracker,
      segment_reader,
      segment_index,
      std::optional<segment_appender>,
      std::optional<compacted_index_writer>,
      std::optional<batch_cache_index>) noexcept;
    ~segment() noexcept = default;
    segment(segment&&) noexcept = default;
    // rwlock does not have move-assignment
    segment& operator=(segment&&) noexcept = delete;
    segment(const segment&) = delete;
    segment& operator=(const segment&) = delete;

    ss::future<> close();
    ss::future<> flush();
    ss::future<> release_appender();
    ss::future<> truncate(model::offset, size_t physical);

    /// main write interface
    /// auto indexes record_batch
    /// We recommend using the const-ref method below over the r-value since we
    /// do not need to take ownership of the batch itself
    ss::future<append_result> append(model::record_batch&&);
    ss::future<append_result> append(const model::record_batch&);
    ss::future<bool> materialize_index();

    /// main read interface
    ss::input_stream<char>
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
    bool is_compacted_segment() const;
    void mark_as_finished_self_compaction();
    bool finished_self_compaction() const;
    /// \brief used for compaction, to reset the tracker from index
    void force_set_commit_offset_from_index();
    // low level api's are discouraged and might be deprecated
    // please use higher level API's when possible
    segment_reader& reader();
    const segment_reader& reader() const;
    segment_index& index();
    const segment_index& index() const;
    segment_appender& appender();
    const segment_appender& appender() const;
    bool has_appender() const;
    compacted_index_writer& compaction_index();
    const compacted_index_writer& compaction_index() const;

    /** Cache methods */
    batch_cache_index& cache();
    const batch_cache_index& cache() const;
    bool has_cache() const;
    batch_cache_index::read_result cache_get(
      model::offset offset,
      model::offset max_offset,
      std::optional<model::record_batch_type> type_filter,
      std::optional<model::timestamp> first_ts,
      size_t max_bytes,
      bool skip_lru_promote);
    void cache_put(const model::record_batch& batch);

    ss::future<ss::rwlock::holder> read_lock(
      ss::semaphore::time_point timeout = ss::semaphore::time_point::max());

    ss::future<ss::rwlock::holder> write_lock(
      ss::semaphore::time_point timeout = ss::semaphore::time_point::max());

private:
    void set_close();
    void cache_truncate(model::offset offset);
    void check_segment_not_closed(const char* msg);
    ss::future<> do_truncate(model::offset prev_last_offset, size_t physical);
    ss::future<> do_close();
    ss::future<> do_flush();
    ss::future<> do_release_appender(
      std::optional<segment_appender>,
      std::optional<batch_cache_index>,
      std::optional<compacted_index_writer>);
    ss::future<> remove_tombstones();
    ss::future<> compaction_index_batch(const model::record_batch&);
    ss::future<> do_compaction_index_batch(const model::record_batch&);

    offset_tracker _tracker;
    segment_reader _reader;
    segment_index _idx;
    bitflags _flags{bitflags::none};
    std::optional<segment_appender> _appender;
    std::optional<compacted_index_writer> _compaction_index;
    std::optional<batch_cache_index> _cache;
    ss::rwlock _destructive_ops;
    ss::gate _gate;

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
  const std::filesystem::path& path,
  debug_sanitize_files sanitize_fileops,
  std::optional<batch_cache_index> batch_cache,
  size_t buf_size = default_segment_readahead_size);

ss::future<ss::lw_shared_ptr<segment>> make_segment(
  const ntp_config& ntpc,
  model::offset base_offset,
  model::term_id term,
  ss::io_priority_class pc,
  record_version_type version,
  size_t buf_size,
  debug_sanitize_files sanitize_fileops,
  std::optional<batch_cache_index> batch_cache);

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
    return _reader.empty();
}
inline size_t segment::size_bytes() const {
    if (_appender) {
        return _appender->file_byte_offset();
    }
    return _reader.file_size();
}
inline bool segment::has_compaction_index() const {
    return _compaction_index != std::nullopt;
}
inline void segment::mark_as_compacted_segment() {
    _flags |= bitflags::is_compacted_segment;
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
inline batch_cache_index& segment::cache() { return *_cache; }
inline const batch_cache_index& segment::cache() const { return *_cache; }
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
inline void segment::cache_put(const model::record_batch& batch) {
    if (likely(bool(_cache))) {
        _cache->put(batch);
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
inline segment_reader& segment::reader() { return _reader; }
inline const segment_reader& segment::reader() const { return _reader; }
inline segment_index& segment::index() { return _idx; }
inline const segment_index& segment::index() const { return _idx; }
inline segment_appender& segment::appender() { return *_appender; }
inline const segment_appender& segment::appender() const { return *_appender; }
inline bool segment::has_appender() const { return _appender != std::nullopt; }
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
    _tracker.committed_offset = _idx.max_offset();
    _tracker.dirty_offset = _idx.max_offset();
}

} // namespace storage
