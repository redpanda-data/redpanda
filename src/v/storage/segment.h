#pragma once

#include "storage/batch_cache.h"
#include "storage/compacted_index_writer.h"
#include "storage/segment_appender.h"
#include "storage/segment_index.h"
#include "storage/segment_reader.h"
#include "storage/types.h"
#include "storage/version.h"

#include <seastar/core/file.hh>
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
    bool empty() const {
        if (_appender) {
            return _appender->file_byte_offset() == 0;
        }
        return _reader.empty();
    }
    size_t size_bytes() const {
        if (_appender) {
            return _appender->file_byte_offset();
        }
        return _reader.file_size();
    }
    // low level api's are discouraged and might be deprecated
    // please use higher level API's when possible
    segment_reader& reader() { return _reader; }
    const segment_reader& reader() const { return _reader; }
    segment_index& index() { return _idx; }
    const segment_index& index() const { return _idx; }
    segment_appender& appender() { return *_appender; }
    const segment_appender& appender() const { return *_appender; }
    bool has_appender() const { return _appender != std::nullopt; }
    compacted_index_writer& compaction_index() { return *_compaction_index; }
    const compacted_index_writer& compaction_index() const {
        return *_compaction_index;
    }
    bool has_compacion_index() const {
        return _compaction_index != std::nullopt;
    }
    batch_cache_index& cache() { return *_cache; }
    const batch_cache_index& cache() const { return *_cache; }
    bool has_cache() const { return _cache != std::nullopt; }

    batch_cache_index::read_result cache_get(
      model::offset offset,
      model::offset max_offset,
      std::optional<model::record_batch_type> type_filter,
      std::optional<model::timestamp> first_ts,
      size_t max_bytes) {
        if (likely(bool(_cache))) {
            return _cache->read(
              offset, max_offset, type_filter, first_ts, max_bytes);
        }
        return batch_cache_index::read_result{
          .next_batch = offset,
        };
    }

    void cache_put(const model::record_batch& batch) {
        if (likely(bool(_cache))) {
            _cache->put(batch);
        }
    }

    ss::future<ss::rwlock::holder> read_lock(
      ss::semaphore::time_point timeout = ss::semaphore::time_point::max()) {
        return _destructive_ops.hold_read_lock(timeout);
    }

    ss::future<ss::rwlock::holder> write_lock(
      ss::semaphore::time_point timeout = ss::semaphore::time_point::max()) {
        return _destructive_ops.hold_write_lock(timeout);
    }

    void tombstone() { _tombstone = true; }
    bool has_outstanding_locks() const { return _destructive_ops.locked(); }
    bool is_closed() const { return _closed; }

private:
    void cache_truncate(model::offset offset);
    void check_segment_not_closed(const char* msg);
    ss::future<> do_truncate(model::offset prev_last_offset, size_t physical);
    ss::future<> do_close();
    ss::future<> do_flush();
    ss::future<> remove_thombsones();
    ss::future<> compaction_index_batch(const model::record_batch&);
    ss::future<> do_compaction_index_batch(const model::record_batch&);

    offset_tracker _tracker;
    segment_reader _reader;
    segment_index _idx;
    std::optional<segment_appender> _appender;
    std::optional<compacted_index_writer> _compaction_index;
    std::optional<batch_cache_index> _cache;
    ss::rwlock _destructive_ops;
    bool _tombstone = false;
    bool _closed = false;

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
  const ss::sstring& base_dir,
  debug_sanitize_files sanitize_fileops,
  std::optional<batch_cache_index> batch_cache);

} // namespace storage
