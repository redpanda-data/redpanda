#pragma once

#include "storage/batch_cache.h"
#include "storage/segment_appender.h"
#include "storage/segment_index.h"
#include "storage/segment_reader.h"
#include "storage/types.h"

#include <seastar/core/file.hh>
#include <seastar/core/rwlock.hh>

#include <optional>

namespace storage {
class segment {
public:
    segment(
      segment_reader,
      segment_index,
      std::optional<segment_appender>,
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
    ss::future<append_result> append(model::record_batch);

    /// main read interface
    ss::input_stream<char>
      offset_data_stream(model::offset, ss::io_priority_class);

    bool empty() const {
        if (_appender) {
            return _dirty_offset() < 0;
        }
        return _reader.empty();
    }
    model::offset committed_offset() const { return _reader.max_offset(); }
    model::offset dirty_offset() const {
        if (_appender) {
            return _dirty_offset;
        }
        return committed_offset();
    }
    // low level api's are discouraged and might be deprecated
    // please use higher level API's when possible

    segment_reader& reader() { return _reader; }
    const segment_reader& reader() const { return _reader; }
    segment_index& index() { return _idx; }
    const segment_index& index() const { return _idx; }
    segment_appender& appender() { return *_appender; }
    const segment_appender& appender() const { return *_appender; }
    bool has_appender() const { return bool(_appender); }
    model::term_id term() const { return _reader.term(); }

    batch_cache_index::read_result cache_get(
      model::offset offset,
      model::offset max_offset,
      std::optional<model::record_batch_type> type_filter,
      size_t max_bytes) {
        if (likely(bool(_cache))) {
            return _cache->read(offset, max_offset, type_filter, max_bytes);
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

private:
    void cache_truncate(model::offset offset);
    void check_segment_not_closed(const char* msg);
    ss::future<> do_truncate(model::offset prev_last_offset, size_t physical);
    ss::future<> do_close();

    // last offset of the last batch, i.e.: batch.last_offset()
    model::offset _dirty_offset;
    segment_reader _reader;
    segment_index _idx;
    std::optional<segment_appender> _appender;
    std::optional<batch_cache_index> _cache;
    ss::rwlock _destructive_ops;
    bool _tombstone = false;
    bool _closed = false;

    friend std::ostream& operator<<(std::ostream&, const segment&);
};

} // namespace storage
